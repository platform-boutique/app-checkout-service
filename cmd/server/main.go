package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"checkout-service/internal/checkout"
	pb "checkout-service/proto"

	"github.com/grafana/pyroscope-go"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

var log *logrus.Logger

func init() {
	log = logrus.New()
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
}

func main() {
	// --- Telemetry: traces, metrics and logs pushed over OTLP/gRPC (Alloy → Tempo/Mimir/Loki) ---
	shutdownTelemetry, err := initTelemetry(context.Background(), "checkout-service", log, checkout.Logger())
	if err != nil {
		log.Warnf("telemetry disabled, continuing without it: %v", err)
	} else {
		log.Info("telemetry enabled (OTLP/gRPC)")
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTelemetry(ctx)
		}()
	}

	// --- Profiling ---
	if os.Getenv("ENABLE_PROFILING") == "1" {
		initProfiling()
	} else {
		log.Info("profiling disabled — set ENABLE_PROFILING=1 to enable")
	}

	port := "5050"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	log.Infof("starting grpc server on :%s", port)
	if err := run(port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func run(port string) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(grpcprom.UnaryServerInterceptor),
		grpc.StreamInterceptor(grpcprom.StreamServerInterceptor),
	)

	// --- Connect to downstream services ---
	svc := &checkout.Service{}

	var connErr error
	if svc.ProductCatalogConn, connErr = dialGRPC(envOrDefault("PRODUCT_CATALOG_SERVICE_ADDR", "product-catalog-service:3550")); connErr != nil {
		return fmt.Errorf("connect to product-catalog: %w", connErr)
	}
	if svc.CartConn, connErr = dialGRPC(envOrDefault("CART_SERVICE_ADDR", "cart-service:7070")); connErr != nil {
		return fmt.Errorf("connect to cart: %w", connErr)
	}
	if svc.CurrencyConn, connErr = dialGRPC(envOrDefault("CURRENCY_SERVICE_ADDR", "currency-service:7000")); connErr != nil {
		return fmt.Errorf("connect to currency: %w", connErr)
	}
	if svc.ShippingConn, connErr = dialGRPC(envOrDefault("SHIPPING_SERVICE_ADDR", "shipping-service:50051")); connErr != nil {
		return fmt.Errorf("connect to shipping: %w", connErr)
	}
	if svc.PaymentConn, connErr = dialGRPC(envOrDefault("PAYMENT_SERVICE_ADDR", "payment-service:50051")); connErr != nil {
		return fmt.Errorf("connect to payment: %w", connErr)
	}
	if svc.EmailConn, connErr = dialGRPC(envOrDefault("EMAIL_SERVICE_ADDR", "email-service:8080")); connErr != nil {
		return fmt.Errorf("connect to email: %w", connErr)
	}

	// Order storage is optional: without ORDER_SERVICE_ADDR checkout behaves as before
	// (no order is stored). With it, storing the order is mandatory: if order-service
	// cannot be reached PlaceOrder fails with Unavailable before the card is charged.
	if addr := os.Getenv("ORDER_SERVICE_ADDR"); addr != "" {
		if svc.OrderConn, connErr = dialGRPC(addr); connErr != nil {
			return fmt.Errorf("connect to order-service: %w", connErr)
		}
		log.Infof("order storage enabled (order-service at %s)", addr)
	} else {
		log.Info("order storage disabled: ORDER_SERVICE_ADDR is not set, orders are not stored")
	}

	// Stock reservation is optional: without INVENTORY_SERVICE_ADDR checkout behaves as
	// before (no reserve/commit). With it, a reservation is mandatory (fail closed): if
	// inventory-service cannot be reached PlaceOrder fails with Unavailable before the
	// card is charged.
	if addr := os.Getenv("INVENTORY_SERVICE_ADDR"); addr != "" {
		if svc.InventoryConn, connErr = dialGRPC(addr); connErr != nil {
			return fmt.Errorf("connect to inventory-service: %w", connErr)
		}
		log.Infof("stock reservation enabled (inventory-service at %s)", addr)
	} else {
		log.Info("stock reservation disabled: INVENTORY_SERVICE_ADDR is not set, no stock is reserved")
	}

	// The orders.placed event is optional: without KAFKA_BROKERS checkout behaves as
	// before (no event, direct email-service call). With it the event is published
	// after Ship and Commit, best effort, and checkout no longer calls email-service.
	// franz-go connects lazily: a broker that is down at startup does not stop checkout.
	if brokers := checkout.ParseBrokers(os.Getenv("KAFKA_BROKERS")); len(brokers) > 0 {
		pub, err := checkout.NewKafkaPublisher(brokers)
		if err != nil {
			return fmt.Errorf("kafka producer: %w", err)
		}
		svc.Events = pub
		defer func() {
			// After the gRPC server has drained (see below): flush what is still buffered.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := pub.Close(ctx); err != nil {
				log.Warnf("kafka producer flush on shutdown: %v", err)
			} else {
				log.Info("kafka producer flushed and closed")
			}
		}()
		log.Infof("order events enabled (topic %s, brokers %s)", checkout.OrdersPlacedTopic, os.Getenv("KAFKA_BROKERS"))
	} else {
		log.Info("order events disabled: KAFKA_BROKERS is not set, no event is published and email-service is called directly")
	}

	pb.RegisterCheckoutServiceServer(srv, svc)

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(srv)

	grpcprom.Register(srv)
	grpcprom.EnableHandlingTimeHistogram()

	// --- Metrics HTTP server with explicit timeouts (G114 fix) ---
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())

		// G108 fix: only register pprof handlers when explicitly enabled,
		// never on http.DefaultServeMux.
		if os.Getenv("ENABLE_PROFILING") == "1" {
			registerPprofHandlers(metricsMux)
			log.Info("pprof endpoints registered on metrics server")
		}

		metricsPort := envOrDefault("METRICS_PORT", "9090")
		metricsSrv := &http.Server{
			Addr:              ":" + metricsPort,
			Handler:           metricsMux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      60 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		log.Infof("metrics endpoint on :%s", metricsPort)
		if err := metricsSrv.ListenAndServe(); err != nil {
			log.Warnf("metrics server error: %v", err)
		}
	}()

	// SIGTERM/SIGINT: let in-flight PlaceOrder calls finish (bounded), then return so
	// that the deferred producer flush runs. Serve returns nil after a (graceful) stop.
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	go func() {
		<-sigCtx.Done()
		log.Info("shutdown signal received, draining")
		stopped := make(chan struct{})
		go func() { srv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(15 * time.Second):
			srv.Stop()
		}
	}()

	log.Infof("listening on %s", listener.Addr().String())
	return srv.Serve(listener)
}

// registerPprofHandlers adds pprof handlers to the given mux.
// Avoids the blank import of net/http/pprof which unconditionally
// registers handlers on DefaultServeMux (gosec G108).
func registerPprofHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

func dialGRPC(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	return conn, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func initProfiling() {
	pyroscopeAddr := os.Getenv("PYROSCOPE_ADDR")
	if pyroscopeAddr == "" {
		pyroscopeAddr = "http://pyroscope:4040"
	}

	_, err := pyroscope.Start(pyroscope.Config{
		ApplicationName: "checkout-service",
		ServerAddress:   pyroscopeAddr,
		Logger:          pyroscope.StandardLogger,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
		},
	})
	if err != nil {
		log.Warnf("pyroscope init failed, continuing without it: %v", err)
		return
	}
	log.Info("profiling enabled → pushing to " + pyroscopeAddr)
}
