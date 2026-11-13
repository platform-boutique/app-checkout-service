package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"checkout-service/internal/checkout"
	"checkout-service/internal/obs"
	pb "checkout-service/proto"

	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

var log = logrus.New()

// drainTimeout is how long in-flight calls (a card charge must finish) get after SIGTERM.
const drainTimeout = 15 * time.Second

func main() {
	// Logger, env contract (OTEL_*, PYROSCOPE_ADDR), OTLP pipelines, Pyroscope, build_info and
	// /metrics on :9090 live in internal/obs. The package logger of internal/checkout is the
	// second logger: the request path logs through it.
	o := obs.Start(context.Background(), obs.Options{
		Service: "checkout-service",
		Loggers: []*logrus.Logger{log, checkout.Logger()},
	})
	defer o.Recover()
	defer o.Shutdown()

	port := "5050"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	log.Infof("starting grpc server on :%s", port)
	if err := run(port); err != nil {
		o.Fatal("server error", err)
	}
}

func run(port string) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(obs.UnaryServerInterceptor(), grpcprom.UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(obs.StreamServerInterceptor(), grpcprom.StreamServerInterceptor),
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
			ctx, cancel := context.WithTimeout(context.Background(), obs.DefaultShutdownTimeout)
			defer cancel()
			if err := pub.Close(ctx); err != nil {
				log.WithError(err).Warn("kafka producer flush on shutdown")
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
		case <-time.After(drainTimeout):
			srv.Stop()
		}
	}()

	log.Infof("listening on %s", listener.Addr().String())
	return srv.Serve(listener)
}

func dialGRPC(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(obs.UnaryClientInterceptor()),
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
