package obs

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/bridges/otellogrus"
	promotel "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// queueSize bounds the batch processors (spans and log records are dropped, never
// buffered without limit, when the collector is slow or gone).
const queueSize = 2048

// Resource builds the resource attributes every signal carries (OBS-02):
// service.name, service.version, service.namespace=everly, deployment.environment,
// service.instance.id. OTEL_RESOURCE_ATTRIBUTES from the environment is applied last
// (so an operator can add or override), OTEL_SERVICE_NAME is already in Settings.
func Resource(ctx context.Context, s Settings) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName(s.Service),
			semconv.ServiceVersion(s.Version),
			semconv.ServiceNamespace(Namespace),
			semconv.ServiceInstanceID(s.Instance),
			attribute.String("deployment.environment", s.Environment),
		),
		resource.WithFromEnv(),
	)
}

// SetPropagator installs W3C trace context + baggage as the global propagator. It is
// done even when telemetry is off, so a trace that passes through still continues.
func SetPropagator() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
}

// startTelemetry pushes traces, metrics and logs over OTLP/gRPC to the collector.
//
//   - traces:  OpenTelemetry SDK (spans come from the otelgrpc / otelhttp handlers)
//   - metrics: the Prometheus default registry (go-grpc-prometheus, Go runtime, build_info,
//     custom metrics) is bridged into OTel and pushed, so existing metric names are kept
//   - logs:    every logger passed in gets a logrus hook that mirrors records over OTLP
//     (stdout output is unchanged; the record carries the span of entry.Context)
//
// Nothing here dials synchronously: the exporters connect in the background, so a
// collector that is down never blocks or fails the start (OBS-04). It returns a
// shutdown function that flushes all three pipelines, bounded by its context.
func startTelemetry(ctx context.Context, s Settings, loggers ...*logrus.Logger) (func(context.Context) error, error) {
	res, err := Resource(ctx, s)
	if err != nil {
		return nil, err
	}
	endpoint := s.OTLPEndpoint

	traceExp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpointURL(endpoint))
	if err != nil {
		return nil, err
	}
	// No explicit sampler: the SDK reads OTEL_TRACES_SAMPLER / _ARG (default parentbased_always_on).
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp, sdktrace.WithMaxQueueSize(queueSize)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpointURL(endpoint))
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(metricInterval()),
			sdkmetric.WithProducer(promotel.NewMetricProducer()),
		)),
	)

	logExp, err := otlploggrpc.New(ctx, otlploggrpc.WithEndpointURL(endpoint))
	if err != nil {
		return nil, err
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp, sdklog.WithMaxQueueSize(queueSize))),
	)
	otel.SetLoggerProvider(lp)
	for _, l := range loggers {
		l.AddHook(otellogrus.NewHook(s.Service, otellogrus.WithLoggerProvider(lp)))
	}

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}

func metricInterval() time.Duration {
	if v := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"); v != "" {
		if ms, err := time.ParseDuration(v + "ms"); err == nil && ms > 0 {
			return ms
		}
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 15 * time.Second
}
