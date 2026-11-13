package obs

import (
	"context"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Kafka trace propagation (OBS-61), used by the producer (checkout-service) and the
// consumer (order-service). The W3C traceparent / tracestate / baggage travel in the
// record headers; headers are plain bytes, so any language can read them.

const kafkaTracer = "everly/obs/kafka"

// RecordCarrier adapts the headers of a record to propagation.TextMapCarrier.
type RecordCarrier struct{ Rec *kgo.Record }

// Get returns the last header with the key (case-insensitive), or "".
func (c RecordCarrier) Get(key string) string {
	for i := len(c.Rec.Headers) - 1; i >= 0; i-- {
		if strings.EqualFold(c.Rec.Headers[i].Key, key) {
			return string(c.Rec.Headers[i].Value)
		}
	}
	return ""
}

// Set replaces the header with the key, or appends it.
func (c RecordCarrier) Set(key, value string) {
	for i := range c.Rec.Headers {
		if strings.EqualFold(c.Rec.Headers[i].Key, key) {
			c.Rec.Headers[i].Value = []byte(value)
			return
		}
	}
	c.Rec.Headers = append(c.Rec.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// Keys lists the header keys.
func (c RecordCarrier) Keys() []string {
	keys := make([]string, len(c.Rec.Headers))
	for i, h := range c.Rec.Headers {
		keys[i] = h.Key
	}
	return keys
}

// ProducerSpan starts the PRODUCER span of one record (child of the span in ctx) and
// writes its context into the record headers. End the span when the broker acked.
// Set rec.Topic (or pass the default topic) before the call; topic is the span's
// messaging.destination.name.
func ProducerSpan(ctx context.Context, topic string, rec *kgo.Record) (context.Context, trace.Span) {
	ctx, span := otel.Tracer(kafkaTracer).Start(ctx, topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation", "publish"),
		))
	otel.GetTextMapPropagator().Inject(ctx, RecordCarrier{Rec: rec})
	return ctx, span
}

// ConsumerSpan starts the CONSUMER span for processing one record. With a valid
// traceparent in the headers the span is a child of the producer span, so the order is
// one trace. Missing or malformed headers never break consumption: the span then starts
// a new root trace (there is no producer span to link to).
// group is the consumer group, which equals the service name.
func ConsumerSpan(ctx context.Context, group string, rec *kgo.Record) (context.Context, trace.Span) {
	// Extract into an empty context: a span already in ctx (the poll loop's) must not be
	// mistaken for the producer's.
	extracted := otel.GetTextMapPropagator().Extract(context.Background(), RecordCarrier{Rec: rec})
	parent := ctx
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.String("messaging.operation", "process"),
			attribute.String("messaging.kafka.consumer.group", group),
			attribute.String("messaging.consumer.group.name", group),
			attribute.Int("messaging.destination.partition.id", int(rec.Partition)),
			attribute.String("messaging.kafka.message.offset", strconv.FormatInt(rec.Offset, 10)),
		),
	}
	if sc := trace.SpanContextFromContext(extracted); sc.IsValid() {
		parent = baggage.ContextWithBaggage(trace.ContextWithRemoteSpanContext(ctx, sc), baggage.FromContext(extracted))
	} else {
		opts = append(opts, trace.WithNewRoot())
	}
	return otel.Tracer(kafkaTracer).Start(parent, rec.Topic+" process", opts...)
}

// EndSpan ends span; a non-nil err is recorded on it (status error + exception event).
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
