package obs

import (
	"context"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Kafka trace propagation (OBS-61), used by the producer (checkout-service) and the
// consumer (order-service). The W3C traceparent / tracestate / baggage travel in the
// record headers; headers are plain bytes, so any language can read them. The package
// does not import a Kafka client: callers convert their record headers to KafkaHeader.

const kafkaTracer = "everly/obs/kafka"

// KafkaHeader is one record header.
type KafkaHeader struct {
	Key   string
	Value []byte
}

// HeaderCarrier adapts record headers to propagation.TextMapCarrier.
type HeaderCarrier struct{ H *[]KafkaHeader }

// Get returns the last header with the key (case-insensitive), or "".
func (c HeaderCarrier) Get(key string) string {
	h := *c.H
	for i := len(h) - 1; i >= 0; i-- {
		if strings.EqualFold(h[i].Key, key) {
			return string(h[i].Value)
		}
	}
	return ""
}

// Set replaces the header with the key, or appends it.
func (c HeaderCarrier) Set(key, value string) {
	for i := range *c.H {
		if strings.EqualFold((*c.H)[i].Key, key) {
			(*c.H)[i].Value = []byte(value)
			return
		}
	}
	*c.H = append(*c.H, KafkaHeader{Key: key, Value: []byte(value)})
}

// Keys lists the header keys.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.H))
	for i, h := range *c.H {
		keys[i] = h.Key
	}
	return keys
}

// ProducerSpan starts the PRODUCER span of one record (child of the span in ctx) and
// writes its context into headers. End the span (EndSpan) when the broker acked.
func ProducerSpan(ctx context.Context, topic string, headers *[]KafkaHeader) (context.Context, trace.Span) {
	ctx, span := otel.Tracer(kafkaTracer).Start(ctx, topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation", "publish"),
		))
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier{H: headers})
	return ctx, span
}

// ConsumerSpan starts the CONSUMER span for processing one record. With a valid
// traceparent in the headers the span is a child of the producer span, so the order is
// one trace. Missing or malformed headers never break consumption: the span then starts
// a new root trace (there is no producer span to link to).
// group is the consumer group, which equals the service name.
func ConsumerSpan(ctx context.Context, group, topic string, partition int32, offset int64, headers []KafkaHeader) (context.Context, trace.Span) {
	// Extract into an empty context: a span already in ctx (the poll loop's) must not be
	// mistaken for the producer's.
	extracted := otel.GetTextMapPropagator().Extract(context.Background(), HeaderCarrier{H: &headers})
	parent := ctx
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation", "process"),
			attribute.String("messaging.kafka.consumer.group", group),
			attribute.String("messaging.consumer.group.name", group),
			attribute.Int("messaging.destination.partition.id", int(partition)),
			attribute.String("messaging.kafka.message.offset", strconv.FormatInt(offset, 10)),
		),
	}
	if sc := trace.SpanContextFromContext(extracted); sc.IsValid() {
		parent = baggage.ContextWithBaggage(trace.ContextWithRemoteSpanContext(ctx, sc), baggage.FromContext(extracted))
	} else {
		opts = append(opts, trace.WithNewRoot())
	}
	return otel.Tracer(kafkaTracer).Start(parent, topic+" process", opts...)
}

// EndSpan ends span; a non-nil err is recorded on it (status error + exception event).
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
