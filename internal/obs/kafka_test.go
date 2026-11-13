package obs

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func kafkaSetup(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })
	return rec
}

func attr(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	return ""
}

// One order is one trace: PlaceOrder (server span) -> producer span -> headers -> consumer span.
func TestKafkaRoundTrip(t *testing.T) {
	rec := kafkaSetup(t)
	ctx, root := otel.Tracer("t").Start(context.Background(), "PlaceOrder")

	hs := []KafkaHeader{{Key: "event-type", Value: []byte("order.placed")}}
	_, prod := ProducerSpan(ctx, "orders.placed", &hs)

	var traceparent string
	for _, h := range hs {
		if h.Key == "traceparent" {
			traceparent = string(h.Value)
		}
	}
	if traceparent == "" || len(hs) != 2 {
		t.Fatalf("headers after inject: %+v", hs)
	}

	// The consumer is another process: nothing but the record crosses over.
	cctx, cons := ConsumerSpan(context.Background(), "order-service", "orders.placed", 2, 41, hs)
	if !trace.SpanContextFromContext(cctx).IsValid() {
		t.Fatal("no span in the consumer context")
	}
	EndSpan(cons, nil)
	EndSpan(prod, nil)
	root.End()

	var p, c sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch s.SpanKind() {
		case trace.SpanKindProducer:
			p = s
		case trace.SpanKindConsumer:
			c = s
		}
	}
	if p == nil || c == nil {
		t.Fatalf("spans: %v", rec.Ended())
	}
	if c.SpanContext().TraceID() != root.SpanContext().TraceID() || c.Parent().SpanID() != p.SpanContext().SpanID() {
		t.Fatalf("consumer is not the child of the producer: trace %s vs %s, parent %s vs %s",
			c.SpanContext().TraceID(), root.SpanContext().TraceID(), c.Parent().SpanID(), p.SpanContext().SpanID())
	}
	if p.Parent().SpanID() != root.SpanContext().SpanID() {
		t.Fatal("producer is not the child of PlaceOrder")
	}
	for key, want := range map[string]string{
		"messaging.system": "kafka", "messaging.destination.name": "orders.placed", "messaging.operation": "process",
		"messaging.kafka.consumer.group": "order-service", "messaging.consumer.group.name": "order-service",
		"messaging.destination.partition.id": "2", "messaging.kafka.message.offset": "41",
	} {
		if got := attr(c, key); got != want {
			t.Errorf("consumer %s=%q want %q", key, got, want)
		}
	}
	if attr(p, "messaging.operation") != "publish" || attr(p, "messaging.destination.name") != "orders.placed" {
		t.Errorf("producer attributes: %v", p.Attributes())
	}
	if c.Name() != "orders.placed process" || p.Name() != "orders.placed publish" {
		t.Errorf("names: %q %q", p.Name(), c.Name())
	}
}

func TestKafkaTracestateSurvives(t *testing.T) {
	kafkaSetup(t)
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ts, _ := trace.ParseTraceState("vendor=abc")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, TraceState: ts}))
	var hs []KafkaHeader
	_, span := ProducerSpan(ctx, "orders.placed", &hs)
	span.End()
	got := HeaderCarrier{H: &hs}.Get("tracestate")
	if got != "vendor=abc" {
		t.Fatalf("tracestate header = %q", got)
	}
}

// Missing or malformed headers must not break consumption: a new root span, no panic.
func TestKafkaConsumerWithoutUsableHeaders(t *testing.T) {
	kafkaSetup(t)
	// A current span in ctx must not become the parent of an unrelated message.
	outer, outerSpan := otel.Tracer("t").Start(context.Background(), "poll loop")
	defer outerSpan.End()

	cases := map[string][]KafkaHeader{
		"no headers":         nil,
		"unrelated headers":  {{Key: "event-type", Value: []byte("order.placed")}},
		"garbage":            {{Key: "traceparent", Value: []byte("not-a-traceparent")}},
		"all zero ids":       {{Key: "traceparent", Value: []byte("00-00000000000000000000000000000000-0000000000000000-01")}},
		"binary":             {{Key: "traceparent", Value: []byte{0xff, 0x00, 0x13}}},
		"nil value":          {{Key: "traceparent", Value: nil}},
		"truncated":          {{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-")}},
		"unknown version ff": {{Key: "traceparent", Value: []byte("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")}},
	}
	for name, hs := range cases {
		t.Run(name, func(t *testing.T) {
			cctx, span := ConsumerSpan(outer, "order-service", "orders.placed", 0, 0, hs)
			defer EndSpan(span, nil)
			sc := trace.SpanContextFromContext(cctx)
			if !sc.IsValid() {
				t.Fatal("a root span must still exist")
			}
			if sc.TraceID() == outerSpan.SpanContext().TraceID() {
				t.Fatal("the span joined the poll loop's trace instead of starting its own")
			}
			ro := span.(sdktrace.ReadOnlySpan)
			if ro.Parent().IsValid() || ro.SpanKind() != trace.SpanKindConsumer {
				t.Fatalf("parent=%v kind=%v", ro.Parent(), ro.SpanKind())
			}
		})
	}
}

func TestEndSpanRecordsError(t *testing.T) {
	rec := kafkaSetup(t)
	_, span := otel.Tracer("t").Start(context.Background(), "x")
	EndSpan(span, context.DeadlineExceeded)
	got := rec.Ended()[0]
	if got.Status().Code.String() != "Error" || len(got.Events()) != 1 {
		t.Fatalf("status %v events %v", got.Status(), got.Events())
	}
}
