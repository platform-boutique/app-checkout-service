package checkout

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// OBS-61: the record that goes to the broker carries the W3C trace context of a PRODUCER
// span that is a child of the PlaceOrder span, and keeps the event headers.
func TestBuildRecordInjectsTraceContext(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	defer func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) }()

	ctx, root := otel.Tracer("t").Start(context.Background(), "PlaceOrder")
	msg := Message{Key: "o1", Value: []byte(`{}`), Headers: []Header{{Key: "event-type", Value: eventType}, {Key: "schema-version", Value: "1"}}}
	_, rec, span := buildRecord(ctx, msg)
	span.End()
	root.End()

	got := map[string]string{}
	for _, h := range rec.Headers {
		got[h.Key] = string(h.Value)
	}
	if got["event-type"] != eventType || got["schema-version"] != "1" {
		t.Fatalf("event headers lost: %v", got)
	}
	tp := got["traceparent"]
	want := "00-" + root.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String() + "-01"
	if tp != want {
		t.Fatalf("traceparent %q, want %q", tp, want)
	}
	prod := spans.Ended()[0]
	if prod.SpanKind() != trace.SpanKindProducer || prod.Parent().SpanID() != root.SpanContext().SpanID() || prod.Name() != "orders.placed publish" {
		t.Fatalf("producer span: kind=%v parent=%v name=%q", prod.SpanKind(), prod.Parent(), prod.Name())
	}
}

// Without telemetry (no SDK) the headers still carry nothing invalid: no traceparent at all.
func TestBuildRecordWithoutActiveSpanAddsNoTraceparent(t *testing.T) {
	_, rec, span := buildRecord(context.Background(), Message{Key: "o1", Headers: []Header{{Key: "event-type", Value: eventType}}})
	span.End()
	for _, h := range rec.Headers {
		if h.Key == "traceparent" && string(h.Value) == "00-00000000000000000000000000000000-0000000000000000-00" {
			t.Fatal("all-zero traceparent injected")
		}
	}
}
