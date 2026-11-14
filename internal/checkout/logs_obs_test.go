package checkout

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"checkout-service/internal/obs"
	pb "checkout-service/proto"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OBS-30: every line the request path writes carries the trace and span id of the request,
// in the contract's JSON shape, and none shows the customer's e-mail address.
func TestPlaceOrder_LogsAreJSONWithTraceIDsAndNoEmail(t *testing.T) {
	var buf bytes.Buffer
	oldOut, oldFmt := log.Out, log.Formatter
	log.SetOutput(&buf)
	log.SetFormatter(obs.NewFormatter("checkout-service", "test"))
	defer func() { log.SetOutput(oldOut); log.SetFormatter(oldFmt) }()

	m := defaultMocks()
	m.shipping.shipErr = status.Error(codes.InvalidArgument, "invalid address: city is required")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	ctx = obs.WithRequestID(ctx, "req-7")
	_, err := svc.PlaceOrder(ctx, &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "USD", Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	})
	if err == nil {
		t.Fatal("expected the shipping failure")
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected the request path to log, got:\n%s", buf.String())
	}
	sawError := false
	for _, l := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("not JSON: %q", l)
		}
		if m["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" || m["span_id"] != "00f067aa0ba902b7" || m["request_id"] != "req-7" {
			t.Errorf("line without the request's ids: %s", l)
		}
		if m["service"] != "checkout-service" || m["severity"] == nil || m["timestamp"] == nil || m["message"] == nil {
			t.Errorf("shape: %s", l)
		}
		if m["severity"] == "error" {
			sawError = true
			if e, _ := m["error"].(string); !strings.Contains(e, "invalid address") {
				t.Errorf("error line without the error field: %s", l)
			}
		}
	}
	if !sawError {
		t.Errorf("shipping failure not logged at error level:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), "user@example.com") || strings.Contains(buf.String(), defaultCard().CreditCardNumber) {
		t.Errorf("PII in the logs:\n%s", buf.String())
	}
}
