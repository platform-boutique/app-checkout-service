package checkout

import (
	"context"
	"fmt"
	"strings"
	"testing"

	pb "checkout-service/proto"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordSpans installs an in-memory tracer provider for one test and returns the recorder.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	// Back to a no-op provider, NOT to the value read before: the first SetTracerProvider of a
	// process makes the default global provider delegate to it for good, so "restoring" that
	// default would keep recording into this test's provider and change span ids for later tests.
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	return rec
}

// sagaSpans returns the saga spans by step name, failing on duplicates (one span per step).
func sagaSpans(t *testing.T, rec *tracetest.SpanRecorder) map[string]sdktrace.ReadOnlySpan {
	t.Helper()
	out := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range rec.Ended() {
		if !strings.HasPrefix(s.Name(), "saga ") {
			continue
		}
		if _, dup := out[s.Name()]; dup {
			t.Errorf("two spans named %q", s.Name())
		}
		out[s.Name()] = s
	}
	return out
}

func attrOf(s sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String(), true
		}
	}
	return "", false
}

func names(m map[string]sdktrace.ReadOnlySpan) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// placeUnderRoot runs PlaceOrder under a root span (the otelgrpc server span in production).
func placeUnderRoot(t *testing.T, svc *Service) (rootID string, err error) {
	t.Helper()
	ctx, root := otel.Tracer("test").Start(context.Background(), "PlaceOrder")
	defer root.End()
	_, err = placeOrderCtx(ctx, svc)
	return root.SpanContext().SpanID().String(), err
}

// assertNoPII fails when any span of the recording carries personal or payment data.
func assertNoPII(t *testing.T, rec *tracetest.SpanRecorder) {
	t.Helper()
	secrets := []string{"user@example.com", "4111111111111111", "1 Main St", "Testville", "Test Widget"}
	for _, s := range rec.Ended() {
		var dump []string
		dump = append(dump, s.Name(), s.Status().Description)
		for _, kv := range s.Attributes() {
			dump = append(dump, string(kv.Key), kv.Value.String())
		}
		for _, ev := range s.Events() {
			dump = append(dump, ev.Name)
			for _, kv := range ev.Attributes {
				dump = append(dump, string(kv.Key), kv.Value.String())
			}
		}
		all := strings.Join(dump, "\n")
		for _, secret := range secrets {
			if strings.Contains(all, secret) {
				t.Errorf("span %q carries %q:\n%s", s.Name(), secret, all)
			}
		}
	}
}

func TestSagaSpans_HappyPath_OneSpanPerStep_ChildrenOfPlaceOrder(t *testing.T) {
	rec := recordSpans(t)
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	rootID, err := placeUnderRoot(t, svc)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	got := sagaSpans(t, rec)
	want := []string{"saga prepare", "saga create_order", "saga reserve_stock", "saga charge", "saga ship", "saga commit_stock", "saga confirm_order", "saga empty_cart", "saga send_confirmation"}
	for _, name := range want {
		s, ok := got[name]
		if !ok {
			t.Errorf("missing span %q (have %v)", name, names(got))
			continue
		}
		if s.Parent().SpanID().String() != rootID {
			t.Errorf("%s is not a child of PlaceOrder", name)
		}
		if s.Status().Code == otelcodes.Error {
			t.Errorf("%s has error status on the happy path: %q", name, s.Status().Description)
		}
		if v, _ := attrOf(s, "checkout.compensation"); v != "false" {
			t.Errorf("%s checkout.compensation=%q", name, v)
		}
		if id, _ := attrOf(s, "checkout.order_id"); id != m.orders.creates[0].GetOrderId() {
			t.Errorf("%s checkout.order_id=%q, want the stored order id", name, id)
		}
	}
	if len(got) != len(want) {
		t.Errorf("saga spans %v, want exactly %v (no compensation on the happy path)", names(got), want)
	}
	if v, _ := attrOf(got["saga reserve_stock"], "checkout.items"); v != "1" {
		t.Errorf("reserve_stock checkout.items=%q", v)
	}
	assertNoPII(t, rec)
}

func TestSagaSpans_ShipFails_CompensationSpansAndErrorStatus(t *testing.T) {
	rec := recordSpans(t)
	m := sagaMocks()
	m.shipping.shipErr = status.Error(codes.Unavailable, "carrier down for user@example.com")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	rootID, err := placeUnderRoot(t, svc)
	if err == nil {
		t.Fatal("expected the order to fail")
	}

	got := sagaSpans(t, rec)
	ship := got["saga ship"]
	if ship == nil || ship.Status().Code != otelcodes.Error || ship.Status().Description != "Unavailable" {
		t.Fatalf("ship span: %v", ship)
	}
	if len(ship.Events()) != 1 || ship.Events()[0].Name != "exception" {
		t.Errorf("ship span events: %v", ship.Events())
	}
	for _, name := range []string{"saga refund", "saga release_stock", "saga fail_order"} {
		s, ok := got[name]
		if !ok {
			t.Errorf("missing compensation span %q (have %v)", name, names(got))
			continue
		}
		if v, _ := attrOf(s, "checkout.compensation"); v != "true" {
			t.Errorf("%s checkout.compensation=%q", name, v)
		}
		if s.Parent().SpanID().String() != rootID {
			t.Errorf("%s is not a child of PlaceOrder", name)
		}
		if s.Status().Code == otelcodes.Error {
			t.Errorf("%s failed: %q", name, s.Status().Description)
		}
	}
	for _, name := range []string{"saga commit_stock", "saga confirm_order", "saga empty_cart"} {
		if _, ok := got[name]; ok {
			t.Errorf("span %q exists for an order that failed", name)
		}
	}
	// The downstream message ("... user@example.com") must not reach the trace, only the code.
	assertNoPII(t, rec)
}

func TestSagaSpans_ChargeDeclined_NoShipSpan_ReleaseButNoRefund(t *testing.T) {
	rec := recordSpans(t)
	m := sagaMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: Your card 4111111111111111 was declined")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	if _, err := placeUnderRoot(t, svc); err == nil {
		t.Fatal("expected the order to fail")
	}

	got := sagaSpans(t, rec)
	if s := got["saga charge"]; s == nil || s.Status().Description != "FailedPrecondition" {
		t.Fatalf("charge span: %v", s)
	}
	for _, name := range []string{"saga ship", "saga refund"} {
		if _, ok := got[name]; ok {
			t.Errorf("unexpected span %q", name)
		}
	}
	for _, name := range []string{"saga release_stock", "saga fail_order"} {
		if _, ok := got[name]; !ok {
			t.Errorf("missing %q", name)
		}
	}
	assertNoPII(t, rec)
}

func TestSagaSpans_OutOfStock_BusinessAnswerIsNotAnError(t *testing.T) {
	rec := recordSpans(t)
	m := sagaMocks()
	m.inventory.stock = map[string]int32{"prod-1": 0}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	if _, err := placeUnderRoot(t, svc); err == nil {
		t.Fatal("expected out of stock")
	}

	s := sagaSpans(t, rec)["saga reserve_stock"]
	if s == nil || s.Status().Code == otelcodes.Error {
		t.Fatalf("reserve span: %v", s)
	}
	if v, _ := attrOf(s, "checkout.outcome"); v != "out_of_stock" {
		t.Errorf("checkout.outcome=%q", v)
	}
	assertNoPII(t, rec)
}

func TestErrCode_ContextErrorsMapLikeGRPC(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{context.DeadlineExceeded, "DeadlineExceeded"},
		{fmt.Errorf("wrapped: %w", context.Canceled), "Canceled"},
		{status.Error(codes.NotFound, "x"), "NotFound"},
		{newCallError("payment", fmt.Errorf("w: %w", status.Error(codes.Unavailable, "x"))), "Unavailable"},
		{fmt.Errorf("plain"), "Unknown"},
	} {
		if got := errCode(c.err); got != c.want {
			t.Errorf("errCode(%v)=%q want %q", c.err, got, c.want)
		}
	}
}

func placeOrderCtx(ctx context.Context, svc *Service) (*pb.PlaceOrderResponse, error) {
	return svc.PlaceOrder(ctx, &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "USD", Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	})
}
