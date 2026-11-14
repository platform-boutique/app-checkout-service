package checkout

import (
	"context"
	"path"
	"sync"
	"testing"
	"time"

	pb "checkout-service/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// callLog records, per RPC method, the deadline the callee saw (CODE-05). gRPC carries the
// caller's deadline in the grpc-timeout header, so what the SERVER sees is what the wire says.
type callLog struct {
	mu    sync.Mutex
	calls []seenCall
}

type seenCall struct {
	method    string // short name, e.g. "Charge"
	hasDL     bool
	remaining time.Duration
}

func (l *callLog) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	c := seenCall{method: path.Base(info.FullMethod)}
	if dl, ok := ctx.Deadline(); ok {
		c.hasDL, c.remaining = true, time.Until(dl)
	}
	l.mu.Lock()
	l.calls = append(l.calls, c)
	l.mu.Unlock()
	return h(ctx, req)
}

func (l *callLog) snapshot() []seenCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]seenCall(nil), l.calls...)
}

// Every outgoing call of a full order (all backends on, Kafka off so email-service is called)
// carries a deadline even when the caller set none, and it never exceeds the documented bound.
func TestDeadlines_EveryOutgoingCallHasABoundedDeadline(t *testing.T) {
	m := sagaMocks()
	m.calls = &callLog{}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	// context.Background(): no caller deadline at all.
	if _, err := placeOrder(t, svc); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	bound := map[string]time.Duration{
		"GetCart": readTimeout, "EmptyCart": readTimeout, "GetProduct": readTimeout, "Convert": readTimeout, "GetQuote": readTimeout,
		"Charge": chargeTimeout, "ShipOrder": shipTimeout, "SendOrderConfirmation": emailTimeout,
		"CreateOrder": createOrderTimeout, "Reserve": reserveTimeout, "Commit": commitTimeout, "ConfirmOrder": confirmTimeout,
	}
	seen := map[string]bool{}
	for _, c := range m.calls.snapshot() {
		seen[c.method] = true
		want, ok := bound[c.method]
		if !ok {
			t.Errorf("call %s is not in the bound table (new outgoing call: give it a deadline and list it here)", c.method)
			continue
		}
		if !c.hasDL {
			t.Errorf("%s was called without a deadline", c.method)
		} else if c.remaining <= 0 || c.remaining > want {
			t.Errorf("%s deadline remaining %v, want in (0, %v]", c.method, c.remaining, want)
		}
	}
	for method := range bound {
		if !seen[method] {
			t.Errorf("%s was never called, the table and the flow disagree", method)
		}
	}
}

// The compensation calls run on a detached context but are bounded too.
func TestDeadlines_CompensationCallsAreBounded(t *testing.T) {
	m := sagaMocks()
	m.calls = &callLog{}
	m.shipping.shipErr = status.Error(codes.Unavailable, "carrier down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, _ = placeOrder(t, svc)

	bound := map[string]time.Duration{"Refund": refundTimeout, "Release": stockTimeout, "FailOrder": settleOrderTimeout}
	seen := map[string]bool{}
	for _, c := range m.calls.snapshot() {
		want, ok := bound[c.method]
		if !ok {
			continue
		}
		seen[c.method] = true
		if !c.hasDL || c.remaining <= 0 || c.remaining > want {
			t.Errorf("%s: hasDeadline=%v remaining=%v, want in (0, %v]", c.method, c.hasDL, c.remaining, want)
		}
	}
	for method := range bound {
		if !seen[method] {
			t.Errorf("%s was never called", method)
		}
	}
}

// A caller deadline that is shorter than the bound wins: the bound only ever shortens.
func TestDeadlines_ShorterCallerDeadlineWins(t *testing.T) {
	m := defaultMocks()
	m.calls = &callLog{}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := svc.PlaceOrder(ctx, &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "USD", Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	}); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	for _, c := range m.calls.snapshot() {
		if !c.hasDL || c.remaining > 1*time.Second {
			t.Errorf("%s: remaining %v exceeds the caller's 1 s deadline", c.method, c.remaining)
		}
	}
}

// withShortTimeouts shrinks the bounds for one test. The hung backend blocks until the bound
// fires, so no test sleeps: it waits for the context, with a generous guard against a hang.
func withShortTimeouts(t *testing.T) {
	t.Helper()
	r, c := readTimeout, chargeTimeout
	readTimeout, chargeTimeout = 150*time.Millisecond, 150*time.Millisecond
	t.Cleanup(func() { readTimeout, chargeTimeout = r, c })
}

func placeWithGuard(t *testing.T, svc *Service) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := placeOrder(t, svc); done <- err }()
	select {
	case err := <-done:
		return err
	case <-time.After(60 * time.Second): // a generous guard: the bound under test is 150 ms
		t.Fatal("PlaceOrder did not return: a hung backend holds the request (no deadline)")
		return nil
	}
}

func TestDeadlines_HungCartService_FailsUnavailable_NothingStoredOrCharged(t *testing.T) {
	withShortTimeouts(t)
	m := ordersMocks()
	m.cart.hang = true
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	err := placeWithGuard(t, svc)

	assertStatus(t, err, codes.Unavailable, "cart service is temporarily unavailable")
	if m.payment.charges() != 0 {
		t.Error("charged although the cart never answered")
	}
	if creates, _, _ := m.orders.counts(); creates != 0 {
		t.Error("order stored although the cart never answered")
	}
}

func TestDeadlines_HungPayment_FailsUnavailable_StockReleased_OrderFailed_NotShipped(t *testing.T) {
	withShortTimeouts(t)
	m := sagaMocks()
	m.payment.hang = true
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	err := placeWithGuard(t, svc)

	assertStatus(t, err, codes.Unavailable, "payment service is temporarily unavailable")
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "Release", "FailOrder")
	if m.shipping.shipped() != 0 {
		t.Error("shipped after a charge that did not answer")
	}
}
