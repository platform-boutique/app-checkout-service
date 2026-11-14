package checkout

import (
	"strings"
	"sync"
	"testing"
	"time"

	pb "checkout-service/proto"
	orderspb "checkout-service/proto/orders"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// replicaOf returns a second Service instance (its own in-process guard) wired to the very same
// downstream connections, i.e. another checkout replica talking to the same order-service.
func replicaOf(s *Service) *Service {
	return &Service{
		ProductCatalogConn: s.ProductCatalogConn, CartConn: s.CartConn, CurrencyConn: s.CurrencyConn,
		ShippingConn: s.ShippingConn, PaymentConn: s.PaymentConn, EmailConn: s.EmailConn,
		OrderConn: s.OrderConn, InventoryConn: s.InventoryConn,
	}
}

func keyedOrderID(key string) string {
	return idempotentOrderID(&pb.PlaceOrderRequest{UserId: "user-1", IdempotencyKey: key})
}

// ─── Order.created: the cross-replica race ───────────────────────────────────

// Two replicas, one order-service, the same key. Both GetOrder lookups miss (neither order
// exists yet), both reach CreateOrder; only one is told created=true. The other must stop
// without reserving, charging or shipping. Exactly one charge.
func TestCreated_TwoReplicasSameKey_ExactlyOneCharge(t *testing.T) {
	m := sagaMocks()
	m.orders.getMisses = 2
	var arrived sync.WaitGroup
	arrived.Add(2)
	m.orders.onGetMiss = func() { arrived.Done(); arrived.Wait() } // both are past the lookup before either creates
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	m.payment.onCharge = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	svcA, cleanup := startMockServers(t, m)
	defer cleanup()
	svcB := replicaOf(svcA)

	type result struct {
		resp *pb.PlaceOrderResponse
		err  error
	}
	results := make(chan result, 2)
	for _, svc := range []*Service{svcA, svcB} {
		go func(svc *Service) {
			r, err := placeKeyed(svc, "two-replicas")
			results <- result{r, err}
		}(svc)
	}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	<-entered // the first one is inside Charge
	var loser result
	select {
	case loser = <-results:
	case <-time.After(10 * time.Second):
		unblock()
		t.Fatal("the second replica did not stop: it is waiting in Charge too (created=false was not honoured)")
	}
	assertStatus(t, loser.err, codes.Aborted, "this order is already being processed")
	unblock()
	winner := <-results
	if winner.err != nil {
		t.Fatalf("winner: %v", winner.err)
	}

	if m.payment.charges() != 1 || m.shipping.shipped() != 1 {
		t.Errorf("charges=%d ships=%d, want exactly 1", m.payment.charges(), m.shipping.shipped())
	}
	if rs, cm, rel := m.inventory.counts(); rs != 1 || cm != 1 || rel != 0 {
		t.Errorf("reserve/commit/release = %d/%d/%d, want 1/1/0", rs, cm, rel)
	}
	if cr, co, fa := m.orders.counts(); cr != 2 || co != 1 || fa != 0 {
		t.Errorf("create/confirm/fail = %d/%d/%d, want 2/1/0 (the loser still called CreateOrder)", cr, co, fa)
	}
	if m.cart.emptyCalls != 1 || m.email.calls != 1 {
		t.Errorf("cart emptied %d, emails %d, want 1/1", m.cart.emptyCalls, m.email.calls)
	}
}

// created=false must route through the same branches as the GetOrder-first path.
func TestCreated_FalseOnExistingOrder_TakesReplayBranches(t *testing.T) {
	const key = "k1"
	cases := []struct {
		name   string
		stored *orderspb.Order
		check  func(t *testing.T, resp *pb.PlaceOrderResponse, err error)
	}{
		{
			name:   "PENDING -> ABORTED",
			stored: &orderspb.Order{Status: orderspb.OrderStatus_ORDER_STATUS_PENDING},
			check: func(t *testing.T, _ *pb.PlaceOrderResponse, err error) {
				assertStatus(t, err, codes.Aborted, "this order is already being processed")
			},
		},
		{
			name:   "FAILED -> FAILED_PRECONDITION",
			stored: &orderspb.Order{Status: orderspb.OrderStatus_ORDER_STATUS_FAILED, FailureReason: "card declined"},
			check: func(t *testing.T, _ *pb.PlaceOrderResponse, err error) {
				assertStatus(t, err, codes.FailedPrecondition, "this attempt failed, start the checkout again")
			},
		},
		{
			name: "PLACED -> stored result",
			stored: &orderspb.Order{
				Status: orderspb.OrderStatus_ORDER_STATUS_PLACED, ShippingTrackingId: "TRK-OLD",
				ShippingCost:    &orderspb.Money{CurrencyCode: "USD", Units: 5},
				ShippingAddress: &orderspb.Address{StreetAddress: "1 Main St", City: "Testville", Country: "US", ZipCode: 10001},
				Items: []*orderspb.OrderItem{{ProductId: "prod-1", Name: "Test Widget", Quantity: 2,
					UnitPrice: &orderspb.Money{CurrencyCode: "USD", Units: 10}}},
			},
			check: func(t *testing.T, resp *pb.PlaceOrderResponse, err error) {
				if err != nil {
					t.Fatalf("replay of a PLACED order: %v", err)
				}
				if o := resp.GetOrder(); o.GetOrderId() != keyedOrderID(key) || o.GetShippingTrackingId() != "TRK-OLD" || len(o.GetItems()) != 1 {
					t.Errorf("stored result = %v", o)
				}
			},
		},
		{
			name:   "SHIPPED (computed status) -> stored result",
			stored: &orderspb.Order{Status: orderspb.OrderStatus_ORDER_STATUS_SHIPPED, ShippingTrackingId: "TRK-OLD"},
			check: func(t *testing.T, resp *pb.PlaceOrderResponse, err error) {
				if err != nil || resp.GetOrder().GetShippingTrackingId() != "TRK-OLD" {
					t.Errorf("resp %v err %v", resp, err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := sagaMocks()
			id := keyedOrderID(key)
			tc.stored.OrderId, tc.stored.SessionId = id, "user-1"
			m.orders.stored = map[string]*orderspb.Order{id: tc.stored}
			m.orders.getMisses = 1 // the lookup runs before "the other replica" created it
			svc, cleanup := startMockServers(t, m)
			defer cleanup()

			resp, err := placeKeyed(svc, key)

			tc.check(t, resp, err)
			assertEvents(t, m, "GetOrder", "CreateOrder")
			if m.payment.charges() != 0 || m.shipping.shipped() != 0 || m.cart.emptyCalls != 0 || m.email.calls != 0 {
				t.Errorf("charges=%d ships=%d empties=%d emails=%d, want all 0", m.payment.charges(), m.shipping.shipped(), m.cart.emptyCalls, m.email.calls)
			}
			if rs, cm, rel := m.inventory.counts(); rs != 0 || cm != 0 || rel != 0 {
				t.Errorf("inventory reserve/commit/release = %d/%d/%d, want 0/0/0", rs, cm, rel)
			}
			if _, co, fa := m.orders.counts(); co != 0 || fa != 0 {
				t.Errorf("confirm/fail = %d/%d, want 0/0", co, fa)
			}
		})
	}
}

// An order-service that predates Order.created never sets the field: unset means "unknown", so checkout
// goes on exactly as before (the GetOrder lookup is the only protection). The same stored PENDING order that
// the test above turns into ABORTED is therefore NOT stopped here: this documents the remaining gap.
func TestCreated_UnsetFromOldOrderService_OldBehaviour(t *testing.T) {
	m := sagaMocks()
	m.orders.omitCreated = true
	m.orders.getMisses = 1
	id := keyedOrderID("k1")
	m.orders.stored = map[string]*orderspb.Order{id: {OrderId: id, SessionId: "user-1", Status: orderspb.OrderStatus_ORDER_STATUS_PENDING}}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	if _, err := placeKeyed(svc, "k1"); err != nil {
		t.Fatalf("old order-service: the checkout must proceed as before: %v", err)
	}
	assertEvents(t, m, "GetOrder", "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "ConfirmOrder")
}

// Old order-service, normal keyed flows: new order, sequential replay, still correct.
func TestCreated_OldOrderService_KeyedFlowsUnchanged(t *testing.T) {
	m := sagaMocks()
	m.orders.omitCreated = true
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	first, err := placeKeyed(svc, "k1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := placeKeyed(svc, "k1")
	if err != nil || second.GetOrder().GetOrderId() != first.GetOrder().GetOrderId() {
		t.Fatalf("replay: %v %v", second, err)
	}
	if m.payment.charges() != 1 {
		t.Errorf("charges = %d, want 1", m.payment.charges())
	}
}

// created=true (a new order) goes on with the saga as before.
func TestCreated_TrueGoesOn(t *testing.T) {
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	if _, err := placeKeyed(svc, "fresh"); err != nil {
		t.Fatal(err)
	}
	assertEvents(t, m, "GetOrder", "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "ConfirmOrder")
}

// Without a key the order id is random, created is never consulted.
func TestCreated_FalseWithoutKeyIsIgnored(t *testing.T) {
	m := sagaMocks()
	m.orders.stored = map[string]*orderspb.Order{}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "ConfirmOrder")
}

// ─── ConfirmOrder retries ────────────────────────────────────────────────────

func TestConfirm_SucceedsAfterTransientFailures(t *testing.T) {
	for _, failures := range []int{1, 2} {
		m := sagaMocks()
		m.orders.confirmFailures = failures
		svc, cleanup := startMockServers(t, m)
		logs := captureLogs(t)

		resp, err := placeOrder(t, svc)

		if err != nil {
			t.Fatalf("failures=%d: %v", failures, err)
		}
		if _, co, fa := m.orders.counts(); co != failures+1 || fa != 0 {
			t.Errorf("failures=%d: confirm attempts=%d fail=%d, want %d/0", failures, co, fa, failures+1)
		}
		if st := m.orders.stored[resp.GetOrder().GetOrderId()].GetStatus(); st != orderspb.OrderStatus_ORDER_STATUS_PLACED {
			t.Errorf("failures=%d: stored status %v, want PLACED", failures, st)
		}
		if strings.Contains(logs.String(), "ORDER NOT CONFIRMED") {
			t.Errorf("failures=%d: logged a confirm failure although a retry worked", failures)
		}
		cleanup()
	}
}

func TestConfirm_FailsAlways_OriginalFlowUnchanged_Logged(t *testing.T) {
	m := sagaMocks()
	m.orders.confirmErr = status.Error(codes.Unavailable, "order db down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	logs := captureLogs(t)

	resp, err := placeOrder(t, svc)

	if err != nil {
		t.Fatalf("a confirm failure must not fail the order: %v", err)
	}
	id := resp.GetOrder().GetOrderId()
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "ConfirmOrder", "ConfirmOrder", "ConfirmOrder")
	if st := m.orders.stored[id].GetStatus(); st != orderspb.OrderStatus_ORDER_STATUS_PENDING {
		t.Errorf("stored status %v, want PENDING", st)
	}
	if len(m.payment.refundRequests()) != 0 || m.cart.emptyCalls != 1 || m.email.calls != 1 {
		t.Errorf("refunds=%d empties=%d emails=%d", len(m.payment.refundRequests()), m.cart.emptyCalls, m.email.calls)
	}
	if rs, cm, rel := m.inventory.counts(); rs != 1 || cm != 1 || rel != 0 {
		t.Errorf("reserve/commit/release = %d/%d/%d", rs, cm, rel)
	}
	out := logs.String()
	for _, want := range []string{"level=error", "ORDER NOT CONFIRMED order=" + id} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
}

// A final answer (the order was failed or never existed) is not worth retrying.
func TestConfirm_FinalAnswerIsNotRetried(t *testing.T) {
	m := sagaMocks()
	m.orders.confirmErr = status.Error(codes.FailedPrecondition, "order is FAILED")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	logs := captureLogs(t)

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	if _, co, _ := m.orders.counts(); co != 1 {
		t.Errorf("confirm attempts = %d, want 1", co)
	}
	if !strings.Contains(logs.String(), "ORDER NOT CONFIRMED order=") {
		t.Errorf("not logged:\n%s", logs)
	}
}

// ─── Commit retries ──────────────────────────────────────────────────────────

func TestCommit_SucceedsOnThirdAttempt(t *testing.T) {
	m := sagaMocks()
	m.inventory.commitFailures = 2
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	logs := captureLogs(t)

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	if _, commits, _ := m.inventory.counts(); commits != 3 {
		t.Errorf("commit attempts = %d, want 3", commits)
	}
	if strings.Contains(logs.String(), "STOCK NOT COMMITTED") {
		t.Error("logged a commit failure although the third attempt worked")
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "Commit", "Commit", "ConfirmOrder")
}
