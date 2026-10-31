package checkout

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	pb "checkout-service/proto"
	inventorypb "checkout-service/proto/inventory"
	orderspb "checkout-service/proto/orders"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockInventoryServer is a fake inventory-service with a stock map. Reserve is
// all-or-nothing, like the real one; every call is recorded.
type mockInventoryServer struct {
	inventorypb.UnimplementedInventoryServiceServer
	rec *recorder

	stock          map[string]int32 // nil entry = unknown product (available 0)
	reserveErr     error
	commitErr      error
	commitFailures int // the first N Commit calls fail with Unavailable
	releaseErr     error

	mu       sync.Mutex
	reserves []*inventorypb.ReserveRequest
	commits  int
	releases int
}

func (m *mockInventoryServer) Reserve(_ context.Context, req *inventorypb.ReserveRequest) (*inventorypb.ReserveResponse, error) {
	m.rec.add("Reserve")
	m.mu.Lock()
	m.reserves = append(m.reserves, req)
	m.mu.Unlock()
	if m.reserveErr != nil {
		return nil, m.reserveErr
	}
	var short []*inventorypb.Shortage
	for _, it := range req.GetItems() {
		if have := m.stock[it.GetProductId()]; have < it.GetQuantity() {
			short = append(short, &inventorypb.Shortage{ProductId: it.GetProductId(), Requested: it.GetQuantity(), Available: have})
		}
	}
	return &inventorypb.ReserveResponse{Reserved: len(short) == 0, Shortages: short}, nil
}

func (m *mockInventoryServer) Commit(_ context.Context, _ *inventorypb.CommitRequest) (*inventorypb.CommitResponse, error) {
	m.rec.add("Commit")
	m.mu.Lock()
	m.commits++
	n := m.commits
	m.mu.Unlock()
	if n <= m.commitFailures {
		return nil, status.Error(codes.Unavailable, "blip")
	}
	if m.commitErr != nil {
		return nil, m.commitErr
	}
	return &inventorypb.CommitResponse{}, nil
}

func (m *mockInventoryServer) Release(_ context.Context, _ *inventorypb.ReleaseRequest) (*inventorypb.ReleaseResponse, error) {
	m.rec.add("Release")
	m.mu.Lock()
	m.releases++
	m.mu.Unlock()
	if m.releaseErr != nil {
		return nil, m.releaseErr
	}
	return &inventorypb.ReleaseResponse{}, nil
}

func (m *mockInventoryServer) counts() (reserves, commits, releases int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.reserves), m.commits, m.releases
}

// sagaMocks is ordersMocks plus an inventory with plenty of stock, on the same recorder.
func sagaMocks() *mocks {
	m := ordersMocks()
	m.inventory = &mockInventoryServer{rec: m.orders.rec, stock: map[string]int32{"prod-1": 100}}
	return m
}

func init() {
	// Fast retries in tests.
	stockBackoff = []time.Duration{time.Millisecond, time.Millisecond}
}

func placeKeyed(svc *Service, key string) (*pb.PlaceOrderResponse, error) {
	return svc.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "USD", Address: defaultAddress(), Email: "user@example.com",
		CreditCard: defaultCard(), IdempotencyKey: key,
	})
}

// ─── Saga: happy path and failure steps ──────────────────────────────────────

func TestSaga_HappyPath_ReserveBeforeCharge_CommitAfterShip(t *testing.T) {
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	resp, err := placeOrder(t, svc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "ConfirmOrder")
	r := m.inventory.reserves[0]
	if r.GetOrderId() != resp.GetOrder().GetOrderId() || r.GetOrderId() != m.orders.creates[0].GetOrderId() {
		t.Errorf("reserve order id %q, result %q, created %q", r.GetOrderId(), resp.GetOrder().GetOrderId(), m.orders.creates[0].GetOrderId())
	}
	if len(r.GetItems()) != 1 || r.GetItems()[0].GetProductId() != "prod-1" || r.GetItems()[0].GetQuantity() != 2 {
		t.Errorf("reserve items = %v", r.GetItems())
	}
	if m.cart.emptyCalls != 1 || m.email.calls != 1 {
		t.Errorf("cart emptied %d, emails %d", m.cart.emptyCalls, m.email.calls)
	}
}

func TestSaga_ReserveShort_FailedPrecondition_NothingCharged(t *testing.T) {
	m := sagaMocks()
	m.productCatalog.byID = map[string]*pb.Product{
		"scarf": {Id: "scarf", Name: "Wool Scarf", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 20}},
		"tote":  {Id: "tote", Name: "Tote Bag", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 12}},
		"mug":   {Id: "mug", Name: "Mug", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 8}},
	}
	m.cart.cart = &pb.Cart{UserId: "user-1", Items: []*pb.CartItem{
		{ProductId: "scarf", Quantity: 3}, {ProductId: "mug", Quantity: 1}, {ProductId: "tote", Quantity: 1},
	}}
	m.inventory.stock = map[string]int32{"scarf": 2, "mug": 10} // tote unknown/sold out
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	want := `out of stock: "Wool Scarf" (only 2 left, you asked for 3), "Tote Bag" (sold out)`
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != want {
		t.Fatalf("err = %v, want FailedPrecondition %q", err, want)
	}
	// Nothing was held, so no Release; the order is marked FAILED with the same message.
	assertEvents(t, m, "CreateOrder", "Reserve", "FailOrder")
	if m.payment.charges() != 0 || m.shipping.shipped() != 0 || m.cart.emptyCalls != 0 || m.email.calls != 0 {
		t.Error("something happened after the shortage")
	}
	if got := m.orders.fails[0].GetReason(); got != want {
		t.Errorf("FailOrder reason = %q", got)
	}
}

func TestSaga_ReserveRPCError_FailClosed_Unavailable(t *testing.T) {
	m := sagaMocks()
	m.inventory.reserveErr = status.Error(codes.Unavailable, "connection refused")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.Unavailable, "stock check unavailable, try again")
	// Release is called because the Reserve may have arrived although the answer did not.
	assertEvents(t, m, "CreateOrder", "Reserve", "Release", "FailOrder")
	if m.payment.charges() != 0 {
		t.Error("charged without a reservation")
	}
}

func TestSaga_ChargeDeclined_ReleaseNoRefund(t *testing.T) {
	m := sagaMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: Your card was declined")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.FailedPrecondition, "card declined")
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "Release", "FailOrder")
	if len(m.payment.refundRequests()) != 0 {
		t.Error("refund after a decline")
	}
}

func TestSaga_ShipFails_RefundReleaseFailOrder(t *testing.T) {
	m := sagaMocks()
	m.shipping.shipErr = status.Error(codes.Unavailable, "carrier down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.Unavailable, "refunded")
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Refund", "Release", "FailOrder")
	if _, commits, _ := m.inventory.counts(); commits != 0 {
		t.Error("committed stock of a failed order")
	}
	if got := m.orders.fails[0].GetReason(); !strings.Contains(got, "refunded") {
		t.Errorf("FailOrder reason %q lacks the refund note", got)
	}
}

func TestSaga_CommitFailsThreeTimes_StillConfirms_Logged(t *testing.T) {
	m := sagaMocks()
	m.inventory.commitErr = status.Error(codes.Unavailable, "inventory down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	logs := captureLogs(t)

	resp, err := placeOrder(t, svc)

	if err != nil {
		t.Fatalf("a commit failure must not fail the order: %v", err)
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "Commit", "Commit", "ConfirmOrder")
	want := "STOCK NOT COMMITTED order=" + resp.GetOrder().GetOrderId()
	if !strings.Contains(logs.String(), want) || !strings.Contains(logs.String(), `level=error`) {
		t.Errorf("log lacks %q at error level:\n%s", want, logs)
	}
	if m.cart.emptyCalls != 1 || m.email.calls != 1 {
		t.Error("cart/email skipped after a commit failure")
	}
}

func TestSaga_CommitSucceedsOnSecondAttempt(t *testing.T) {
	m := sagaMocks()
	m.inventory.commitFailures = 1
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	logs := captureLogs(t)

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	if _, commits, _ := m.inventory.counts(); commits != 2 {
		t.Errorf("commit attempts = %d, want 2", commits)
	}
	if strings.Contains(logs.String(), "STOCK NOT COMMITTED") {
		t.Error("logged a commit failure although the retry worked")
	}
}

// A failing Release must not replace the original error, and FailOrder still runs.
func TestSaga_ReleaseFails_OriginalErrorKept_FailOrderStillCalled(t *testing.T) {
	m := sagaMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: nope")
	m.inventory.releaseErr = status.Error(codes.Internal, "boom")
	m.orders.failErr = status.Error(codes.Unavailable, "order-service down too")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	logs := captureLogs(t)

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.FailedPrecondition, "card declined")
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "Release", "Release", "Release", "FailOrder") // 3 attempts, then FailOrder
	if !strings.Contains(logs.String(), "STOCK NOT RELEASED") {
		t.Errorf("log lacks STOCK NOT RELEASED:\n%s", logs)
	}
}

func TestSaga_ReleaseRetriedOnTransientError(t *testing.T) {
	m := sagaMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: nope")
	m.inventory.releaseErr = status.Error(codes.Unavailable, "blip")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	_ = captureLogs(t)

	_, _ = placeOrder(t, svc)

	if _, _, rel := m.inventory.counts(); rel != stockAttempts {
		t.Errorf("release attempts = %d, want %d", rel, stockAttempts)
	}
}

// The customer closes the tab while the card is being charged: the request
// context is cancelled, Release and FailOrder must still reach their services.
func TestSaga_ClientDisconnect_ReleaseUsesFreshContext(t *testing.T) {
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	m.payment.onCharge = cancel

	_, err := svc.PlaceOrder(ctx, &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "USD", Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	})

	assertStatus(t, err, codes.Canceled)
	if ctx.Err() == nil {
		t.Fatal("test bug: context not cancelled")
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "Release", "FailOrder")
}

func TestSaga_DuplicateCartLinesAreSummed(t *testing.T) {
	m := sagaMocks()
	m.cart.cart = &pb.Cart{UserId: "user-1", Items: []*pb.CartItem{{ProductId: "prod-1", Quantity: 2}, {ProductId: "prod-1", Quantity: 3}}}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	items := m.inventory.reserves[0].GetItems()
	if len(items) != 1 || items[0].GetQuantity() != 5 {
		t.Errorf("reserve items = %v, want one line of 5", items)
	}
}

func TestSaga_InventoryWithoutOrderStorage(t *testing.T) {
	m := defaultMocks()
	rec := &recorder{}
	m.payment.rec, m.shipping.rec = rec, rec
	m.inventory = &mockInventoryServer{rec: rec, stock: map[string]int32{"prod-1": 5}}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	resp, err := placeOrder(t, svc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rec.list(), []string{"Reserve", "Charge", "ShipOrder", "Commit"}; !reflect.DeepEqual(got, want) {
		t.Errorf("events = %v, want %v", got, want)
	}
	if m.inventory.reserves[0].GetOrderId() != resp.GetOrder().GetOrderId() {
		t.Error("reserve order id differs from the result")
	}
}

// No INVENTORY_SERVICE_ADDR: exactly the old behaviour, inventory never contacted.
func TestSaga_NoInventoryConfigured_OldBehaviour(t *testing.T) {
	m := ordersMocks() // inventory == nil
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	if svc.InventoryConn != nil {
		t.Fatal("test bug: inventory configured")
	}

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	assertEvents(t, m, "CreateOrder", "Charge", "ShipOrder", "ConfirmOrder")
}

// ─── Idempotency ─────────────────────────────────────────────────────────────

func TestIdempotency_ReplayOfPlacedOrder_NoSecondCharge(t *testing.T) {
	m := sagaMocks()
	m.payment.transactionID = "TX-1"
	m.shipping.trackingID = "TRK-1"
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	first, err := placeKeyed(svc, "form-abc.123_X-y")
	if err != nil {
		t.Fatal(err)
	}
	second, err := placeKeyed(svc, "form-abc.123_X-y")
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if m.payment.charges() != 1 || m.shipping.shipped() != 1 || m.cart.emptyCalls != 1 || m.email.calls != 1 {
		t.Errorf("charges=%d ships=%d empties=%d emails=%d, want 1 each", m.payment.charges(), m.shipping.shipped(), m.cart.emptyCalls, m.email.calls)
	}
	if rs, cm, rel := m.inventory.counts(); rs != 1 || cm != 1 || rel != 0 {
		t.Errorf("reserve/commit/release = %d/%d/%d, want 1/1/0", rs, cm, rel)
	}
	a, b := first.GetOrder(), second.GetOrder()
	if a.GetOrderId() != b.GetOrderId() || b.GetShippingTrackingId() != "TRK-1" {
		t.Errorf("order ids %q / %q, tracking %q", a.GetOrderId(), b.GetOrderId(), b.GetShippingTrackingId())
	}
	if !reflect.DeepEqual(a.GetShippingCost().GetUnits(), b.GetShippingCost().GetUnits()) || b.GetShippingCost().GetCurrencyCode() != "USD" || b.GetShippingCost().GetUnits() != 5 {
		t.Errorf("shipping cost = %v", b.GetShippingCost())
	}
	if b.GetShippingAddress().GetStreetAddress() != "1 Main St" || b.GetShippingAddress().GetZipCode() != 10001 {
		t.Errorf("address = %v", b.GetShippingAddress())
	}
	if len(b.GetItems()) != 1 || b.GetItems()[0].GetItem().GetProductId() != "prod-1" || b.GetItems()[0].GetItem().GetQuantity() != 2 ||
		b.GetItems()[0].GetCost().GetUnits() != 10 {
		t.Errorf("items = %v", b.GetItems())
	}
}

func TestIdempotency_ReplayAfterCartEmptied_StillReturnsOrder(t *testing.T) {
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	if _, err := placeKeyed(svc, "k1"); err != nil {
		t.Fatal(err)
	}
	m.cart.cart = &pb.Cart{UserId: "user-1"} // what an emptied cart looks like
	if _, err := placeKeyed(svc, "k1"); err != nil {
		t.Fatalf("replay must not depend on the cart: %v", err)
	}
}

func TestIdempotency_ReplayWhilePending_Aborted(t *testing.T) {
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	id := idempotentOrderID(&pb.PlaceOrderRequest{UserId: "user-1", IdempotencyKey: "k1"})
	m.orders.stored = map[string]*orderspb.Order{id: {OrderId: id, SessionId: "user-1", Status: orderspb.OrderStatus_ORDER_STATUS_PENDING}}

	_, err := placeKeyed(svc, "k1")

	assertStatus(t, err, codes.Aborted, "this order is already being processed")
	assertEvents(t, m, "GetOrder")
	if m.payment.charges() != 0 {
		t.Error("charged a PENDING order again")
	}
}

func TestIdempotency_ReplayOfFailedOrder_FailedPrecondition_NewKeyIsNewAttempt(t *testing.T) {
	m := sagaMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: nope")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	_, err := placeKeyed(svc, "k1")
	assertStatus(t, err, codes.FailedPrecondition, "card declined")

	_, err = placeKeyed(svc, "k1")
	assertStatus(t, err, codes.FailedPrecondition, "this attempt failed, start the checkout again")
	if m.payment.charges() != 1 {
		t.Errorf("charges = %d, the replay must not charge", m.payment.charges())
	}

	m.payment.err = nil
	resp, err := placeKeyed(svc, "k2")
	if err != nil {
		t.Fatalf("a different key is a new attempt: %v", err)
	}
	if resp.GetOrder().GetOrderId() == idempotentOrderID(&pb.PlaceOrderRequest{UserId: "user-1", IdempotencyKey: "k1"}) {
		t.Error("new attempt reused the failed order id")
	}
}

func TestIdempotency_ConcurrentDoubleSubmit_ExactlyOneCharge(t *testing.T) {
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	m.payment.onCharge = func() {
		once.Do(func() { close(entered) })
		<-release
	}

	type result struct {
		resp *pb.PlaceOrderResponse
		err  error
	}
	first := make(chan result, 1)
	go func() {
		r, err := placeKeyed(svc, "dbl")
		first <- result{r, err}
	}()
	<-entered // the first call is inside Charge

	const others = 8
	var wg sync.WaitGroup
	errs := make([]error, others)
	for i := 0; i < others; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = placeKeyed(svc, "dbl")
		}(i)
	}
	wg.Wait()
	close(release)
	r := <-first

	if r.err != nil {
		t.Fatalf("first call: %v", r.err)
	}
	for i, err := range errs {
		if status.Code(err) != codes.Aborted {
			t.Errorf("twin %d: %v, want Aborted", i, err)
		}
	}
	if m.payment.charges() != 1 || m.shipping.shipped() != 1 {
		t.Errorf("charges=%d ships=%d, want exactly 1", m.payment.charges(), m.shipping.shipped())
	}
	if rs, _, _ := m.inventory.counts(); rs != 1 {
		t.Errorf("reserves = %d, want 1", rs)
	}
	// After the first call finished, the same key replays the stored order.
	again, err := placeKeyed(svc, "dbl")
	if err != nil || again.GetOrder().GetOrderId() != r.resp.GetOrder().GetOrderId() {
		t.Errorf("replay after the race: %v, %v", again, err)
	}
	if m.payment.charges() != 1 {
		t.Error("replay charged")
	}
}

func TestIdempotency_OrderIDDerivation(t *testing.T) {
	id := func(user, account, key string) string {
		return idempotentOrderID(&pb.PlaceOrderRequest{UserId: user, AccountId: account, IdempotencyKey: key})
	}
	if id("u1", "", "k") != id("u1", "", "k") {
		t.Error("not deterministic")
	}
	if id("u1", "", "k") == id("u2", "", "k") {
		t.Error("two visitors with the same key collide")
	}
	if id("u1", "", "k") == id("u1", "", "k2") {
		t.Error("different keys collide")
	}
	// An account's key is shared across its devices (different cart keys).
	if id("u1", "acc", "k") != id("u2", "acc", "k") || id("u1", "acc", "k") == id("u1", "", "k") {
		t.Error("account owner not used")
	}
	// The NUL separator: ("a", "bk") and ("ab", "k") are different.
	if id("a", "", "bk") == id("ab", "", "k") {
		t.Error("owner/key boundary is ambiguous")
	}
	if len(id("u", "", "k")) != 36 || id("u", "", "k")[14] != '5' {
		t.Errorf("not a UUIDv5: %s", id("u", "", "k"))
	}
}

func TestIdempotency_TwoVisitorsSameKeyBothOrder(t *testing.T) {
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	r1, err := placeKeyed(svc, "same")
	if err != nil {
		t.Fatal(err)
	}
	m.cart.cart = &pb.Cart{UserId: "user-2", Items: []*pb.CartItem{{ProductId: "prod-1", Quantity: 1}}}
	r2, err := svc.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId: "user-2", UserCurrency: "USD", Address: defaultAddress(), Email: "b@example.com", CreditCard: defaultCard(), IdempotencyKey: "same",
	})
	if err != nil {
		t.Fatal(err)
	}
	if r1.GetOrder().GetOrderId() == r2.GetOrder().GetOrderId() || m.payment.charges() != 2 {
		t.Errorf("ids %q / %q, charges %d", r1.GetOrder().GetOrderId(), r2.GetOrder().GetOrderId(), m.payment.charges())
	}
}

func TestIdempotency_KeyValidation(t *testing.T) {
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	for _, key := range []string{strings.Repeat("a", 65), "has space", "slash/x", "ünï", "semi;colon"} {
		_, err := placeKeyed(svc, key)
		assertStatus(t, err, codes.InvalidArgument, "idempotency_key")
	}
	if got := m.orders.rec.list(); len(got) != 0 {
		t.Errorf("downstream calls with an invalid key: %v", got)
	}
	for _, key := range []string{"", "a", strings.Repeat("a", 64), "A-b_c.9"} {
		if !validIdempotencyKey(key) {
			t.Errorf("key %q rejected", key)
		}
	}
}

func TestIdempotency_GetOrderUnavailable_NothingCharged(t *testing.T) {
	m := sagaMocks()
	m.orders.getErr = status.Error(codes.Unavailable, "down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeKeyed(svc, "k")

	assertStatus(t, err, codes.Unavailable)
	if m.payment.charges() != 0 {
		t.Error("charged although the order lookup failed")
	}
}

// Without order storage the key is validated and otherwise ignored.
func TestIdempotency_WithoutOrderStorage_KeyIgnored(t *testing.T) {
	m := defaultMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	r1, err := placeKeyed(svc, "k")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := placeKeyed(svc, "k")
	if err != nil {
		t.Fatal(err)
	}
	if r1.GetOrder().GetOrderId() == r2.GetOrder().GetOrderId() || m.payment.charges() != 2 {
		t.Error("expected the old behaviour: two orders, two charges")
	}
}

func TestOutOfStockMessage_CapsTheList(t *testing.T) {
	var prep orderPrep
	var sh []*inventorypb.Shortage
	for i := 0; i < 13; i++ {
		id := string(rune('a' + i))
		prep.orderItems = append(prep.orderItems, &pb.OrderItem{Item: &pb.CartItem{ProductId: id, Quantity: 1}})
		prep.products = append(prep.products, &pb.Product{Id: id, Name: "P" + id})
		sh = append(sh, &inventorypb.Shortage{ProductId: id, Requested: 1})
	}
	msg := outOfStockMessage(sh, prep)
	if !strings.HasSuffix(msg, `"Pj" (sold out), and 3 more`) || strings.Contains(msg, "Pk") {
		t.Errorf("message = %q", msg)
	}
}
