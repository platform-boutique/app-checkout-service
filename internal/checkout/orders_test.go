package checkout

import (
	"bytes"
	"context"
	"math/big"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"

	pb "checkout-service/proto"
	orderspb "checkout-service/proto/orders"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// recorder keeps the order in which the downstream calls arrive. The nil recorder
// is valid (the mocks of tests that do not care call add on it).
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(ev string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// mockOrderServer is a fake order-service. CreateOrder repeats the contract's
// money rule independently of the checkout code: total == sum(quantity x
// unit_price) + shipping_cost, exactly, in one currency (big.Int nanos).
type mockOrderServer struct {
	orderspb.UnimplementedOrderServiceServer
	rec *recorder

	createErr  error
	confirmErr error
	failErr    error

	mu       sync.Mutex
	creates  []*orderspb.CreateOrderRequest
	confirms []*orderspb.ConfirmOrderRequest
	fails    []*orderspb.FailOrderRequest
}

func nanosOf(m *orderspb.Money) *big.Int {
	n := new(big.Int).Mul(big.NewInt(m.GetUnits()), big.NewInt(1_000_000_000))
	return n.Add(n, big.NewInt(int64(m.GetNanos())))
}

func (m *mockOrderServer) CreateOrder(_ context.Context, req *orderspb.CreateOrderRequest) (*orderspb.Order, error) {
	m.rec.add("CreateOrder")
	m.mu.Lock()
	m.creates = append(m.creates, req)
	m.mu.Unlock()
	if m.createErr != nil {
		return nil, m.createErr
	}
	if req.GetOrderId() == "" || req.GetSessionId() == "" || len(req.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "order_id, session_id and items are required")
	}
	cur := req.GetTotal().GetCurrencyCode()
	sum := new(big.Int)
	for _, it := range req.GetItems() {
		if it.GetUnitPrice().GetCurrencyCode() != cur || it.GetQuantity() < 1 || it.GetName() == "" {
			return nil, status.Error(codes.InvalidArgument, "bad item")
		}
		sum.Add(sum, new(big.Int).Mul(nanosOf(it.GetUnitPrice()), big.NewInt(int64(it.GetQuantity()))))
	}
	if sc := req.GetShippingCost(); sc != nil {
		if sc.GetCurrencyCode() != cur {
			return nil, status.Error(codes.InvalidArgument, "currency mismatch")
		}
		sum.Add(sum, nanosOf(sc))
	}
	if nanosOf(req.GetTotal()).Cmp(sum) != 0 || req.GetTotal().GetNanos() < 0 || req.GetTotal().GetNanos() > 999_999_999 {
		return nil, status.Errorf(codes.InvalidArgument, "total %v does not equal items + shipping (%v nanos)", req.GetTotal(), sum)
	}
	return &orderspb.Order{OrderId: req.GetOrderId()}, nil
}

func (m *mockOrderServer) ConfirmOrder(_ context.Context, req *orderspb.ConfirmOrderRequest) (*orderspb.Order, error) {
	m.rec.add("ConfirmOrder")
	m.mu.Lock()
	m.confirms = append(m.confirms, req)
	m.mu.Unlock()
	if m.confirmErr != nil {
		return nil, m.confirmErr
	}
	return &orderspb.Order{OrderId: req.GetOrderId()}, nil
}

func (m *mockOrderServer) FailOrder(_ context.Context, req *orderspb.FailOrderRequest) (*orderspb.Order, error) {
	m.rec.add("FailOrder")
	m.mu.Lock()
	m.fails = append(m.fails, req)
	m.mu.Unlock()
	if m.failErr != nil {
		return nil, m.failErr
	}
	return &orderspb.Order{OrderId: req.GetOrderId()}, nil
}

func (m *mockOrderServer) counts() (creates, confirms, fails int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.creates), len(m.confirms), len(m.fails)
}

// ordersMocks is defaultMocks with order storage on and one shared recorder.
func ordersMocks() *mocks {
	m := defaultMocks()
	rec := &recorder{}
	m.orders = &mockOrderServer{rec: rec}
	m.payment.rec = rec
	m.shipping.rec = rec
	return m
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := log.Out
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(old) })
	return &buf
}

func assertEvents(t *testing.T, m *mocks, want ...string) {
	t.Helper()
	if got := m.orders.rec.list(); !reflect.DeepEqual(got, want) {
		t.Errorf("call order = %v, want %v", got, want)
	}
}

// ─── Happy path ──────────────────────────────────────────────────────────────

func TestOrders_HappyPath_CreateBeforeCharge_ConfirmAfterShip(t *testing.T) {
	m := ordersMocks()
	m.payment.transactionID = "TX-9"
	m.shipping.trackingID = "TRK-9"
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	resp, err := placeOrder(t, svc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertEvents(t, m, "CreateOrder", "Charge", "ShipOrder", "ConfirmOrder")
	cr, co, fa := m.orders.counts()
	if cr != 1 || co != 1 || fa != 0 {
		t.Fatalf("create/confirm/fail = %d/%d/%d, want 1/1/0", cr, co, fa)
	}

	create := m.orders.creates[0]
	if create.GetOrderId() != resp.GetOrder().GetOrderId() {
		t.Errorf("stored order id %q != OrderResult.OrderId %q", create.GetOrderId(), resp.GetOrder().GetOrderId())
	}
	if len(create.GetOrderId()) != 36 {
		t.Errorf("order id %q is not a UUID", create.GetOrderId())
	}
	if create.GetSessionId() != "user-1" || create.GetEmail() != "user@example.com" || create.GetUserId() != "" {
		t.Errorf("session/email/user = %q/%q/%q", create.GetSessionId(), create.GetEmail(), create.GetUserId())
	}
	wantAddr := &orderspb.Address{StreetAddress: "1 Main St", City: "Testville", State: "NY", Country: "US", ZipCode: 10001}
	if a := create.GetShippingAddress(); a.GetStreetAddress() != wantAddr.StreetAddress || a.GetCity() != wantAddr.City ||
		a.GetState() != wantAddr.State || a.GetCountry() != wantAddr.Country || a.GetZipCode() != wantAddr.ZipCode {
		t.Errorf("address = %v, want %v", a, wantAddr)
	}
	if len(create.GetItems()) != 1 {
		t.Fatalf("items = %v", create.GetItems())
	}
	it := create.GetItems()[0]
	if it.GetProductId() != "prod-1" || it.GetName() != "Test Widget" || it.GetQuantity() != 2 ||
		it.GetUnitPrice().GetCurrencyCode() != "USD" || it.GetUnitPrice().GetUnits() != 10 {
		t.Errorf("item snapshot = %v", it)
	}
	// 2 x 10.00 + 5.00
	if tot := create.GetTotal(); tot.GetUnits() != 25 || tot.GetNanos() != 0 || tot.GetCurrencyCode() != "USD" {
		t.Errorf("total = %v", tot)
	}
	if sc := create.GetShippingCost(); sc.GetUnits() != 5 || sc.GetCurrencyCode() != "USD" {
		t.Errorf("shipping cost = %v", sc)
	}

	conf := m.orders.confirms[0]
	if conf.GetOrderId() != create.GetOrderId() || conf.GetShippingTrackingId() != "TRK-9" || conf.GetPaymentTransactionId() != "TX-9" {
		t.Errorf("confirm = %v", conf)
	}
	if m.cart.emptyCalls != 1 || m.email.calls != 1 {
		t.Errorf("cart emptied %d times, emails %d, want 1/1", m.cart.emptyCalls, m.email.calls)
	}
}

// The snapshot is taken in the USER's currency: the unit price is the converted one.
func TestOrders_SnapshotInUserCurrency(t *testing.T) {
	m := ordersMocks()
	m.productCatalog.product.Picture = "/static/img/widget.jpg"
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := svc.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "EUR", Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := m.orders.creates[0]
	if c.GetItems()[0].GetUnitPrice().GetCurrencyCode() != "EUR" || c.GetTotal().GetCurrencyCode() != "EUR" || c.GetShippingCost().GetCurrencyCode() != "EUR" {
		t.Errorf("currencies = %v / %v / %v, want EUR", c.GetItems()[0].GetUnitPrice(), c.GetShippingCost(), c.GetTotal())
	}
	if c.GetItems()[0].GetPicture() != "/static/img/widget.jpg" {
		t.Errorf("picture = %q", c.GetItems()[0].GetPicture())
	}
}

// ─── Exact money ─────────────────────────────────────────────────────────────

// 0.1 x 3 + 0.2 x 3 + 33.333333333 x 3 + 4.999999999 = 105.899999998 exactly.
// float64 arithmetic cannot represent that; the fake order-service checks the
// rule with big.Int nanos, and the charge must be for the same amount.
func TestOrders_TotalIsExactInNanos(t *testing.T) {
	m := ordersMocks()
	m.productCatalog.byID = map[string]*pb.Product{
		"a": {Id: "a", Name: "Tenth", PriceUsd: &pb.Money{CurrencyCode: "USD", Nanos: 100_000_000}},
		"b": {Id: "b", Name: "Fifth", PriceUsd: &pb.Money{CurrencyCode: "USD", Nanos: 200_000_000}},
		"c": {Id: "c", Name: "Third", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 33, Nanos: 333_333_333}},
	}
	m.cart.cart = &pb.Cart{UserId: "user-1", Items: []*pb.CartItem{
		{ProductId: "a", Quantity: 3}, {ProductId: "b", Quantity: 3}, {ProductId: "c", Quantity: 3},
	}}
	m.shipping.quote = &pb.Money{CurrencyCode: "USD", Units: 4, Nanos: 999_999_999}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatalf("CreateOrder (which checks the total exactly) or the order failed: %v", err)
	}
	total := m.orders.creates[0].GetTotal()
	if total.GetUnits() != 105 || total.GetNanos() != 899_999_998 {
		t.Errorf("total = %d.%09d, want 105.899999998", total.GetUnits(), total.GetNanos())
	}
}

func TestOrders_ChargedAmountEqualsStoredTotal(t *testing.T) {
	m := ordersMocks()
	m.shipping.shipErr = status.Error(codes.Unavailable, "carrier down") // makes checkout send the charged amount in a Refund
	m.productCatalog.product = &pb.Product{Id: "prod-1", Name: "W", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 19, Nanos: 990_000_001}}
	m.shipping.quote = &pb.Money{CurrencyCode: "USD", Units: 4, Nanos: 750_000_000}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, _ = placeOrder(t, svc)

	refunds := m.payment.refundRequests()
	if len(refunds) != 1 || len(m.orders.creates) != 1 {
		t.Fatalf("refunds=%d creates=%d", len(refunds), len(m.orders.creates))
	}
	if got, want := refunds[0].GetAmount(), m.orders.creates[0].GetTotal(); got.GetUnits() != want.GetUnits() || got.GetNanos() != want.GetNanos() {
		t.Errorf("charged %v != stored total %v", got, want)
	}
	// 2 x 19.990000001 + 4.75 = 44.730000002
	if tot := m.orders.creates[0].GetTotal(); tot.GetUnits() != 44 || tot.GetNanos() != 730_000_002 {
		t.Errorf("total = %v", tot)
	}
}

// ─── Failure paths ───────────────────────────────────────────────────────────

func TestOrders_CardDeclined_FailOrderWithReason_NoShip(t *testing.T) {
	m := ordersMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: Your card (ending 0002) was declined by the issuer")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.FailedPrecondition, "card declined")
	assertEvents(t, m, "CreateOrder", "Charge", "FailOrder")
	if m.shipping.shipped() != 0 {
		t.Error("shipped after a decline")
	}
	f := m.orders.fails[0]
	if f.GetOrderId() != m.orders.creates[0].GetOrderId() {
		t.Errorf("FailOrder for %q, created %q", f.GetOrderId(), m.orders.creates[0].GetOrderId())
	}
	if f.GetReason() != status.Convert(err).Message() || !strings.Contains(f.GetReason(), "declined by the issuer") {
		t.Errorf("reason = %q, error message = %q", f.GetReason(), status.Convert(err).Message())
	}
	if strings.Contains(f.GetReason(), defaultCard().CreditCardNumber) {
		t.Errorf("card number in the reason: %q", f.GetReason())
	}
	if _, co, _ := m.orders.counts(); co != 0 {
		t.Error("ConfirmOrder called after a decline")
	}
	if m.cart.emptyCalls != 0 {
		t.Error("cart emptied after a decline")
	}
}

func TestOrders_ShipFailure_RefundThenFailOrder(t *testing.T) {
	m := ordersMocks()
	m.payment.transactionID = "TX-42"
	m.shipping.shipErr = status.Error(codes.InvalidArgument, "invalid address: city is required")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.InvalidArgument, "refunded")
	assertEvents(t, m, "CreateOrder", "Charge", "ShipOrder", "Refund", "FailOrder")
	if n := len(m.payment.refundRequests()); n != 1 {
		t.Errorf("refunds = %d, want 1", n)
	}
	f := m.orders.fails[0]
	if !strings.Contains(f.GetReason(), "invalid address: city is required") || !strings.Contains(f.GetReason(), "refunded") {
		t.Errorf("reason = %q", f.GetReason())
	}
	if _, co, _ := m.orders.counts(); co != 0 {
		t.Error("ConfirmOrder called after a ship failure")
	}
}

func TestOrders_CreateUnavailable_NoCharge(t *testing.T) {
	logs := captureLogs(t)
	m := ordersMocks()
	m.orders.createErr = status.Error(codes.Unavailable, "database not ready")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.Unavailable, "order service is temporarily unavailable")
	assertEvents(t, m, "CreateOrder")
	if m.payment.charges() != 0 || m.shipping.shipped() != 0 || m.cart.emptyCalls != 0 {
		t.Error("charge/ship/empty-cart happened although the order could not be stored")
	}
	if strings.Contains(err.Error(), "database") {
		t.Errorf("downstream detail leaked to the caller: %v", err)
	}
	if !strings.Contains(logs.String(), "nothing was charged") {
		t.Errorf("not logged:\n%s", logs.String())
	}
}

// A real connection error (nothing listens on the address) must also stop before the charge.
func TestOrders_CreateConnectionRefused_Unavailable_NoCharge(t *testing.T) {
	m := ordersMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close() // nothing listens here any more
	svc.OrderConn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.OrderConn.Close()

	_, err = placeOrder(t, svc)

	assertStatus(t, err, codes.Unavailable, "order service is temporarily unavailable")
	if m.payment.charges() != 0 || m.shipping.shipped() != 0 {
		t.Error("charged or shipped although order storage was not reachable")
	}
}

func TestOrders_CreateInvalidArgument_IsInternal_LoggedAsOurBug_NoCharge(t *testing.T) {
	logs := captureLogs(t)
	for _, c := range []codes.Code{codes.InvalidArgument, codes.AlreadyExists, codes.Internal, codes.NotFound} {
		t.Run(c.String(), func(t *testing.T) {
			m := ordersMocks()
			m.orders.createErr = status.Error(c, "total 25 does not equal items + shipping")
			svc, cleanup := startMockServers(t, m)
			defer cleanup()

			_, err := placeOrder(t, svc)

			assertStatus(t, err, codes.Internal, "internal error while placing the order")
			if strings.Contains(err.Error(), "total 25") {
				t.Errorf("order-service detail leaked: %v", err)
			}
			if m.payment.charges() != 0 {
				t.Error("charged")
			}
			if _, _, fa := m.orders.counts(); fa != 0 {
				t.Error("FailOrder called for an order that was never stored")
			}
		})
	}
	if !strings.Contains(logs.String(), `level=error`) || !strings.Contains(logs.String(), "total 25 does not equal") {
		t.Errorf("rejection not logged at error level with the detail:\n%s", logs.String())
	}
}

func TestOrders_ConfirmFails_SuccessReturned_ErrorLogged(t *testing.T) {
	logs := captureLogs(t)
	m := ordersMocks()
	m.payment.transactionID = "TX-CONF"
	m.orders.confirmErr = status.Error(codes.Unavailable, "order db down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	resp, err := placeOrder(t, svc)

	if err != nil {
		t.Fatalf("the customer paid and the parcel is ordered, expected success, got %v", err)
	}
	orderID := resp.GetOrder().GetOrderId()
	if orderID == "" || resp.GetOrder().GetShippingTrackingId() != "TRACK-123" {
		t.Errorf("order result = %v", resp.GetOrder())
	}
	assertEvents(t, m, "CreateOrder", "Charge", "ShipOrder", "ConfirmOrder")
	if len(m.payment.refundRequests()) != 0 {
		t.Error("refunded although the order was shipped")
	}
	if _, _, fa := m.orders.counts(); fa != 0 {
		t.Error("FailOrder called for a paid and shipped order")
	}
	if m.cart.emptyCalls != 1 || m.email.calls != 1 {
		t.Errorf("cart emptied %d, emails %d, want 1/1", m.cart.emptyCalls, m.email.calls)
	}
	out := logs.String()
	for _, want := range []string{"level=error", "ORDER NOT CONFIRMED", orderID, "TX-CONF"} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
}

func TestOrders_FailOrderError_DoesNotMaskOriginalError(t *testing.T) {
	logs := captureLogs(t)
	m := ordersMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: nope")
	m.orders.failErr = status.Error(codes.Internal, "boom")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.FailedPrecondition, "card declined: nope")
	if !strings.Contains(logs.String(), "FailOrder failed") {
		t.Errorf("FailOrder error not logged:\n%s", logs.String())
	}
}

// FailOrder runs on a context detached from the caller: a caller that is already
// gone (here: cancelled while the charge is in flight) must not leave the order PENDING.
func TestOrders_FailOrderSurvivesCallerCancellation(t *testing.T) {
	m := ordersMocks()
	ctx, cancel := context.WithCancel(context.Background())
	m.payment.onCharge = cancel
	m.payment.err = status.Error(codes.Internal, "x")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := svc.PlaceOrder(ctx, &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "USD", Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	})

	if err == nil {
		t.Fatal("expected an error")
	}
	if _, _, fa := m.orders.counts(); fa != 1 {
		t.Errorf("FailOrder calls = %d, want 1 (events %v)", fa, m.orders.rec.list())
	}
}

func TestOrders_PrepareFailure_NothingStored(t *testing.T) {
	m := ordersMocks()
	m.shipping.quoteErr = status.Error(codes.InvalidArgument, "invalid address: bad")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.InvalidArgument)
	assertEvents(t, m) // no order-service call at all
}

func TestOrders_Validation_NothingStored(t *testing.T) {
	cases := map[string]func(*pb.PlaceOrderRequest){
		"empty email":   func(r *pb.PlaceOrderRequest) { r.Email = "" },
		"display name":  func(r *pb.PlaceOrderRequest) { r.Email = "Ada <ada@example.com>" },
		"no at":         func(r *pb.PlaceOrderRequest) { r.Email = "ada.example.com" },
		"long user id":  func(r *pb.PlaceOrderRequest) { r.UserId = strings.Repeat("u", 129) },
		"empty street":  func(r *pb.PlaceOrderRequest) { r.Address.StreetAddress = " " },
		"long city":     func(r *pb.PlaceOrderRequest) { r.Address.City = strings.Repeat("c", 101) },
		"negative zip":  func(r *pb.PlaceOrderRequest) { r.Address.ZipCode = -1 },
		"empty country": func(r *pb.PlaceOrderRequest) { r.Address.Country = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := ordersMocks()
			svc, cleanup := startMockServers(t, m)
			defer cleanup()
			req := &pb.PlaceOrderRequest{UserId: "user-1", UserCurrency: "USD", Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard()}
			mutate(req)

			_, err := svc.PlaceOrder(context.Background(), req)

			assertStatus(t, err, codes.InvalidArgument)
			assertEvents(t, m)
			if m.payment.charges() != 0 {
				t.Error("charged")
			}
		})
	}
}

func TestOrders_CartTooLargeForOrderService_NoCharge(t *testing.T) {
	m := ordersMocks()
	m.cart.cart = &pb.Cart{UserId: "user-1", Items: []*pb.CartItem{{ProductId: "prod-1", Quantity: 1001}}}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.InvalidArgument, "quantity must be between 1 and 1000")
	assertEvents(t, m)
}

// ─── ORDER_SERVICE_ADDR unset: the old behaviour ─────────────────────────────

func TestOrders_Disabled_OldBehaviour(t *testing.T) {
	m := defaultMocks() // m.orders == nil -> Service.OrderConn == nil
	m.payment.err = nil
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	if svc.OrderConn != nil {
		t.Fatal("OrderConn must be nil when order storage is not configured")
	}

	// even an email order-service would reject is fine without order storage
	resp, err := svc.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "USD", Address: defaultAddress(), Email: "", CreditCard: defaultCard(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetOrder().GetOrderId() == "" || resp.GetOrder().GetShippingTrackingId() != "TRACK-123" {
		t.Errorf("order = %v", resp.GetOrder())
	}
	if m.payment.charges() != 1 || m.shipping.shipped() != 1 || m.cart.emptyCalls != 1 {
		t.Error("old flow did not run")
	}
}

func TestOrders_Disabled_FailuresStillBehaveAsBefore(t *testing.T) {
	m := defaultMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: nope")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.FailedPrecondition, "card declined: nope")
}

// ─── Logs ────────────────────────────────────────────────────────────────────

func TestOrders_NoCardDataInLogsOrReasons(t *testing.T) {
	logs := captureLogs(t)
	cases := map[string]func(*mocks){
		"decline":      func(m *mocks) { m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: declined") },
		"ship failure": func(m *mocks) { m.shipping.shipErr = status.Error(codes.Internal, "boom") },
		"confirm fail": func(m *mocks) { m.orders.confirmErr = status.Error(codes.Internal, "boom") },
		"create fail":  func(m *mocks) { m.orders.createErr = status.Error(codes.InvalidArgument, "bad") },
		"fail fails": func(m *mocks) {
			m.payment.err = status.Error(codes.Internal, "x")
			m.orders.failErr = status.Error(codes.Internal, "y")
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			m := ordersMocks()
			setup(m)
			svc, cleanup := startMockServers(t, m)
			defer cleanup()
			_, _ = placeOrder(t, svc)

			card := defaultCard()
			m.orders.mu.Lock()
			defer m.orders.mu.Unlock()
			for _, f := range m.orders.fails {
				if strings.Contains(f.GetReason(), card.CreditCardNumber) {
					t.Errorf("card number in reason %q", f.GetReason())
				}
			}
		})
	}
	if strings.Contains(logs.String(), defaultCard().CreditCardNumber) {
		t.Errorf("card number found in the logs:\n%s", logs.String())
	}
}

func TestFailureReason(t *testing.T) {
	if got := failureReason(status.Error(codes.Internal, "")); got != "order failed" {
		t.Errorf("empty message -> %q", got)
	}
	long := status.Error(codes.Internal, strings.Repeat("é", 800))
	if got := failureReason(long); len([]rune(got)) != 500 {
		t.Errorf("reason length = %d runes, want 500", len([]rune(got)))
	}
	if got := failureReason(status.Error(codes.Internal, "a\n  b\tc")); got != "a b c" {
		t.Errorf("whitespace not collapsed: %q", got)
	}
}
