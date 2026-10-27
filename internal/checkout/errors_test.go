package checkout

import (
	"bytes"
	"context"
	"strings"
	"testing"

	pb "checkout-service/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func placeOrder(t *testing.T, svc *Service) (*pb.PlaceOrderResponse, error) {
	t.Helper()
	return svc.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId:       "user-1",
		UserCurrency: "USD",
		Address:      defaultAddress(),
		Email:        "user@example.com",
		CreditCard:   defaultCard(),
	})
}

func assertStatus(t *testing.T, err error, code codes.Code, msgContains ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %v, got nil error", code)
	}
	if got := status.Code(err); got != code {
		t.Fatalf("expected code %v, got %v (%v)", code, got, err)
	}
	for _, sub := range msgContains {
		if !strings.Contains(status.Convert(err).Message(), sub) {
			t.Errorf("message %q does not contain %q", status.Convert(err).Message(), sub)
		}
	}
}

// ─── Charge failures: code mapping, nothing after the charge happens ─────────

func TestPlaceOrder_CardDeclined_FailedPrecondition_NothingElseCalled(t *testing.T) {
	m := defaultMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: Your card (ending 0002) was declined by the issuer")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.FailedPrecondition, "card declined: Your card (ending 0002) was declined by the issuer")
	if strings.Contains(err.Error(), "card_declined") {
		t.Errorf("reason prefix should be stripped: %v", err)
	}
	if n := m.shipping.shipped(); n != 0 {
		t.Errorf("ShipOrder called %d times after a decline", n)
	}
	if m.cart.emptyCalls != 0 {
		t.Errorf("EmptyCart called after a decline")
	}
	if m.email.calls != 0 {
		t.Errorf("email sent after a decline")
	}
	if n := len(m.payment.refundRequests()); n != 0 {
		t.Errorf("refund requested although the charge failed: %d", n)
	}
}

func TestPlaceOrder_PaymentInvalidArgument_StaysInvalidArgument(t *testing.T) {
	m := defaultMocks()
	m.payment.err = status.Error(codes.InvalidArgument, "expired_card: Your credit card (ending 1111) expired on 1/2020")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.InvalidArgument, "card rejected: Your credit card (ending 1111) expired on 1/2020")
	if m.shipping.shipped() != 0 {
		t.Error("ShipOrder called after a rejected card")
	}
}

func TestPlaceOrder_PaymentUnavailable_MapsToUnavailable(t *testing.T) {
	m := defaultMocks()
	m.payment.err = status.Error(codes.Unavailable, "processing_error: The payment processor could not complete the charge; please retry")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.Unavailable, "payment service is temporarily unavailable")
	if m.shipping.shipped() != 0 {
		t.Error("ShipOrder called after a failed charge")
	}
}

// ─── Mapping table, all downstream stages before the charge ──────────────────

func TestPlaceOrder_CodeMapping(t *testing.T) {
	type tc struct {
		name     string
		set      func(*mocks, error)
		in       error
		wantCode codes.Code
		wantMsg  string // substring
		notMsg   string // must not appear (internal detail)
	}
	setCart := func(m *mocks, e error) { m.cart.getErr = e }
	setCatalog := func(m *mocks, e error) { m.productCatalog.err = e }
	setCurrency := func(m *mocks, e error) { m.currency.err = e }
	setQuote := func(m *mocks, e error) { m.shipping.quoteErr = e }
	setPay := func(m *mocks, e error) { m.payment.err = e }

	tests := []tc{
		{"cart invalid argument", setCart, status.Error(codes.InvalidArgument, "bad user id"), codes.InvalidArgument, "cart: bad user id", ""},
		{"cart not found", setCart, status.Error(codes.NotFound, "no such cart"), codes.NotFound, "cart: no such cart", ""},
		{"cart unavailable", setCart, status.Error(codes.Unavailable, "redis at 10.0.0.5:6379 refused"), codes.Unavailable, "cart service is temporarily unavailable", "10.0.0.5"},
		{"cart deadline", setCart, status.Error(codes.DeadlineExceeded, "slow"), codes.Unavailable, "cart service is temporarily unavailable", ""},
		{"cart unknown", setCart, status.Error(codes.Unknown, "pq: password authentication failed for user shop"), codes.Internal, "internal error", "password"},
		{"catalog not found", setCatalog, status.Error(codes.NotFound, "no product with ID prod-1"), codes.NotFound, "product: no product with ID prod-1", ""},
		{"catalog failed precondition", setCatalog, status.Error(codes.FailedPrecondition, "product is discontinued"), codes.FailedPrecondition, "product: product is discontinued", ""},
		{"catalog internal", setCatalog, status.Error(codes.Internal, "db at 10.0.3.7:5432 refused"), codes.Internal, "internal error", "10.0.3.7"},
		{"currency invalid argument", setCurrency, status.Error(codes.InvalidArgument, "unsupported currency XXX"), codes.InvalidArgument, "currency: unsupported currency XXX", ""},
		{"currency unavailable", setCurrency, status.Error(codes.Unavailable, "down"), codes.Unavailable, "currency service is temporarily unavailable", ""},
		{"quote invalid argument", setQuote, status.Error(codes.InvalidArgument, `invalid address: country "Kosovo" is not supported`), codes.InvalidArgument, `shipping: invalid address: country "Kosovo" is not supported`, ""},
		{"quote failed precondition", setQuote, status.Error(codes.FailedPrecondition, "no shipping to this zone"), codes.FailedPrecondition, "shipping: no shipping to this zone", ""},
		{"quote unavailable", setQuote, status.Error(codes.Unavailable, "down"), codes.Unavailable, "shipping service is temporarily unavailable", ""},
		{"quote unimplemented", setQuote, status.Error(codes.Unimplemented, "method GetQuote not implemented"), codes.Internal, "internal error", "GetQuote"},
		{"payment internal", setPay, status.Error(codes.Internal, "internal_error: unexpected payment failure"), codes.Internal, "internal error", "unexpected payment"},
		{"payment resource exhausted", setPay, status.Error(codes.ResourceExhausted, "slow down"), codes.Unavailable, "payment service is temporarily unavailable", ""},
		{"payment not found", setPay, status.Error(codes.NotFound, "no such merchant"), codes.NotFound, "payment: no such merchant", ""},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			m := defaultMocks()
			c.set(m, c.in)
			svc, cleanup := startMockServers(t, m)
			defer cleanup()

			_, err := placeOrder(t, svc)

			assertStatus(t, err, c.wantCode, c.wantMsg)
			if c.notMsg != "" && strings.Contains(err.Error(), c.notMsg) {
				t.Errorf("internal detail %q leaked into %q", c.notMsg, err.Error())
			}
			if m.shipping.shipped() != 0 {
				t.Error("ShipOrder must not be called when an earlier step failed")
			}
		})
	}
}

// A failure before the charge never reaches payment.
func TestPlaceOrder_QuoteFailure_NoCharge(t *testing.T) {
	m := defaultMocks()
	m.shipping.quoteErr = status.Error(codes.InvalidArgument, `invalid address: country "Kosovo" is not supported`)
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.InvalidArgument)
	if n := m.payment.charges(); n != 0 {
		t.Errorf("card charged %d times although the quote failed", n)
	}
}

func TestPlaceOrder_EmptyCart_FailedPrecondition_NoCharge(t *testing.T) {
	m := defaultMocks()
	m.cart.cart = &pb.Cart{UserId: "user-1"}
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.FailedPrecondition, "cart is empty")
	if m.payment.charges() != 0 {
		t.Error("card charged for an empty cart")
	}
}

func TestPlaceOrder_InvalidRequest_NoDownstreamCalls(t *testing.T) {
	good := func() *pb.PlaceOrderRequest {
		return &pb.PlaceOrderRequest{UserId: "u", UserCurrency: "USD", Address: defaultAddress(), CreditCard: defaultCard()}
	}
	cases := map[string]func(*pb.PlaceOrderRequest){
		"no user":     func(r *pb.PlaceOrderRequest) { r.UserId = " " },
		"no currency": func(r *pb.PlaceOrderRequest) { r.UserCurrency = "" },
		"no address":  func(r *pb.PlaceOrderRequest) { r.Address = nil },
		"no card":     func(r *pb.PlaceOrderRequest) { r.CreditCard = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := defaultMocks()
			svc, cleanup := startMockServers(t, m)
			defer cleanup()
			req := good()
			mutate(req)

			_, err := svc.PlaceOrder(context.Background(), req)

			assertStatus(t, err, codes.InvalidArgument)
			if m.payment.charges() != 0 {
				t.Error("charged on an invalid request")
			}
		})
	}
}

// ─── Compensation: shipping fails after a successful charge ──────────────────

func TestPlaceOrder_ShipFailure_RefundsExactlyOnce(t *testing.T) {
	m := defaultMocks()
	m.payment.transactionID = "TX-42"
	m.shipping.shipErr = status.Error(codes.InvalidArgument, `invalid address: country "Kosovo" is not an ISO 3166-1 alpha-2 code or a known country name`)
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.InvalidArgument, `shipping: invalid address: country "Kosovo"`, "refunded")
	if strings.Contains(err.Error(), "pending") {
		t.Errorf("refund succeeded, message must not say pending: %v", err)
	}
	refunds := m.payment.refundRequests()
	if len(refunds) != 1 {
		t.Fatalf("expected exactly 1 Refund, got %d", len(refunds))
	}
	if refunds[0].GetTransactionId() != "TX-42" {
		t.Errorf("refund for transaction %q, want TX-42", refunds[0].GetTransactionId())
	}
	// 2 x 10.00 items + 5.00 shipping
	if a := refunds[0].GetAmount(); a.GetUnits() != 25 || a.GetCurrencyCode() != "USD" {
		t.Errorf("refund amount = %v, want 25 USD", a)
	}
	if m.payment.charges() != 1 {
		t.Errorf("expected 1 charge, got %d", m.payment.charges())
	}
	if m.cart.emptyCalls != 0 {
		t.Error("cart emptied although the order failed")
	}
	if m.email.calls != 0 {
		t.Error("confirmation email sent although the order failed")
	}
}

func TestPlaceOrder_ShipUnavailable_RefundsAndReturnsUnavailable(t *testing.T) {
	m := defaultMocks()
	m.shipping.shipErr = status.Error(codes.Unavailable, "carrier api down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	assertStatus(t, err, codes.Unavailable, "shipping service is temporarily unavailable", "refunded")
	if n := len(m.payment.refundRequests()); n != 1 {
		t.Errorf("expected 1 Refund, got %d", n)
	}
}

func TestPlaceOrder_ShipFailure_RefundAlsoFails_KeepsOriginalErrorAndLogsTransactionID(t *testing.T) {
	var buf bytes.Buffer
	old := log.Out
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	m := defaultMocks()
	m.payment.transactionID = "TX-77"
	m.shipping.shipErr = status.Error(codes.InvalidArgument, "invalid address: city is required")
	m.payment.refundErr = status.Error(codes.Unavailable, "payment down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	_, err := placeOrder(t, svc)

	// original (shipping) error is kept, with a note that the refund is pending
	assertStatus(t, err, codes.InvalidArgument, "shipping: invalid address: city is required", "refund is pending", "TX-77")
	if n := len(m.payment.refundRequests()); n != 1 {
		t.Errorf("expected 1 Refund attempt, got %d", n)
	}
	logs := buf.String()
	if !strings.Contains(logs, "REFUND FAILED") || !strings.Contains(logs, "level=error") || !strings.Contains(logs, "TX-77") {
		t.Errorf("refund failure not logged at error level with the transaction id:\n%s", logs)
	}
}

func TestPlaceOrder_NoCardDataInLogs(t *testing.T) {
	var buf bytes.Buffer
	old := log.Out
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	m := defaultMocks()
	m.shipping.shipErr = status.Error(codes.Internal, "boom")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	_, _ = placeOrder(t, svc)

	if strings.Contains(buf.String(), defaultCard().CreditCardNumber) {
		t.Errorf("card number found in the logs:\n%s", buf.String())
	}
}

// Happy path keeps working and never refunds.
func TestPlaceOrder_Success_NoRefund(t *testing.T) {
	m := defaultMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()

	resp, err := placeOrder(t, svc)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetOrder().GetShippingTrackingId() != "TRACK-123" {
		t.Errorf("tracking id = %q", resp.GetOrder().GetShippingTrackingId())
	}
	if n := len(m.payment.refundRequests()); n != 0 {
		t.Errorf("refund requested on the happy path: %d", n)
	}
}
