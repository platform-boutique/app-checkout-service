package checkout

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// counterValue reads one counter; the counters are process-wide, so tests compare before and after.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func TestChargeOutcome(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{nil, "success"},
		{status.Error(codes.FailedPrecondition, "card_declined: no"), "declined"},
		{status.Error(codes.InvalidArgument, "invalid_card: no"), "rejected"},
		{status.Error(codes.Unavailable, "down"), "unavailable"},
		{status.Error(codes.DeadlineExceeded, "slow"), "unavailable"},
		{status.Error(codes.ResourceExhausted, "busy"), "unavailable"},
		{status.Error(codes.Canceled, "gone"), "canceled"},
		{status.Error(codes.Internal, "boom"), "error"},
		{fmt.Errorf("wrapped: %w", status.Error(codes.FailedPrecondition, "x")), "declined"},
		{context.DeadlineExceeded, "unavailable"},
		{context.Canceled, "canceled"},
		{errors.New("plain"), "error"},
	}
	for _, c := range cases {
		if got := chargeOutcome(c.err); got != c.want {
			t.Errorf("chargeOutcome(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

// OBS-20: orders placed and payments by outcome are counted by the real saga (mock servers, as in saga_test.go).
func TestBusinessCounters_HappyPath(t *testing.T) {
	placed, paid := counterValue(t, ordersPlacedTotal), counterValue(t, paymentsTotal.WithLabelValues("success"))
	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	if d := counterValue(t, ordersPlacedTotal) - placed; d != 1 {
		t.Errorf("orders placed grew by %v, want 1", d)
	}
	if d := counterValue(t, paymentsTotal.WithLabelValues("success")) - paid; d != 1 {
		t.Errorf("successful payments grew by %v, want 1", d)
	}
}

func TestBusinessCounters_DeclinedIsNotPlaced(t *testing.T) {
	placed, declined := counterValue(t, ordersPlacedTotal), counterValue(t, paymentsTotal.WithLabelValues("declined"))
	m := sagaMocks()
	m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: Your card was declined")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	if _, err := placeOrder(t, svc); err == nil {
		t.Fatal("want an error")
	}
	if d := counterValue(t, ordersPlacedTotal) - placed; d != 0 {
		t.Errorf("orders placed grew by %v, want 0", d)
	}
	if d := counterValue(t, paymentsTotal.WithLabelValues("declined")) - declined; d != 1 {
		t.Errorf("declined payments grew by %v, want 1", d)
	}
}

func TestBusinessCounters_ShipFailureRefunds(t *testing.T) {
	placed, refunded := counterValue(t, ordersPlacedTotal), counterValue(t, refundsTotal.WithLabelValues("success"))
	m := sagaMocks()
	m.shipping.shipErr = status.Error(codes.Unavailable, "carrier down")
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	if _, err := placeOrder(t, svc); err == nil {
		t.Fatal("want an error")
	}
	if d := counterValue(t, ordersPlacedTotal) - placed; d != 0 {
		t.Errorf("orders placed grew by %v, want 0", d)
	}
	if d := counterValue(t, refundsTotal.WithLabelValues("success")) - refunded; d != 1 {
		t.Errorf("refunds grew by %v, want 1", d)
	}
}
