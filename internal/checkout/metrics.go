package checkout

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Business counters (OBS-20), next to the RED series of the gRPC middleware and the event counters in events.go.
// Label values are a fixed small set, never ids, amounts or messages.
var (
	// ordersPlacedTotal counts orders that were charged and shipped (the order is PLACED). A replay of an
	// already placed order (idempotency key) is not counted again.
	ordersPlacedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "checkout_orders_placed_total",
		Help: "Orders placed: charged and shipped.",
	})
	// paymentsTotal counts the card charge of every order that got that far, by outcome (see chargeOutcome).
	paymentsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "checkout_payments_total",
		Help: "Card charges by outcome: success, declined, rejected, unavailable, canceled, error.",
	}, []string{"outcome"})
	// refundsTotal counts the compensation of a charge after a later step failed.
	refundsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "checkout_refunds_total",
		Help: "Refunds after a failed order by outcome: success, error (the customer must be refunded by hand).",
	}, []string{"outcome"})
)

func init() {
	// Series exist (at 0) from the start, so a rate() over them works before the first order.
	for _, o := range []string{"success", "declined", "rejected", "unavailable", "canceled", "error"} {
		paymentsTotal.WithLabelValues(o)
	}
	for _, o := range []string{"success", "error"} {
		refundsTotal.WithLabelValues(o)
	}
}

// chargeOutcome is the label of checkout_payments_total for the result of the Charge call: a decline
// (FailedPrecondition) and a rejected card (InvalidArgument) are the customer's, unavailable (including a
// deadline, where the money may have been taken) is the platform's.
func chargeOutcome(err error) string {
	if err == nil {
		return "success"
	}
	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) {
		switch gs.GRPCStatus().Code() {
		case codes.FailedPrecondition:
			return "declined"
		case codes.InvalidArgument:
			return "rejected"
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			return "unavailable"
		case codes.Canceled:
			return "canceled"
		}
		return "error"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "unavailable"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	return "error"
}

func resultLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}
