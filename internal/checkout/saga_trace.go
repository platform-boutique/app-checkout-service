package checkout

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Manual spans for the steps of the checkout saga (OBS-10). Every step of PlaceOrder
// that has a side effect or a downstream call gets ONE span, children of the PlaceOrder
// server span, named `saga <step>` (low cardinality, no ids in the name):
//
//	forward steps:  prepare, create_order, reserve_stock, charge, ship, commit_stock, confirm_order, empty_cart, send_confirmation
//	compensations:  refund, release_stock, fail_order   (attribute checkout.compensation=true)
//
// The otelgrpc client spans of the calls inside a step become children of the step span.
// Attributes carry ids and counts only: never an email, address, card data, name or token.
// The span status is Error with the gRPC status CODE as description; the exception event
// holds the code too, never the downstream message (those texts go to the log).
const sagaTracer = "everly/checkout/saga"

// Attribute keys (all low-cardinality except checkout.order_id, which is an opaque UUID).
const (
	attrOrderID      = attribute.Key("checkout.order_id")
	attrStep         = attribute.Key("checkout.step")
	attrCompensation = attribute.Key("checkout.compensation")
	attrItems        = attribute.Key("checkout.items")
	attrAttempts     = attribute.Key("checkout.attempts")
	attrCurrency     = attribute.Key("checkout.currency")
	attrOutcome      = attribute.Key("checkout.outcome")
)

// startStep starts the span of one saga step. The returned func ends it; pass the step's
// error (nil on success). More attributes may be added through the returned span.
func startStep(ctx context.Context, step, orderID string, compensation bool, attrs ...attribute.KeyValue) (context.Context, trace.Span, func(error)) {
	all := append([]attribute.KeyValue{attrStep.String(step), attrCompensation.Bool(compensation)}, attrs...)
	if orderID != "" {
		all = append(all, attrOrderID.String(orderID))
	}
	ctx, span := otel.Tracer(sagaTracer).Start(ctx, "saga "+step, trace.WithAttributes(all...))
	return ctx, span, func(err error) {
		if err != nil {
			code := errCode(err)
			span.RecordError(fmt.Errorf("%s", code))
			span.SetStatus(otelcodes.Error, code)
		}
		span.End()
	}
}

// errCode is the gRPC status code name of err; context errors that did not come from a
// peer (the call's own deadline, a cancelled caller) are mapped like gRPC maps them.
func errCode(err error) string {
	if _, ok := status.FromError(err); !ok || status.Code(err) == codes.Unknown {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return codes.DeadlineExceeded.String()
		case errors.Is(err, context.Canceled):
			return codes.Canceled.String()
		}
	}
	return status.Code(err).String()
}
