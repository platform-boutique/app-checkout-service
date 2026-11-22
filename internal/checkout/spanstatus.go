package checkout

import (
	"context"

	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// The otelgrpc client handler marks the span of every call that ends with a non-OK gRPC
// status as an ERROR, with no hook to say "this status is the expected answer". The
// idempotency lookup (order-service GetOrder) expects NotFound for a new order, so every
// successful checkout showed a red client span. ExpectedStatusHandler wraps the handler:
// when a call was started with a context from expectCodes and ends with one of those
// codes, it sets the span status to Ok BEFORE otelgrpc ends the span (an Ok status is
// final in the SDK, otelgrpc's later Error is ignored). The span keeps its
// rpc.response.status_code attribute (NOT_FOUND), only the status flag changes. Any
// other code on that call, and every call without the marker, stays an error span.

type expectedCodesKey struct{}

// expectCodes marks the outgoing calls made with the returned context: a reply with one
// of codes is an expected answer, not an error span.
func expectCodes(ctx context.Context, cs ...codes.Code) context.Context {
	return context.WithValue(ctx, expectedCodesKey{}, cs)
}

// ExpectedStatusHandler wraps the client stats handler (otelgrpc.NewClientHandler()).
func ExpectedStatusHandler(inner stats.Handler) stats.Handler {
	return expectedStatusHandler{Handler: inner}
}

type expectedStatusHandler struct{ stats.Handler }

func (h expectedStatusHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if end, ok := s.(*stats.End); ok && end.Error != nil {
		if expected, _ := ctx.Value(expectedCodesKey{}).([]codes.Code); len(expected) > 0 {
			got := status.Code(end.Error)
			for _, c := range expected {
				if c == got {
					trace.SpanFromContext(ctx).SetStatus(otelcodes.Ok, "")
					break
				}
			}
		}
	}
	h.Handler.HandleRPC(ctx, s)
}
