package checkout

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// callError tags an error returned by a downstream gRPC call with the name of
// the service it came from. toStatus uses it to map the downstream status code
// to the code PlaceOrder returns; the full error text only goes to the log.
type callError struct {
	service string // "cart", "product", "currency", "shipping", "payment"
	err     error
}

func (e *callError) Error() string { return e.service + ": " + e.err.Error() }
func (e *callError) Unwrap() error { return e.err }

func newCallError(service string, err error) error {
	if err == nil {
		return nil
	}
	return &callError{service: service, err: err}
}

// maxDetail caps how much of a downstream message is shown to the caller.
const maxDetail = 300

// reasonPrefix matches the machine-readable "<reason>: " prefix payment-service
// puts in front of its messages (card_declined, expired_card, ...).
var reasonPrefix = regexp.MustCompile(`^[a-z]+(_[a-z]+)*: `)

func cleanDetail(msg string) string {
	msg = strings.Join(strings.Fields(msg), " ")
	msg = reasonPrefix.ReplaceAllString(msg, "")
	if r := []rune(msg); len(r) > maxDetail {
		msg = string(r[:maxDetail]) + "..."
	}
	return msg
}

// toStatus turns an error from the order flow into the gRPC status PlaceOrder
// returns. Contract (the frontend maps these codes to 422 / 404 / 503 / 500):
//
//	downstream InvalidArgument, FailedPrecondition -> same code, readable message
//	downstream NotFound                            -> NotFound, readable message
//	downstream Unavailable, DeadlineExceeded,
//	  ResourceExhausted                            -> Unavailable, generic "<service> is temporarily unavailable"
//	caller cancelled                               -> Canceled
//	anything else                                  -> Internal, generic message (details only in the log)
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var ce *callError
	service := ""
	inner := err
	if errors.As(err, &ce) {
		service = ce.service
		inner = ce.err
	}

	var gs interface{ GRPCStatus() *status.Status }
	if !errors.As(inner, &gs) {
		switch {
		case errors.Is(inner, context.DeadlineExceeded):
			return status.Errorf(codes.Unavailable, "%s is temporarily unavailable, please retry", nameOrService(service))
		case errors.Is(inner, context.Canceled):
			return status.Error(codes.Canceled, "request canceled")
		}
		return internalErr()
	}
	st := gs.GRPCStatus()
	detail := cleanDetail(st.Message())

	switch st.Code() {
	case codes.InvalidArgument, codes.FailedPrecondition:
		return status.Errorf(st.Code(), "%s: %s", label(service, st.Code()), detailOrDefault(detail, st.Code()))
	case codes.NotFound:
		return status.Errorf(codes.NotFound, "%s: %s", label(service, st.Code()), detailOrDefault(detail, st.Code()))
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return status.Errorf(codes.Unavailable, "%s is temporarily unavailable, please retry", nameOrService(service))
	case codes.Canceled:
		return status.Error(codes.Canceled, "request canceled")
	default:
		return internalErr()
	}
}

func internalErr() error {
	return status.Error(codes.Internal, "internal error while placing the order")
}

func nameOrService(service string) string {
	if service == "" {
		return "a backend service"
	}
	return service + " service"
}

// label is the prefix of the user-facing message.
func label(service string, c codes.Code) string {
	if service == "payment" {
		if c == codes.FailedPrecondition {
			return "card declined"
		}
		if c == codes.InvalidArgument {
			return "card rejected"
		}
	}
	if service == "" {
		return "order"
	}
	return service
}

func detailOrDefault(detail string, c codes.Code) string {
	if detail != "" {
		return detail
	}
	return strings.ToLower(c.String())
}
