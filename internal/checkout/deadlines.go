package checkout

import (
	"context"
	"time"
)

// Deadlines of the outgoing calls that run on the caller's context (CODE-05). The effective
// deadline is the EARLIER of this bound and the caller's own (the frontend gives PlaceOrder 15 s),
// so a bound never extends a caller's deadline; it only stops a hung backend from holding a
// request (and a goroutine) until the caller gives up, or forever when the caller set none.
// Calls that run on a detached context have their own bounds (orders.go, inventory.go).
//
// Variables, not constants, so that tests can shrink them; nothing else writes them.
var (
	// readTimeout: cart GetCart/EmptyCart, catalog GetProduct, currency Convert, shipping GetQuote.
	// Same budget as the frontend gives these backends (3 s).
	readTimeout = 3 * time.Second
	// chargeTimeout: payment Charge. Longer than a read because the card network is behind it,
	// but below the 15 s the frontend waits for the whole PlaceOrder. A charge that times out
	// is an ambiguous outcome (the money may have been taken): see docs/limitations.md.
	chargeTimeout = 10 * time.Second
	// shipTimeout: shipping ShipOrder (a write; its failure triggers the refund).
	shipTimeout = 5 * time.Second
	// emailTimeout: email-service SendOrderConfirmation (only when Kafka is off); best effort.
	emailTimeout = 5 * time.Second
)

// withTimeout derives the context of one outgoing call.
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
