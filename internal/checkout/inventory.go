package checkout

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "checkout-service/proto"
	inventorypb "checkout-service/proto/inventory"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Stock reservation (inventory-service) is optional: with Service.InventoryConn == nil
// (INVENTORY_SERVICE_ADDR unset) PlaceOrder neither reserves nor commits stock, as before.
// When it is set it is mandatory (fail closed: never sell without a reservation):
//
//	CreateOrder (PENDING)
//	Reserve         short -> FailOrder, FAILED_PRECONDITION "out of stock: ..."; RPC error -> Release, FailOrder, UNAVAILABLE
//	Charge          failure -> Release, FailOrder
//	ShipOrder       failure -> Refund, Release, FailOrder
//	Commit          3 attempts; still failing -> logged (STOCK NOT COMMITTED), the order goes on
//	ConfirmOrder ...
//
// Compensation order: money first (Refund), then stock (Release), then the order record
// (FailOrder, whose reason carries the refund note). Every compensation step is best
// effort: its failure is logged and never replaces the error PlaceOrder returns.
const (
	reserveTimeout = 5 * time.Second
	// stockTimeout bounds one attempt of Commit / Release. Both run on a context
	// detached from the caller's: a customer who closed the tab must neither leave
	// stock held (Release) nor sold-but-uncommitted (Commit).
	stockTimeout = 5 * time.Second
	// stockAttempts is the number of tries of Commit and Release.
	stockAttempts = 3

	// maxShortagesListed caps the product list of the out-of-stock message.
	maxShortagesListed = 10
)

// stockBackoff is the pause before the 2nd and 3rd attempt of Commit / Release (a variable so tests are fast).
var stockBackoff = []time.Duration{100 * time.Millisecond, 300 * time.Millisecond}

func (s *Service) inventoryEnabled() bool { return s.InventoryConn != nil }

func (s *Service) inventory() inventorypb.InventoryServiceClient {
	return inventorypb.NewInventoryServiceClient(s.InventoryConn)
}

// stockLines turns the cart into reservation lines: one per product, quantities of
// repeated lines added up (inventory-service rejects duplicate product ids).
func stockLines(items []*pb.CartItem) ([]*inventorypb.ReserveItem, error) {
	var lines []*inventorypb.ReserveItem
	idx := map[string]int{}
	for _, it := range items {
		id := it.GetProductId()
		if i, ok := idx[id]; ok {
			lines[i].Quantity += it.GetQuantity()
			continue
		}
		idx[id] = len(lines)
		lines = append(lines, &inventorypb.ReserveItem{ProductId: id, Quantity: it.GetQuantity()})
	}
	if len(lines) > maxOrderItems {
		return nil, status.Errorf(codes.InvalidArgument, "order: at most %d different products per order", maxOrderItems)
	}
	for _, l := range lines {
		if l.Quantity < 1 || l.Quantity > maxItemQuantity {
			return nil, status.Errorf(codes.InvalidArgument, "order: quantity must be between 1 and %d", maxItemQuantity)
		}
		if n := len(l.ProductId); n < 1 || n > maxProductIDLen {
			return nil, status.Errorf(codes.InvalidArgument, "order: product id must be 1-%d characters", maxProductIDLen)
		}
	}
	return lines, nil
}

// reserveStock holds the stock for the order. It returns nil when the stock is
// reserved, otherwise the status PlaceOrder returns (nothing was charged). When the
// reservation could not be confirmed (RPC error) it releases best effort first,
// because the request may have arrived and the answer got lost.
func (s *Service) reserveStock(ctx context.Context, orderID string, lines []*inventorypb.ReserveItem, prep orderPrep) error {
	rctx, cancel := context.WithTimeout(ctx, reserveTimeout)
	defer cancel()
	resp, err := s.inventory().Reserve(rctx, &inventorypb.ReserveRequest{OrderId: orderID, Items: lines})
	fields := log.WithField("order_id", orderID)
	if err != nil {
		if c := status.Code(err); c == codes.Unavailable || c == codes.DeadlineExceeded || c == codes.Canceled || c == codes.ResourceExhausted {
			fields.Warnf("[PlaceOrder] Reserve failed, nothing was charged: %v", err)
		} else {
			fields.Errorf("[PlaceOrder] Reserve was rejected (checkout bug or contract mismatch), nothing was charged: %v", err)
		}
		s.releaseStock(ctx, orderID)
		return status.Error(codes.Unavailable, "stock check unavailable, try again")
	}
	if resp.GetReserved() {
		return nil
	}
	msg := outOfStockMessage(resp.GetShortages(), prep)
	fields.Infof("[PlaceOrder] Reserve found a shortage, nothing was charged: %s", msg)
	return status.Error(codes.FailedPrecondition, msg)
}

// outOfStockMessage names every short product by its name (from the catalog data
// the order was priced with), in cart order, e.g.
// `out of stock: "Wool Scarf" (only 2 left, you asked for 3), "Tote Bag" (sold out)`.
func outOfStockMessage(shortages []*inventorypb.Shortage, prep orderPrep) string {
	names := map[string]string{}
	var order []string
	for i, it := range prep.orderItems {
		id := it.GetItem().GetProductId()
		if i < len(prep.products) && prep.products[i].GetName() != "" {
			names[id] = prep.products[i].GetName()
		}
		order = append(order, id)
	}
	byID := map[string]*inventorypb.Shortage{}
	for _, sh := range shortages {
		byID[sh.GetProductId()] = sh
		if _, ok := names[sh.GetProductId()]; !ok {
			names[sh.GetProductId()] = sh.GetProductId()
			order = append(order, sh.GetProductId())
		}
	}
	var parts []string
	seen := map[string]bool{}
	for _, id := range order {
		sh, ok := byID[id]
		if !ok || seen[id] {
			continue
		}
		seen[id] = true
		name := strings.Join(strings.Fields(names[id]), " ")
		if sh.GetAvailable() <= 0 {
			parts = append(parts, fmt.Sprintf("%q (sold out)", name))
		} else {
			parts = append(parts, fmt.Sprintf("%q (only %d left, you asked for %d)", name, sh.GetAvailable(), sh.GetRequested()))
		}
	}
	if len(parts) == 0 {
		return "out of stock: some items are no longer available"
	}
	if len(parts) > maxShortagesListed {
		more := len(parts) - maxShortagesListed
		parts = append(parts[:maxShortagesListed], fmt.Sprintf("and %d more", more))
	}
	return "out of stock: " + strings.Join(parts, ", ")
}

// releaseStock gives the held stock back after a failure. Best effort: it never
// returns an error, a failure is logged (the hold then expires by the TTL of
// inventory-service). It runs on a context detached from the caller's so a client
// that disconnected does not leak the hold.
func (s *Service) releaseStock(ctx context.Context, orderID string) {
	if !s.inventoryEnabled() {
		return
	}
	ctx = context.WithoutCancel(ctx)
	err := retryStock(func() error {
		actx, cancel := context.WithTimeout(ctx, stockTimeout)
		defer cancel()
		_, err := s.inventory().Release(actx, &inventorypb.ReleaseRequest{OrderId: orderID})
		return err
	}, func(err error) bool { return status.Code(err) != codes.FailedPrecondition })
	if err != nil {
		log.WithField("order_id", orderID).Errorf("STOCK NOT RELEASED: the reservation stays held until it expires: %v", err)
	}
}

// commitStock turns the reservation into sold stock after the charge and the
// shipment worked: 3 attempts with a short backoff, on a detached context. If it
// still fails the order goes on (the customer paid and the parcel is ordered); the
// failure is logged at error level and the reservation expires by its TTL.
func (s *Service) commitStock(ctx context.Context, orderID string) {
	if !s.inventoryEnabled() {
		return
	}
	ctx = context.WithoutCancel(ctx)
	err := retryStock(func() error {
		actx, cancel := context.WithTimeout(ctx, stockTimeout)
		defer cancel()
		_, err := s.inventory().Commit(actx, &inventorypb.CommitRequest{OrderId: orderID})
		return err
	}, func(error) bool { return true })
	if err != nil {
		log.WithFields(logrus.Fields{"order_id": orderID}).Errorf("STOCK NOT COMMITTED order=%s: the customer was charged and the parcel is ordered, the reservation expires by its TTL and the stock may be sold again: %v", orderID, err)
	}
}

// retryStock runs op up to stockAttempts times with stockBackoff between the tries.
// retryable decides whether an error is worth another attempt.
func retryStock(op func() error, retryable func(error) bool) error {
	var err error
	for i := 0; i < stockAttempts; i++ {
		if i > 0 {
			time.Sleep(stockBackoff[min(i-1, len(stockBackoff)-1)])
		}
		if err = op(); err == nil {
			return nil
		}
		if !retryable(err) {
			return err
		}
	}
	return err
}
