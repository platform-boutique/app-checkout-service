package checkout

import (
	"context"
	"regexp"
	"strings"
	"sync"

	pb "checkout-service/proto"
	orderspb "checkout-service/proto/orders"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Idempotency (PlaceOrderRequest.idempotency_key). It needs order storage: the order
// in order-service is the record of what already happened. Without ORDER_SERVICE_ADDR
// the key is validated and otherwise ignored (random order id, nothing to look up).
//
// With a key the order id is UUIDv5(namespace, owner NUL key), owner = the account id
// when set, else the cart key (user_id), so two visitors never collide. Then:
//
//	no such order            -> a new attempt, normal flow (CreateOrder is idempotent by order id)
//	PLACED and later         -> the stored result is returned, nothing is charged, reserved, shipped or emptied again
//	PENDING                  -> ABORTED "this order is already being processed"
//	FAILED                   -> FAILED_PRECONDITION "this attempt failed, start the checkout again" (a new attempt needs a new key)
//
// The same key arriving twice at once in this process is stopped by an in-memory
// guard (ABORTED, like PENDING). Between several checkout replicas the guard does
// not exist: CreateOrder does not say whether it created the order, so two replicas
// receiving the same key within the same few milliseconds could both proceed
// (known gap, see docs/limitations.md).

const maxIdempotencyKeyLen = 64

var idempotencyKeyRe = regexp.MustCompile(`^[A-Za-z0-9._-]*$`)

// idempotencyNamespace is fixed forever: changing it changes every derived order id.
var idempotencyNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://platform-boutique.example/checkout-service/idempotency-key"))

func validIdempotencyKey(key string) bool {
	return len(key) <= maxIdempotencyKeyLen && idempotencyKeyRe.MatchString(key)
}

// idempotentOrderID is the deterministic order id for (owner, key).
func idempotentOrderID(req *pb.PlaceOrderRequest) string {
	owner := strings.TrimSpace(req.GetAccountId())
	if owner == "" {
		owner = req.GetUserId()
	}
	return uuid.NewSHA1(idempotencyNamespace, []byte(owner+"\x00"+req.GetIdempotencyKey())).String()
}

// inflight is the set of order ids being processed by this process.
type inflight struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

// acquire returns a release func, or nil when the id is already being processed.
func (f *inflight) acquire(id string) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, busy := f.ids[id]; busy {
		return nil
	}
	if f.ids == nil {
		f.ids = map[string]struct{}{}
	}
	f.ids[id] = struct{}{}
	return func() {
		f.mu.Lock()
		delete(f.ids, id)
		f.mu.Unlock()
	}
}

func errAlreadyProcessing() error {
	return status.Error(codes.Aborted, "this order is already being processed")
}

// replayStoredOrder looks the order up. It returns (response, nil) for a PLACED (or
// later) order, (nil, err) when the attempt must stop (PENDING, FAILED, lookup
// problem), and (nil, nil) when the order does not exist and the checkout goes on.
func (s *Service) replayStoredOrder(ctx context.Context, orderID string, req *pb.PlaceOrderRequest) (*pb.PlaceOrderResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, createOrderTimeout)
	defer cancel()
	get := &orderspb.GetOrderRequest{OrderId: orderID}
	// Defence in depth: the order must belong to the owner the id was derived from.
	if acc := strings.TrimSpace(req.GetAccountId()); acc != "" {
		get.UserId = acc
	} else {
		get.SessionId = req.GetUserId()
	}
	o, err := orderspb.NewOrderServiceClient(s.OrderConn).GetOrder(ctx, get)
	fields := log.WithField("order_id", orderID)
	if err != nil {
		switch c := status.Code(err); c {
		case codes.NotFound:
			return nil, nil
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Canceled:
			fields.Warnf("[PlaceOrder] GetOrder failed, nothing was charged: %v", err)
			return nil, toStatus(newCallError("order", err))
		default:
			fields.Errorf("[PlaceOrder] GetOrder was rejected (checkout bug or contract mismatch), nothing was charged: %v", err)
			return nil, internalErr()
		}
	}
	switch o.GetStatus() {
	case orderspb.OrderStatus_ORDER_STATUS_PLACED, orderspb.OrderStatus_ORDER_STATUS_PROCESSING,
		orderspb.OrderStatus_ORDER_STATUS_SHIPPED, orderspb.OrderStatus_ORDER_STATUS_DELIVERED:
		fields.Info("[PlaceOrder] idempotent replay: returning the stored order, nothing is charged again")
		return &pb.PlaceOrderResponse{Order: orderResultFromStored(o)}, nil
	case orderspb.OrderStatus_ORDER_STATUS_PENDING:
		fields.Info("[PlaceOrder] idempotent replay of an order that is still PENDING")
		return nil, errAlreadyProcessing()
	case orderspb.OrderStatus_ORDER_STATUS_FAILED:
		fields.Info("[PlaceOrder] idempotent replay of a FAILED order")
		return nil, status.Error(codes.FailedPrecondition, "this attempt failed, start the checkout again")
	default:
		fields.Errorf("[PlaceOrder] stored order has an unexpected status %v", o.GetStatus())
		return nil, internalErr()
	}
}

// orderResultFromStored builds the OrderResult of a stored order: id, tracking id,
// shipping cost and address, and the lines (quantity and unit price in the order currency).
func orderResultFromStored(o *orderspb.Order) *pb.OrderResult {
	a := o.GetShippingAddress()
	items := make([]*pb.OrderItem, len(o.GetItems()))
	for i, it := range o.GetItems() {
		items[i] = &pb.OrderItem{
			Item: &pb.CartItem{ProductId: it.GetProductId(), Quantity: it.GetQuantity()},
			Cost: fromOrderMoney(it.GetUnitPrice()),
		}
	}
	return &pb.OrderResult{
		OrderId:            o.GetOrderId(),
		ShippingTrackingId: o.GetShippingTrackingId(),
		ShippingCost:       fromOrderMoney(o.GetShippingCost()),
		ShippingAddress: &pb.Address{
			StreetAddress: a.GetStreetAddress(),
			City:          a.GetCity(),
			State:         a.GetState(),
			Country:       a.GetCountry(),
			ZipCode:       a.GetZipCode(),
		},
		Items: items,
	}
}

func fromOrderMoney(m *orderspb.Money) *pb.Money {
	return &pb.Money{CurrencyCode: m.GetCurrencyCode(), Units: m.GetUnits(), Nanos: m.GetNanos()}
}
