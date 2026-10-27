package checkout

import (
	"context"
	"net/mail"
	"strings"
	"time"
	"unicode/utf8"

	pb "checkout-service/proto"
	orderspb "checkout-service/proto/orders"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Order storage (order-service) is optional: with Service.OrderConn == nil
// (ORDER_SERVICE_ADDR unset) PlaceOrder behaves as it did before the integration.
// When it is set the order is stored before the card is charged:
//
//	validate, cart, prices, quote   (no side effects)
//	CreateOrder  (PENDING)          failure -> nothing is charged
//	Charge                          failure -> FailOrder
//	ShipOrder                       failure -> Refund, FailOrder
//	ConfirmOrder (PLACED)           failure -> logged, the customer still gets the success
//	empty cart, confirmation email
const (
	// createOrderTimeout bounds CreateOrder (it runs on the caller's context).
	createOrderTimeout = 5 * time.Second
	// settleOrderTimeout bounds FailOrder and ConfirmOrder. They run on a context
	// detached from the caller's, so a caller that gave up does not leave the
	// order PENDING after the money moved.
	settleOrderTimeout = 10 * time.Second

	// Limits of order-service (proto/orders/orders.proto) that checkout can hit
	// with data a customer controls. Checked up front so the customer gets a
	// readable InvalidArgument instead of an Internal error from CreateOrder.
	maxEmailLen     = 254
	minEmailLen     = 3
	maxSessionIDLen = 128
	maxStreetLen    = 200
	maxCityLen      = 100
	maxStateLen     = 100
	maxCountryLen   = 100
	maxOrderItems   = 100
	maxItemQuantity = 1000
	maxProductIDLen = 128
	maxNameLen      = 200
	maxPictureLen   = 500
	maxReasonLen    = 500
)

func (s *Service) ordersEnabled() bool { return s.OrderConn != nil }

func runeLen(v string) int { return utf8.RuneCountInString(strings.TrimSpace(v)) }

// validateForOrders is the extra request validation that applies only when order
// storage is configured: what order-service would reject with InvalidArgument.
func validateForOrders(req *pb.PlaceOrderRequest) error {
	email := strings.TrimSpace(req.GetEmail())
	if n := utf8.RuneCountInString(email); n < minEmailLen || n > maxEmailLen {
		return status.Error(codes.InvalidArgument, "order: a valid email address is required")
	}
	if a, err := mail.ParseAddress(email); err != nil || a.Address != email {
		return status.Error(codes.InvalidArgument, "order: a valid email address is required")
	}
	if runeLen(req.GetUserId()) > maxSessionIDLen {
		return status.Error(codes.InvalidArgument, "order: user_id is too long")
	}
	a := req.GetAddress()
	switch {
	case runeLen(a.GetStreetAddress()) == 0 || runeLen(a.GetStreetAddress()) > maxStreetLen:
		return status.Error(codes.InvalidArgument, "order: street address must be 1-200 characters")
	case runeLen(a.GetCity()) == 0 || runeLen(a.GetCity()) > maxCityLen:
		return status.Error(codes.InvalidArgument, "order: city must be 1-100 characters")
	case runeLen(a.GetState()) > maxStateLen:
		return status.Error(codes.InvalidArgument, "order: state must be at most 100 characters")
	case runeLen(a.GetCountry()) == 0 || runeLen(a.GetCountry()) > maxCountryLen:
		return status.Error(codes.InvalidArgument, "order: country must be 1-100 characters")
	case a.GetZipCode() < 0:
		return status.Error(codes.InvalidArgument, "order: zip code must not be negative")
	}
	return nil
}

// checkOrderLimits rejects carts that order-service would not accept, before any charge.
func checkOrderLimits(prep orderPrep) error {
	if len(prep.orderItems) > maxOrderItems {
		return status.Errorf(codes.InvalidArgument, "order: at most %d different products per order", maxOrderItems)
	}
	for _, it := range prep.orderItems {
		if q := it.GetItem().GetQuantity(); q < 1 || q > maxItemQuantity {
			return status.Errorf(codes.InvalidArgument, "order: quantity must be between 1 and %d", maxItemQuantity)
		}
	}
	return nil
}

// createOrder stores the order as PENDING. The error is the status PlaceOrder
// returns: Unavailable stays Unavailable, everything else (including
// InvalidArgument, which means checkout built a bad request) is Internal.
func (s *Service) createOrder(ctx context.Context, orderID string, req *pb.PlaceOrderRequest, prep orderPrep, total *pb.Money) error {
	ctx, cancel := context.WithTimeout(ctx, createOrderTimeout)
	defer cancel()
	_, err := orderspb.NewOrderServiceClient(s.OrderConn).CreateOrder(ctx, buildCreateOrder(orderID, req, prep, total))
	if err == nil {
		return nil
	}
	fields := log.WithField("order_id", orderID)
	switch c := status.Code(err); c {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Canceled:
		fields.Warnf("[PlaceOrder] CreateOrder failed, nothing was charged: %v", err)
		return toStatus(newCallError("order", err))
	default:
		fields.Errorf("[PlaceOrder] CreateOrder rejected the order (checkout bug or contract mismatch), nothing was charged: %v", err)
		return internalErr()
	}
}

// failOrder marks the order FAILED after a failure past CreateOrder. cause is the
// error PlaceOrder returns; its message becomes the stored reason (no card data:
// the messages are built from downstream status messages and never contain the
// card). It never returns an error: a FailOrder problem is logged and must not
// replace the original error.
func (s *Service) failOrder(ctx context.Context, orderID string, cause error) {
	if !s.ordersEnabled() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleOrderTimeout)
	defer cancel()
	_, err := orderspb.NewOrderServiceClient(s.OrderConn).FailOrder(ctx, &orderspb.FailOrderRequest{
		OrderId: orderID,
		Reason:  failureReason(cause),
	})
	if err != nil {
		log.WithField("order_id", orderID).Errorf("[PlaceOrder] FailOrder failed, the order stays PENDING: %v", err)
	}
}

// confirmOrder marks the order PLACED after the charge and the shipment worked.
// A failure here is only logged: the money is taken and the parcel is ordered,
// so PlaceOrder still reports success and the order stays PENDING for manual follow-up.
func (s *Service) confirmOrder(ctx context.Context, orderID, trackingID, txID string) {
	if !s.ordersEnabled() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleOrderTimeout)
	defer cancel()
	_, err := orderspb.NewOrderServiceClient(s.OrderConn).ConfirmOrder(ctx, &orderspb.ConfirmOrderRequest{
		OrderId:              orderID,
		ShippingTrackingId:   trackingID,
		PaymentTransactionId: txID,
	})
	if err != nil {
		log.WithFields(logrus.Fields{
			"order_id":       orderID,
			"transaction_id": txID,
			"tracking_id":    trackingID,
		}).Errorf("ORDER NOT CONFIRMED: the customer was charged and the parcel is ordered, but ConfirmOrder failed, the order stays PENDING and needs manual follow-up: %v", err)
	}
}

// failureReason is the short stored reason: the message of the status PlaceOrder returns.
func failureReason(cause error) string {
	msg := strings.Join(strings.Fields(status.Convert(cause).Message()), " ")
	if msg == "" {
		msg = "order failed"
	}
	if r := []rune(msg); len(r) > maxReasonLen {
		msg = string(r[:maxReasonLen])
	}
	return strings.ToValidUTF8(msg, "")
}

func buildCreateOrder(orderID string, req *pb.PlaceOrderRequest, prep orderPrep, total *pb.Money) *orderspb.CreateOrderRequest {
	items := make([]*orderspb.OrderItem, len(prep.orderItems))
	for i, it := range prep.orderItems {
		var p *pb.Product
		if i < len(prep.products) {
			p = prep.products[i]
		}
		name, picture := p.GetName(), p.GetPicture()
		if r := []rune(name); len(r) > maxNameLen {
			name = string(r[:maxNameLen])
		}
		if utf8.RuneCountInString(picture) > maxPictureLen {
			picture = "" // a truncated URL would be worse than none
		}
		items[i] = &orderspb.OrderItem{
			ProductId: it.GetItem().GetProductId(),
			Name:      name,
			Quantity:  it.GetItem().GetQuantity(),
			UnitPrice: toOrderMoney(it.GetCost()),
			Picture:   picture,
		}
	}
	a := req.GetAddress()
	return &orderspb.CreateOrderRequest{
		OrderId:   orderID,
		SessionId: req.GetUserId(),
		Email:     strings.TrimSpace(req.GetEmail()),
		ShippingAddress: &orderspb.Address{
			StreetAddress: a.GetStreetAddress(),
			City:          a.GetCity(),
			State:         a.GetState(),
			Country:       a.GetCountry(),
			ZipCode:       a.GetZipCode(),
		},
		Items:        items,
		ShippingCost: toOrderMoney(prep.shippingCostLocalized),
		Total:        toOrderMoney(total),
	}
}

func toOrderMoney(m *pb.Money) *orderspb.Money {
	return &orderspb.Money{CurrencyCode: m.GetCurrencyCode(), Units: m.GetUnits(), Nanos: m.GetNanos()}
}
