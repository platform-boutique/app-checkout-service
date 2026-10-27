package checkout

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"checkout-service/internal/money"
	pb "checkout-service/proto"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = logrus.New()

// Service implements hipstershop.CheckoutServiceServer.
type Service struct {
	pb.UnimplementedCheckoutServiceServer

	ProductCatalogConn *grpc.ClientConn
	CartConn           *grpc.ClientConn
	CurrencyConn       *grpc.ClientConn
	ShippingConn       *grpc.ClientConn
	PaymentConn        *grpc.ClientConn
	EmailConn          *grpc.ClientConn
}

func (s *Service) PlaceOrder(ctx context.Context, req *pb.PlaceOrderRequest) (*pb.PlaceOrderResponse, error) {
	log.Infof("[PlaceOrder] user_id=%q user_currency=%q", req.UserId, req.UserCurrency)

	if err := validateRequest(req); err != nil {
		return nil, err
	}

	orderID, err := uuid.NewUUID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate order uuid")
	}

	// Everything that can fail without side effects (cart, products, prices,
	// shipping quote) runs before the card is charged.
	prep, err := s.prepareOrderItemsAndShippingQuote(ctx, req.UserId, req.UserCurrency, req.Address)
	if err != nil {
		log.Warnf("[PlaceOrder] preparing the order failed: %v", err)
		return nil, toStatus(err)
	}

	total := &pb.Money{CurrencyCode: req.UserCurrency, Units: 0, Nanos: 0}
	total = money.Must(money.Sum(total, prep.shippingCostLocalized))
	for _, it := range prep.orderItems {
		qty := it.GetItem().GetQuantity()
		if qty < 0 {
			qty = 0
		}
		multPrice := money.MultiplySlow(it.Cost, uint32(qty))
		total = money.Must(money.Sum(total, multPrice))
	}

	txID, err := s.chargeCard(ctx, total, req.CreditCard)
	if err != nil {
		log.Warnf("[PlaceOrder] charge failed: %v", err)
		return nil, toStatus(err)
	}
	log.Infof("payment went through (transaction_id: %s)", txID)

	shippingTrackingID, err := s.shipOrder(ctx, req.Address, prep.cartItems)
	if err != nil {
		log.Errorf("[PlaceOrder] shipping failed after the charge (transaction_id: %s): %v", txID, err)
		return nil, s.compensate(ctx, orderID.String(), txID, total, toStatus(err))
	}

	_ = s.emptyUserCart(ctx, req.UserId)

	orderResult := &pb.OrderResult{
		OrderId:            orderID.String(),
		ShippingTrackingId: shippingTrackingID,
		ShippingCost:       prep.shippingCostLocalized,
		ShippingAddress:    req.Address,
		Items:              prep.orderItems,
	}

	if err := s.sendOrderConfirmation(ctx, req.Email, orderResult); err != nil {
		log.Warnf("failed to send order confirmation to %q: %+v", req.Email, err)
	} else {
		log.Infof("order confirmation email sent to %q", req.Email)
	}

	return &pb.PlaceOrderResponse{Order: orderResult}, nil
}

// --- internal helpers ---

type orderPrep struct {
	orderItems            []*pb.OrderItem
	cartItems             []*pb.CartItem
	shippingCostLocalized *pb.Money
}

func (s *Service) prepareOrderItemsAndShippingQuote(ctx context.Context, userID, userCurrency string, address *pb.Address) (orderPrep, error) {
	var out orderPrep

	cartItems, err := s.getUserCart(ctx, userID)
	if err != nil {
		return out, fmt.Errorf("cart failure: %w", err)
	}

	if len(cartItems) == 0 {
		return out, status.Error(codes.FailedPrecondition, "the cart is empty")
	}

	orderItems, err := s.prepOrderItems(ctx, cartItems, userCurrency)
	if err != nil {
		return out, fmt.Errorf("failed to prepare order: %w", err)
	}

	shippingUSD, err := s.quoteShipping(ctx, address, cartItems)
	if err != nil {
		return out, fmt.Errorf("shipping quote failure: %w", err)
	}

	shippingPrice, err := s.convertCurrency(ctx, shippingUSD, userCurrency)
	if err != nil {
		return out, fmt.Errorf("failed to convert shipping cost: %w", err)
	}

	out.shippingCostLocalized = shippingPrice
	out.cartItems = cartItems
	out.orderItems = orderItems
	return out, nil
}

func (s *Service) getUserCart(ctx context.Context, userID string) ([]*pb.CartItem, error) {
	cart, err := pb.NewCartServiceClient(s.CartConn).GetCart(ctx, &pb.GetCartRequest{UserId: userID})
	if err != nil {
		return nil, newCallError("cart", fmt.Errorf("failed to get user cart: %w", err))
	}
	return cart.GetItems(), nil
}

func (s *Service) emptyUserCart(ctx context.Context, userID string) error {
	_, err := pb.NewCartServiceClient(s.CartConn).EmptyCart(ctx, &pb.EmptyCartRequest{UserId: userID})
	if err != nil {
		return fmt.Errorf("failed to empty user cart: %w", err)
	}
	return nil
}

func (s *Service) prepOrderItems(ctx context.Context, items []*pb.CartItem, userCurrency string) ([]*pb.OrderItem, error) {
	type productResult struct {
		product *pb.Product
		err     error
	}

	products := make([]productResult, len(items))
	cl := pb.NewProductCatalogServiceClient(s.ProductCatalogConn)

	var wg sync.WaitGroup
	for i, item := range items {
		wg.Add(1)
		go func(i int, item *pb.CartItem) {
			defer wg.Done()
			p, err := cl.GetProduct(ctx, &pb.GetProductRequest{Id: item.GetProductId()})
			products[i] = productResult{p, newCallError("product", err)}
		}(i, item)
	}
	wg.Wait()

	type orderResult struct {
		item *pb.OrderItem
		err  error
	}

	out := make([]orderResult, len(items))
	for i, pr := range products {
		wg.Add(1)
		go func(i int, pr productResult) {
			defer wg.Done()
			if pr.err != nil {
				out[i] = orderResult{err: fmt.Errorf("failed to get product %q: %w", items[i].GetProductId(), pr.err)}
				return
			}
			price, err := s.convertCurrency(ctx, pr.product.GetPriceUsd(), userCurrency)
			if err != nil {
				out[i] = orderResult{err: fmt.Errorf("failed to convert price of %q to %s: %w", items[i].GetProductId(), userCurrency, err)}
				return
			}
			out[i] = orderResult{item: &pb.OrderItem{Item: items[i], Cost: price}}
		}(i, pr)
	}
	wg.Wait()

	result := make([]*pb.OrderItem, len(items))
	for i, r := range out {
		if r.err != nil {
			return nil, r.err
		}
		result[i] = r.item
	}
	return result, nil
}

func (s *Service) quoteShipping(ctx context.Context, address *pb.Address, items []*pb.CartItem) (*pb.Money, error) {
	resp, err := pb.NewShippingServiceClient(s.ShippingConn).GetQuote(ctx, &pb.GetQuoteRequest{
		Address: address,
		Items:   items,
	})
	if err != nil {
		return nil, newCallError("shipping", fmt.Errorf("failed to get shipping quote: %w", err))
	}
	return resp.GetCostUsd(), nil
}

func (s *Service) convertCurrency(ctx context.Context, from *pb.Money, toCurrency string) (*pb.Money, error) {
	result, err := pb.NewCurrencyServiceClient(s.CurrencyConn).Convert(ctx, &pb.CurrencyConversionRequest{
		From:   from,
		ToCode: toCurrency,
	})
	if err != nil {
		return nil, newCallError("currency", fmt.Errorf("failed to convert currency: %w", err))
	}
	return result, nil
}

func (s *Service) chargeCard(ctx context.Context, amount *pb.Money, paymentInfo *pb.CreditCardInfo) (string, error) {
	resp, err := pb.NewPaymentServiceClient(s.PaymentConn).Charge(ctx, &pb.ChargeRequest{
		Amount:     amount,
		CreditCard: paymentInfo,
	})
	if err != nil {
		return "", newCallError("payment", fmt.Errorf("could not charge the card: %w", err))
	}
	return resp.GetTransactionId(), nil
}

func (s *Service) sendOrderConfirmation(ctx context.Context, email string, order *pb.OrderResult) error {
	_, err := pb.NewEmailServiceClient(s.EmailConn).SendOrderConfirmation(ctx, &pb.SendOrderConfirmationRequest{
		Email: email,
		Order: order,
	})
	return err
}

func (s *Service) shipOrder(ctx context.Context, address *pb.Address, items []*pb.CartItem) (string, error) {
	resp, err := pb.NewShippingServiceClient(s.ShippingConn).ShipOrder(ctx, &pb.ShipOrderRequest{
		Address: address,
		Items:   items,
	})
	if err != nil {
		return "", newCallError("shipping", fmt.Errorf("shipment failed: %w", err))
	}
	return resp.GetTrackingId(), nil
}

// refundTimeout bounds the compensation call. The refund runs on a context
// detached from the caller's, so a caller that gave up (or whose deadline was
// used up by the failed shipment) does not leave the customer charged.
const refundTimeout = 10 * time.Second

// refundCharge asks payment-service to refund a charge (idempotent per transaction id).
func (s *Service) refundCharge(ctx context.Context, txID string, amount *pb.Money) (string, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), refundTimeout)
	defer cancel()
	resp, err := pb.NewPaymentServiceClient(s.PaymentConn).Refund(ctx, &pb.RefundRequest{
		TransactionId: txID,
		Amount:        amount,
	})
	if err != nil {
		return "", newCallError("payment", fmt.Errorf("refund failed: %w", err))
	}
	return resp.GetRefundId(), nil
}

// compensate undoes the charge after a later step failed and returns the error
// PlaceOrder reports: the original failure (cause), with a note on the refund.
// If the refund itself fails the original error is kept, the failure is logged
// at error level with the transaction id for manual follow-up, and the message
// says the refund is pending. Card data is never logged.
func (s *Service) compensate(ctx context.Context, orderID, txID string, amount *pb.Money, cause error) error {
	fields := log.WithFields(logrus.Fields{"transaction_id": txID, "order_id": orderID})
	refundID, err := s.refundCharge(ctx, txID, amount)
	if err != nil {
		fields.Errorf("REFUND FAILED, the customer is charged and must be refunded manually: %v", err)
		return withNote(cause, fmt.Sprintf("your payment (reference %s) was charged and the refund is pending", txID))
	}
	fields.WithField("refund_id", refundID).Info("charge refunded after a failed order")
	return withNote(cause, "your payment has been refunded")
}

// withNote appends a note to the message of a status error, keeping its code.
func withNote(err error, note string) error {
	st, _ := status.FromError(err)
	return status.Errorf(st.Code(), "%s; %s", st.Message(), note)
}

// validateRequest rejects requests that cannot succeed before any downstream call.
func validateRequest(req *pb.PlaceOrderRequest) error {
	switch {
	case strings.TrimSpace(req.GetUserId()) == "":
		return status.Error(codes.InvalidArgument, "order: user_id is required")
	case strings.TrimSpace(req.GetUserCurrency()) == "":
		return status.Error(codes.InvalidArgument, "order: user_currency is required")
	case req.GetAddress() == nil:
		return status.Error(codes.InvalidArgument, "order: shipping address is required")
	case req.GetCreditCard() == nil:
		return status.Error(codes.InvalidArgument, "order: credit card is required")
	}
	return nil
}

// Logger exposes the package logger so the entrypoint can attach telemetry hooks to it.
func Logger() *logrus.Logger { return log }
