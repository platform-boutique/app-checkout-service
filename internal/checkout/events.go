package checkout

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	pb "checkout-service/proto"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
)

// The orders.placed event (Kafka) is optional: with Service.Events == nil
// (KAFKA_BROKERS unset) PlaceOrder behaves as before, including the direct
// email-service call. When it is set the saga gets one more step:
//
//	... ShipOrder -> Commit -> Publish -> ConfirmOrder -> empty cart
//
// and checkout no longer calls email-service SendOrderConfirmation (the email
// consumer sends the mail from the event). Publishing is best effort: a failure
// is logged (EVENT NOT PUBLISHED order=<id>) and never fails the order. There is
// no outbox, so an order whose event was lost gets no event (see docs/limitations.md).
// A replay of a stored PLACED order (idempotency) never publishes.
const (
	// OrdersPlacedTopic is the topic of the event.
	OrdersPlacedTopic = "orders.placed"
	// eventSchemaVersion is the value of the schema_version header and field.
	eventSchemaVersion = 1
	// eventType is the value of the event-type header and of the event_type field.
	eventType = "order.placed"
	// eventContentType is the value of the content-type header.
	eventContentType = "application/json"
)

// publishTimeout bounds the whole publish step (a variable so tests are fast). The
// Kafka client uses the same value as its delivery timeout (see kafka.go).
var publishTimeout = 5 * time.Second

// eventNamespace is fixed forever: changing it changes every event_id. It is
// the UUIDv5 of the string 'everly/orders.placed' in the URL namespace.
var eventNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("everly/orders.placed"))

// now is the clock of occurred_at (a variable so tests can pin it).
var now = time.Now

var (
	eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "checkout_order_events_total",
		Help: "orders.placed publish attempts by result (published, failed).",
	}, []string{"result"})
	eventPublishSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "checkout_order_event_publish_seconds",
		Help:    "Duration of the orders.placed publish step.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})
)

// Header is one Kafka record header.
type Header struct{ Key, Value string }

// Message is what the publisher sends: the topic is the publisher's business.
type Message struct {
	Key     string
	Value   []byte
	Headers []Header
}

// EventPublisher sends one message and returns when it is acknowledged (or failed).
// Implementations must honour ctx; the service also bounds the call by itself.
type EventPublisher interface {
	Publish(ctx context.Context, msg Message) error
}

// EventID is the event_id of an order: UUIDv5(namespace, order id), so a republish
// of the same order has the same id.
func EventID(orderID string) string {
	return uuid.NewSHA1(eventNamespace, []byte(orderID)).String()
}

type eventMoney struct {
	CurrencyCode string `json:"currency_code"`
	Units        int64  `json:"units"`
	Nanos        int32  `json:"nanos"`
}

type eventAddress struct {
	StreetAddress string `json:"street_address"`
	City          string `json:"city"`
	State         string `json:"state"`
	Country       string `json:"country"`
	ZipCode       int32  `json:"zip_code"`
}

type eventItem struct {
	ProductID string     `json:"product_id"`
	Name      string     `json:"name"`
	Quantity  int32      `json:"quantity"`
	UnitPrice eventMoney `json:"unit_price"`
	Picture   string     `json:"picture"`
}

// orderPlacedEvent is the value of an orders.placed record (schema version 1).
// It deliberately has no session id, card data or CVV.
type orderPlacedEvent struct {
	EventID              string       `json:"event_id"`
	EventType            string       `json:"event_type"`
	SchemaVersion        int          `json:"schema_version"`
	OccurredAt           string       `json:"occurred_at"`
	OrderID              string       `json:"order_id"`
	UserID               string       `json:"user_id"`
	Email                string       `json:"email"`
	ShippingAddress      eventAddress `json:"shipping_address"`
	Items                []eventItem  `json:"items"`
	ShippingCost         eventMoney   `json:"shipping_cost"`
	Total                eventMoney   `json:"total"`
	ShippingTrackingID   string       `json:"shipping_tracking_id"`
	PaymentTransactionID string       `json:"payment_transaction_id"`
}

func toEventMoney(m *pb.Money) eventMoney {
	return eventMoney{CurrencyCode: m.GetCurrencyCode(), Units: m.GetUnits(), Nanos: m.GetNanos()}
}

// buildOrderPlaced builds the record from what the saga already has. user_id is the
// account id (empty for an anonymous order), never the cart/session key.
func buildOrderPlaced(orderID string, req *pb.PlaceOrderRequest, prep orderPrep, total *pb.Money, trackingID, txID string) (Message, error) {
	items := make([]eventItem, len(prep.orderItems))
	for i, it := range prep.orderItems {
		var p *pb.Product
		if i < len(prep.products) {
			p = prep.products[i]
		}
		items[i] = eventItem{
			ProductID: it.GetItem().GetProductId(),
			Name:      p.GetName(),
			Quantity:  it.GetItem().GetQuantity(),
			UnitPrice: toEventMoney(it.GetCost()),
			Picture:   p.GetPicture(),
		}
	}
	a := req.GetAddress()
	ev := orderPlacedEvent{
		EventID:       EventID(orderID),
		EventType:     eventType,
		SchemaVersion: eventSchemaVersion,
		OccurredAt:    now().UTC().Format("2006-01-02T15:04:05.000Z"),
		OrderID:       orderID,
		UserID:        strings.TrimSpace(req.GetAccountId()),
		Email:         strings.TrimSpace(req.GetEmail()),
		ShippingAddress: eventAddress{
			StreetAddress: a.GetStreetAddress(),
			City:          a.GetCity(),
			State:         a.GetState(),
			Country:       a.GetCountry(),
			ZipCode:       a.GetZipCode(),
		},
		Items:                items,
		ShippingCost:         toEventMoney(prep.shippingCostLocalized),
		Total:                toEventMoney(total),
		ShippingTrackingID:   trackingID,
		PaymentTransactionID: txID,
	}
	value, err := json.Marshal(ev)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Key:   orderID,
		Value: value,
		Headers: []Header{
			{Key: "event-type", Value: eventType},
			{Key: "content-type", Value: eventContentType},
			{Key: "schema-version", Value: "1"},
		},
	}, nil
}

// publishOrderPlaced publishes the event of a charged and shipped order. It never
// returns an error and never takes longer than publishTimeout: the money is taken
// and the parcel is ordered, so a broker problem must not fail (or hold up) the
// order. The failure is logged at error level; the payload is never logged.
func (s *Service) publishOrderPlaced(ctx context.Context, orderID string, req *pb.PlaceOrderRequest, prep orderPrep, total *pb.Money, trackingID, txID string) {
	if s.Events == nil {
		return
	}
	started := time.Now()
	err := s.publish(ctx, orderID, req, prep, total, trackingID, txID)
	eventPublishSeconds.Observe(time.Since(started).Seconds())
	if err != nil {
		eventsTotal.WithLabelValues("failed").Inc()
		log.WithFields(logrus.Fields{
			"order_id":       orderID,
			"transaction_id": txID,
			"tracking_id":    trackingID,
		}).Errorf("EVENT NOT PUBLISHED order=%s: the order is placed but the %s event was not sent (no outbox, nothing retries it, the email consumer will not send the confirmation): %v", orderID, OrdersPlacedTopic, err)
		return
	}
	eventsTotal.WithLabelValues("published").Inc()
	log.WithField("order_id", orderID).Infof("%s event published", OrdersPlacedTopic)
}

func (s *Service) publish(ctx context.Context, orderID string, req *pb.PlaceOrderRequest, prep orderPrep, total *pb.Money, trackingID, txID string) error {
	msg, err := buildOrderPlaced(orderID, req, prep, total, trackingID, txID)
	if err != nil {
		return err
	}
	// Detached from the caller (a customer who closed the tab must not lose the
	// event of a paid order), bounded by publishTimeout.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Events.Publish(ctx, msg) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// The hard bound: holds even for a publisher that does not honour ctx.
		return ctx.Err()
	}
}
