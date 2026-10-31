package checkout

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	pb "checkout-service/proto"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakePublisher is the EventPublisher of the tests: it records the call order on the
// shared recorder ("Publish") and every message.
type fakePublisher struct {
	rec *recorder
	err error
	// block makes Publish wait on this channel and ignore ctx (a misbehaving client).
	block chan struct{}
	// honourCtx makes Publish wait for ctx instead of for block.
	honourCtx bool

	mu       sync.Mutex
	msgs     []Message
	deadline []bool
}

func (f *fakePublisher) Publish(ctx context.Context, msg Message) error {
	f.rec.add("Publish")
	_, hasDeadline := ctx.Deadline()
	f.mu.Lock()
	f.msgs = append(f.msgs, msg)
	f.deadline = append(f.deadline, hasDeadline)
	f.mu.Unlock()
	if f.honourCtx {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.block != nil {
		<-f.block
	}
	return f.err
}

func (f *fakePublisher) messages() []Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Message(nil), f.msgs...)
}

// kafkaMocks is sagaMocks (orders + inventory) with the Kafka step switched on.
func kafkaMocks() (*mocks, *fakePublisher) {
	m := sagaMocks()
	return m, &fakePublisher{rec: m.orders.rec}
}

func withEvents(t *testing.T, m *mocks, pub *fakePublisher) *Service {
	t.Helper()
	svc, cleanup := startMockServers(t, m)
	t.Cleanup(cleanup)
	svc.Events = pub
	return svc
}

func decodeEvent(t *testing.T, msg Message) (orderPlacedEvent, map[string]any) {
	t.Helper()
	var ev orderPlacedEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		t.Fatalf("event is not JSON: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		t.Fatal(err)
	}
	return ev, raw
}

func TestEvent_ID_IsUUIDv5OfNamespaceAndOrderID(t *testing.T) {
	ns := uuid.NewSHA1(uuid.NameSpaceURL, []byte("everly/orders.placed"))
	if ns.String() != "60746bdc-faf5-5e28-91cf-d8fb2554d80b" {
		t.Fatalf("namespace = %s (pinned: other services derive event ids the same way)", ns)
	}
	// Cross-checked against Python: uuid.uuid5(uuid.uuid5(NAMESPACE_URL, 'everly/orders.placed'), '0d4c1a54-...')
	if got := EventID("0d4c1a54-7a52-4a58-9a5e-0e5b5b1f3a11"); got != "f9ebed3d-a36c-57ea-87c0-f0572344656e" {
		t.Errorf("event id = %s", got)
	}
	if EventID("a") == EventID("b") || EventID("a") != EventID("a") {
		t.Error("event id must be a pure function of the order id")
	}
	if u, err := uuid.Parse(EventID("x")); err != nil || u.Version() != 5 {
		t.Errorf("not a v5 uuid: %v %v", u, err)
	}
}

func TestEvent_HappyPath_ContentKeyHeaders_AccountOrder(t *testing.T) {
	m, pub := kafkaMocks()
	m.payment.transactionID = "TX-9"
	m.shipping.trackingID = "TRK-9"
	m.shipping.quote = &pb.Money{CurrencyCode: "USD", Units: 5, Nanos: 250000000}
	m.productCatalog.byID = map[string]*pb.Product{
		"scarf": {Id: "scarf", Name: "Wool Scarf", Picture: "/static/scarf.jpg", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 12, Nanos: 990000000}},
		"mug":   {Id: "mug", Name: "Mug", Picture: "/static/mug.jpg", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 8, Nanos: 5000000}},
	}
	m.cart.cart = &pb.Cart{UserId: "cart-key-1", Items: []*pb.CartItem{{ProductId: "scarf", Quantity: 3}, {ProductId: "mug", Quantity: 1}}}
	m.inventory.stock = map[string]int32{"scarf": 10, "mug": 10}
	svc := withEvents(t, m, pub)
	captured := captureLogs(t)

	fixed := time.Date(2026, 10, 31, 18, 0, 0, 123456789, time.UTC)
	old := now
	now = func() time.Time { return fixed }
	defer func() { now = old }()

	resp, err := svc.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId: "cart-key-1", AccountId: " 9b2f6a1e-0c3d-4e5f-8a7b-1c2d3e4f5a6b ", UserCurrency: "USD",
		Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	orderID := resp.GetOrder().GetOrderId()

	msgs := pub.messages()
	if len(msgs) != 1 {
		t.Fatalf("published %d messages, want 1", len(msgs))
	}
	msg := msgs[0]
	if msg.Key != orderID {
		t.Errorf("key = %q, want the order id %q", msg.Key, orderID)
	}
	wantHeaders := []Header{{"event-type", "order.placed"}, {"content-type", "application/json"}, {"schema-version", "1"}}
	if !reflect.DeepEqual(msg.Headers, wantHeaders) {
		t.Errorf("headers = %v, want %v", msg.Headers, wantHeaders)
	}

	ev, raw := decodeEvent(t, msg)
	// 3 x 12.99 + 8.005 + 5.25 = 38.97 + 8.005 + 5.25 = 52.225
	want := orderPlacedEvent{
		EventID: EventID(orderID), EventType: "order.placed", SchemaVersion: 1, OccurredAt: "2026-10-31T18:00:00.123Z",
		OrderID: orderID, UserID: "9b2f6a1e-0c3d-4e5f-8a7b-1c2d3e4f5a6b", Email: "user@example.com",
		ShippingAddress: eventAddress{StreetAddress: "1 Main St", City: "Testville", State: "NY", Country: "US", ZipCode: 10001},
		Items: []eventItem{
			{ProductID: "scarf", Name: "Wool Scarf", Quantity: 3, UnitPrice: eventMoney{"USD", 12, 990000000}, Picture: "/static/scarf.jpg"},
			{ProductID: "mug", Name: "Mug", Quantity: 1, UnitPrice: eventMoney{"USD", 8, 5000000}, Picture: "/static/mug.jpg"},
		},
		ShippingCost:         eventMoney{"USD", 5, 250000000},
		Total:                eventMoney{"USD", 52, 225000000},
		ShippingTrackingID:   "TRK-9",
		PaymentTransactionID: "TX-9",
	}
	if !reflect.DeepEqual(ev, want) {
		t.Errorf("event =\n%+v\nwant\n%+v", ev, want)
	}
	// The total is the very amount that was stored (and charged).
	if tot := m.orders.creates[0].GetTotal(); tot.GetUnits() != ev.Total.Units || tot.GetNanos() != ev.Total.Nanos {
		t.Errorf("event total %v != stored total %v", ev.Total, tot)
	}

	// Exactly the contract's fields, none missing, none extra.
	wantKeys := []string{"event_id", "event_type", "schema_version", "occurred_at", "order_id", "user_id", "email", "shipping_address",
		"items", "shipping_cost", "total", "shipping_tracking_id", "payment_transaction_id"}
	if len(raw) != len(wantKeys) {
		t.Errorf("event has %d fields, want %d: %v", len(raw), len(wantKeys), raw)
	}
	for _, k := range wantKeys {
		if _, ok := raw[k]; !ok {
			t.Errorf("field %q missing", k)
		}
	}
	// Never a session/cart key, card data or CVV.
	body := string(msg.Value)
	for _, secret := range []string{"cart-key-1", "session", defaultCard().CreditCardNumber, "cvv", "credit_card"} {
		if strings.Contains(strings.ToLower(body), strings.ToLower(secret)) {
			t.Errorf("event contains %q: %s", secret, body)
		}
	}
	// The payload is never logged.
	if l := captured.String(); strings.Contains(l, body) || strings.Contains(l, "street_address") || strings.Contains(l, "Wool Scarf") || strings.Contains(l, "9b2f6a1e") {
		t.Errorf("payload leaked into the log: %s", l)
	}
	if !pub.deadline[0] {
		t.Error("Publish got a context without a deadline")
	}
}

func TestEvent_AnonymousOrder_UserIDEmpty_NoSessionID(t *testing.T) {
	m, pub := kafkaMocks()
	svc := withEvents(t, m, pub)

	if _, err := placeOrder(t, svc); err != nil { // UserId "user-1", no AccountId
		t.Fatal(err)
	}
	msg := pub.messages()[0]
	ev, raw := decodeEvent(t, msg)
	if ev.UserID != "" {
		t.Errorf("user_id = %q, want empty for an anonymous order", ev.UserID)
	}
	if v, ok := raw["user_id"]; !ok || v != "" {
		t.Errorf("user_id must be present and empty: %v", raw["user_id"])
	}
	if strings.Contains(string(msg.Value), "user-1") {
		t.Errorf("session id in the event: %s", msg.Value)
	}
	// Whitespace-only account id is anonymous too.
	m2, pub2 := kafkaMocks()
	svc2 := withEvents(t, m2, pub2)
	if _, err := svc2.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId: "user-1", AccountId: "   ", UserCurrency: "USD", Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	}); err != nil {
		t.Fatal(err)
	}
	if ev2, _ := decodeEvent(t, pub2.messages()[0]); ev2.UserID != "" {
		t.Errorf("user_id = %q", ev2.UserID)
	}
}

func TestEvent_AllFieldsPresentWhenUnknown_ItemsIsAnArray(t *testing.T) {
	// A product without name/picture and an address without a state: strings are "", not missing/null.
	// (No order storage here: order-service itself rejects an empty item name.)
	m := defaultMocks()
	pub := &fakePublisher{rec: &recorder{}}
	m.productCatalog.product = &pb.Product{Id: "prod-1", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 10}}
	svc := withEvents(t, m, pub)
	if _, err := svc.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId: "user-1", UserCurrency: "USD", Email: "user@example.com", CreditCard: defaultCard(),
		Address: &pb.Address{StreetAddress: "1 Main St", City: "Testville", Country: "US"},
	}); err != nil {
		t.Fatal(err)
	}
	_, raw := decodeEvent(t, pub.messages()[0])
	item := raw["items"].([]any)[0].(map[string]any)
	if item["name"] != "" || item["picture"] != "" {
		t.Errorf("item = %v", item)
	}
	addr := raw["shipping_address"].(map[string]any)
	if addr["state"] != "" || addr["zip_code"] != float64(0) {
		t.Errorf("address = %v", addr)
	}
	// And a zero-line message never has "items": null.
	msg, err := buildOrderPlaced("o", &pb.PlaceOrderRequest{}, orderPrep{shippingCostLocalized: &pb.Money{}}, &pb.Money{}, "", "")
	if err != nil || !strings.Contains(string(msg.Value), `"items":[]`) {
		t.Errorf("empty items = %s, %v", msg.Value, err)
	}
}

func TestEvent_OrderOfSteps_PublishAfterCommitBeforeConfirm(t *testing.T) {
	m, pub := kafkaMocks()
	svc := withEvents(t, m, pub)

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "Publish", "ConfirmOrder")
}

func TestEvent_KafkaOn_NoDirectEmail_CartStillEmptied(t *testing.T) {
	m, pub := kafkaMocks()
	svc := withEvents(t, m, pub)

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	if m.email.calls != 0 {
		t.Errorf("email-service called %d times with Kafka on", m.email.calls)
	}
	if m.cart.emptyCalls != 1 {
		t.Errorf("cart emptied %d times", m.cart.emptyCalls)
	}
}

func TestEvent_KafkaOff_DirectEmailAsBefore_NothingPublished(t *testing.T) {
	m, pub := kafkaMocks()
	svc, cleanup := startMockServers(t, m) // svc.Events stays nil: KAFKA_BROKERS unset
	defer cleanup()

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	if m.email.calls != 1 {
		t.Errorf("email calls = %d, want 1", m.email.calls)
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "ConfirmOrder")
	if len(pub.messages()) != 0 {
		t.Error("published with Kafka off")
	}
}

func TestEvent_WithoutOrderStorage_StillPublishedWithItsOwnOrderID(t *testing.T) {
	m := defaultMocks() // no order-service, no inventory
	pub := &fakePublisher{rec: &recorder{}}
	svc := withEvents(t, m, pub)

	resp, err := placeOrder(t, svc)
	if err != nil {
		t.Fatal(err)
	}
	msgs := pub.messages()
	if len(msgs) != 1 || msgs[0].Key != resp.GetOrder().GetOrderId() {
		t.Fatalf("messages = %v", msgs)
	}
	if m.email.calls != 0 {
		t.Error("email called with Kafka on")
	}
}

func TestEvent_PublishFailure_OrderStillSucceeds_Logged_NoDirectEmail(t *testing.T) {
	m, pub := kafkaMocks()
	pub.err = errors.New("kafka: broker unreachable")
	svc := withEvents(t, m, pub)
	captured := captureLogs(t)

	resp, err := placeOrder(t, svc)
	if err != nil {
		t.Fatalf("a publish failure must not fail the order: %v", err)
	}
	orderID := resp.GetOrder().GetOrderId()
	// The saga went on: ConfirmOrder, cart emptied.
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "Publish", "ConfirmOrder")
	if m.cart.emptyCalls != 1 {
		t.Error("cart not emptied")
	}
	if m.email.calls != 0 {
		t.Error("contract: with Kafka on checkout never calls email-service, also when the publish failed")
	}
	logs := captured.String()
	if !strings.Contains(logs, "level=error") || !strings.Contains(logs, "EVENT NOT PUBLISHED order="+orderID) {
		t.Errorf("failure not logged at error level: %s", logs)
	}
	if strings.Contains(logs, string(pub.messages()[0].Value)) || strings.Contains(logs, "street_address") {
		t.Errorf("payload in the log: %s", logs)
	}
}

func TestEvent_PublishTimeout_Bounded_OrderStillSucceeds(t *testing.T) {
	old := publishTimeout
	publishTimeout = 300 * time.Millisecond
	defer func() { publishTimeout = old }()

	for name, pubFor := range map[string]func(m *mocks) *fakePublisher{
		"client honours the context": func(m *mocks) *fakePublisher { return &fakePublisher{rec: m.orders.rec, honourCtx: true} },
		"client ignores the context": func(m *mocks) *fakePublisher {
			return &fakePublisher{rec: m.orders.rec, block: make(chan struct{})} // never released during the call
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := sagaMocks()
			pub := pubFor(m)
			if pub.block != nil {
				defer close(pub.block) // let the leaked goroutine go at the end
			}
			svc := withEvents(t, m, pub)
			captured := captureLogs(t)

			start := time.Now()
			resp, err := placeOrder(t, svc)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("a hanging publish must not fail the order: %v", err)
			}
			// The timeout is 300 ms; the generous ceiling only proves "not stuck" on a slow host.
			if elapsed > 10*time.Second {
				t.Errorf("PlaceOrder took %v with a 300 ms publish timeout", elapsed)
			}
			assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "Publish", "ConfirmOrder")
			if !strings.Contains(captured.String(), "EVENT NOT PUBLISHED order="+resp.GetOrder().GetOrderId()) {
				t.Errorf("timeout not logged: %s", captured.String())
			}
		})
	}
}

func TestEvent_PublishedEvenIfCommitOrConfirmFail(t *testing.T) {
	// Commit that never works (logged as STOCK NOT COMMITTED) and a ConfirmOrder that never works
	// do not stop the event: the order is paid and shipped, the order-service consumer reconciles.
	m, pub := kafkaMocks()
	m.inventory.commitErr = status.Error(codes.Unavailable, "down")
	m.orders.confirmErr = status.Error(codes.Unavailable, "down")
	svc := withEvents(t, m, pub)

	if _, err := placeOrder(t, svc); err != nil {
		t.Fatal(err)
	}
	if len(pub.messages()) != 1 {
		t.Errorf("published %d, want 1", len(pub.messages()))
	}
}

func TestEvent_NotPublishedForFailedOrders(t *testing.T) {
	cases := map[string]func(m *mocks){
		"reserve short":     func(m *mocks) { m.inventory.stock = map[string]int32{"prod-1": 1} }, // asks for 2
		"reserve rpc error": func(m *mocks) { m.inventory.reserveErr = status.Error(codes.Unavailable, "down") },
		"charge declined": func(m *mocks) {
			m.payment.err = status.Error(codes.FailedPrecondition, "card_declined: declined")
		},
		"ship fails": func(m *mocks) { m.shipping.shipErr = status.Error(codes.Unavailable, "carrier down") },
		"empty cart": func(m *mocks) { m.cart.cart = &pb.Cart{UserId: "user-1"} },
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			m, pub := kafkaMocks()
			setup(m)
			svc := withEvents(t, m, pub)

			if _, err := placeOrder(t, svc); err == nil {
				t.Fatal("expected the order to fail")
			}
			if n := len(pub.messages()); n != 0 {
				t.Errorf("published %d events for a failed order", n)
			}
			for _, ev := range m.orders.rec.list() {
				if ev == "Publish" {
					t.Error("Publish was called")
				}
			}
		})
	}
}

func TestEvent_NoPublishOnIdempotentReplay(t *testing.T) {
	m, pub := kafkaMocks()
	svc := withEvents(t, m, pub)

	first, err := placeKeyed(svc, "form-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := placeKeyed(svc, "form-1")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if first.GetOrder().GetOrderId() != second.GetOrder().GetOrderId() {
		t.Fatal("not a replay")
	}
	msgs := pub.messages()
	if len(msgs) != 1 {
		t.Errorf("published %d events for an order and its replay, want 1", len(msgs))
	}
	if m.email.calls != 0 {
		t.Error("email called")
	}
	if msgs[0].Key != first.GetOrder().GetOrderId() {
		t.Errorf("key %q", msgs[0].Key)
	}
}

func TestEvent_ParseBrokers(t *testing.T) {
	cases := map[string][]string{
		"":                    nil,
		"  ":                  nil,
		",":                   nil,
		"kafka:9092":          {"kafka:9092"},
		" a:9092 , b:9092 ,,": {"a:9092", "b:9092"},
	}
	for in, want := range cases {
		if got := ParseBrokers(in); !reflect.DeepEqual(got, want) {
			t.Errorf("ParseBrokers(%q) = %v, want %v", in, got, want)
		}
	}
}
