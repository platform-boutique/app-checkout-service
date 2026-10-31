//go:build integration

package checkout

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	pb "checkout-service/proto"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Run against a throwaway single-node Kafka (see docs/development.md):
//
//	KAFKA_TEST_BROKERS=127.0.0.1:19094 go test -tags integration -count=1 ./internal/checkout/ -run Kafka
//
// The host's clock may jump: deadlines are generous (30 s) and nothing sleeps.
func testBrokers(t *testing.T) []string {
	t.Helper()
	b := os.Getenv("KAFKA_TEST_BROKERS")
	if b == "" {
		b = "127.0.0.1:19094"
	}
	return ParseBrokers(b)
}

// readBack consumes orders.placed from the start until a record with the key shows up.
func readBack(t *testing.T, brokers []string, key string) *kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(OrdersPlacedTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("record with key %q not read back within 30 s", key)
		}
		var found *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) == key {
				found = r
			}
		})
		if found != nil {
			return found
		}
	}
}

// recordingPublisher puts "Publish" on the call-order recorder, then delegates to the real client.
type recordingPublisher struct {
	inner EventPublisher
	rec   *recorder
}

func (r *recordingPublisher) Publish(ctx context.Context, msg Message) error {
	r.rec.add("Publish")
	return r.inner.Publish(ctx, msg)
}

func closePublisher(t *testing.T, pub *KafkaPublisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pub.Close(ctx); err != nil {
		t.Errorf("close/flush: %v", err)
	}
}

func TestKafka_PlaceOrder_PublishesRealEvent_ReadBack(t *testing.T) {
	brokers := testBrokers(t)
	pub, err := NewKafkaPublisher(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer closePublisher(t, pub)

	m := sagaMocks()
	m.payment.transactionID = "TX-INT-1"
	m.shipping.trackingID = "TRK-INT-1"
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	svc.Events = &recordingPublisher{inner: pub, rec: m.orders.rec}
	captured := captureLogs(t)

	start := time.Now()
	resp, err := svc.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
		UserId: "cart-key-int", AccountId: "acc-int-1", UserCurrency: "USD",
		Address: defaultAddress(), Email: "user@example.com", CreditCard: defaultCard(),
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	t.Logf("PlaceOrder with a real broker took %v", time.Since(start))
	orderID := resp.GetOrder().GetOrderId()
	if strings.Contains(captured.String(), "EVENT NOT PUBLISHED") {
		t.Fatalf("publish failed: %s", captured.String())
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "Publish", "ConfirmOrder")
	if m.email.calls != 0 {
		t.Error("email-service called with Kafka on")
	}

	rec := readBack(t, brokers, orderID)
	if rec.Topic != OrdersPlacedTopic {
		t.Errorf("topic = %q", rec.Topic)
	}
	headers := map[string]string{}
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}
	wantHeaders := map[string]string{"event-type": "order.placed", "content-type": "application/json", "schema-version": "1"}
	if len(headers) != len(wantHeaders) {
		t.Errorf("headers = %v", headers)
	}
	for k, v := range wantHeaders {
		if headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, headers[k], v)
		}
	}
	var ev orderPlacedEvent
	if err := json.Unmarshal(rec.Value, &ev); err != nil {
		t.Fatalf("value is not the event JSON: %v", err)
	}
	if ev.EventID != EventID(orderID) || ev.EventType != "order.placed" || ev.SchemaVersion != 1 || ev.OrderID != orderID ||
		ev.UserID != "acc-int-1" || ev.Email != "user@example.com" || ev.ShippingTrackingID != "TRK-INT-1" || ev.PaymentTransactionID != "TX-INT-1" {
		t.Errorf("event = %+v", ev)
	}
	// 2 x 10.00 + 5.00
	if ev.Total != (eventMoney{"USD", 25, 0}) || ev.ShippingCost != (eventMoney{"USD", 5, 0}) {
		t.Errorf("money: total %+v shipping %+v", ev.Total, ev.ShippingCost)
	}
	if len(ev.Items) != 1 || ev.Items[0].ProductID != "prod-1" || ev.Items[0].Quantity != 2 || ev.Items[0].UnitPrice != (eventMoney{"USD", 10, 0}) {
		t.Errorf("items = %+v", ev.Items)
	}
	if strings.Contains(string(rec.Value), "cart-key-int") {
		t.Error("session id in the event")
	}
	if _, err := time.Parse(time.RFC3339Nano, ev.OccurredAt); err != nil {
		t.Errorf("occurred_at %q: %v", ev.OccurredAt, err)
	}
}

// The same key always lands on the same partition (ordering per order id).
func TestKafka_SameKeySamePartition(t *testing.T) {
	brokers := testBrokers(t)
	pub, err := NewKafkaPublisher(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer closePublisher(t, pub)
	key := "order-partition-" + EventID(time.Now().String())
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := pub.Publish(ctx, Message{Key: key, Value: []byte(`{"n":1}`)})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(OrdersPlacedTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	partitions := map[int32]int{}
	for total(partitions) < 2 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read back %v within 30 s", partitions)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) == key {
				partitions[r.Partition]++
			}
		})
	}
	if len(partitions) != 1 {
		t.Errorf("one key on several partitions: %v", partitions)
	}
}

func total(m map[int32]int) (n int) {
	for _, v := range m {
		n += v
	}
	return n
}

// A broker that is down: the producer is created anyway, PlaceOrder succeeds within the
// bound and the failure is logged. Nothing listens on 127.0.0.1:1.
func TestKafka_BrokerDown_PlaceOrderStillSucceedsWithinBound(t *testing.T) {
	pub, err := NewKafkaPublisher([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatalf("a producer for a dead broker must still be created (lazy connect): %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = pub.Close(ctx) // may report undelivered records: that is the point of this test
	}()

	m := sagaMocks()
	svc, cleanup := startMockServers(t, m)
	defer cleanup()
	svc.Events = &recordingPublisher{inner: pub, rec: m.orders.rec}
	captured := captureLogs(t)

	start := time.Now()
	resp, err := placeOrder(t, svc)
	elapsed := time.Since(start)
	t.Logf("PlaceOrder with a dead broker took %v (publishTimeout %v)", elapsed, publishTimeout)
	if err != nil {
		t.Fatalf("PlaceOrder must succeed with the broker down: %v", err)
	}
	// publishTimeout is 5 s; the ceiling leaves room for the other (mocked) steps and a slow host.
	if elapsed < publishTimeout/2 || elapsed > publishTimeout+10*time.Second {
		t.Errorf("elapsed %v, want about the %v publish timeout", elapsed, publishTimeout)
	}
	assertEvents(t, m, "CreateOrder", "Reserve", "Charge", "ShipOrder", "Commit", "Publish", "ConfirmOrder")
	if !strings.Contains(captured.String(), "EVENT NOT PUBLISHED order="+resp.GetOrder().GetOrderId()) {
		t.Errorf("failure not logged: %s", captured.String())
	}
	if m.email.calls != 0 {
		t.Error("email-service called with Kafka on")
	}
}
