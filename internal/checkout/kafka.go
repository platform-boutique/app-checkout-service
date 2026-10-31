package checkout

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher is the franz-go implementation of EventPublisher: acks=all,
// idempotent producer (franz-go's default, kept on explicitly), a 5 s delivery
// timeout and no batching delay. franz-go connects lazily, so creating it never
// fails because a broker is down: produce errors surface per call.
type KafkaPublisher struct {
	client *kgo.Client
}

// ParseBrokers splits KAFKA_BROKERS (comma separated) into seed brokers.
// Empty or blank gives nil: Kafka is off.
func ParseBrokers(v string) []string {
	var out []string
	for _, b := range strings.Split(v, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// NewKafkaPublisher creates the producer for the orders.placed topic.
func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("checkout-service"),
		kgo.DefaultProduceTopic(OrdersPlacedTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent writes are franz-go's default; they need acks=all, which is set above.
		kgo.RecordDeliveryTimeout(publishTimeout),
		kgo.ProduceRequestTimeout(publishTimeout),
		kgo.ProducerLinger(0),
		// The events are a few hundred bytes of JSON: compression gains nothing, and consumers in other languages (email-service uses
		// the pure-Python kafka-python, which has no codec libraries) cannot read compressed batches (UnsupportedCodecError).
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		// The topic is auto-created on first use (compose: KAFKA_NUM_PARTITIONS=3).
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &KafkaPublisher{client: cl}, nil
}

// Publish sends the record and waits for the acknowledgement of all in-sync replicas.
func (p *KafkaPublisher) Publish(ctx context.Context, msg Message) error {
	headers := make([]kgo.RecordHeader, len(msg.Headers))
	for i, h := range msg.Headers {
		headers[i] = kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)}
	}
	rec := &kgo.Record{Key: []byte(msg.Key), Value: msg.Value, Headers: headers}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// Close flushes what is still buffered (until ctx is done) and closes the client.
func (p *KafkaPublisher) Close(ctx context.Context) error {
	err := p.client.Flush(ctx)
	p.client.Close()
	return err
}
