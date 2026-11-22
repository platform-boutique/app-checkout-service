package checkout

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"

	"checkout-service/internal/obs"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

// KafkaPublisher is the franz-go implementation of EventPublisher: acks=all,
// idempotent producer (franz-go's default, kept on explicitly), a 5 s delivery
// timeout and no batching delay. franz-go connects lazily, so creating it never
// fails because a broker is down: produce errors surface per call.
type KafkaPublisher struct {
	client  *kgo.Client
	creator requester // client; tests inject a fake
	topic   string
	// topicReady is set once the topic exists (or this client may not create it): until then every Publish
	// asks the broker to create it first (kafka_topic.go). Publish runs concurrently, hence atomic.
	topicReady atomic.Bool
}

// topicChars are the characters Kafka allows in a topic name; maxTopicLen is its length limit.
var topicChars = regexp.MustCompile(`^[a-zA-Z0-9._-]*$`)

const maxTopicLen = 249

// TopicName returns the topic the event is published to: the value of KAFKA_TOPIC_PREFIX (surrounding blanks
// ignored, empty = none) in front of OrdersPlacedTopic, so several environments can share one Kafka cluster
// (FUT-03). order-service and email-service build the same name from the same variable. A prefix that cannot be
// part of a Kafka topic name is an error (checked at start, so a typo is one line and not an event nobody reads).
func TopicName(prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if !topicChars.MatchString(prefix) {
		return "", fmt.Errorf("KAFKA_TOPIC_PREFIX %q: only letters, digits, '.', '_' and '-' are allowed", prefix)
	}
	if len(prefix)+len(OrdersPlacedTopic) > maxTopicLen {
		return "", fmt.Errorf("KAFKA_TOPIC_PREFIX is too long: the topic name may have at most %d characters", maxTopicLen)
	}
	return prefix + OrdersPlacedTopic, nil
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

// NewKafkaPublisher creates the producer for the given topic (TopicName of KAFKA_TOPIC_PREFIX; OrdersPlacedTopic
// without a prefix).
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("checkout-service"),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent writes are franz-go's default; they need acks=all, which is set above.
		kgo.RecordDeliveryTimeout(publishTimeout),
		kgo.ProduceRequestTimeout(publishTimeout),
		kgo.ProducerLinger(0),
		// The events are a few hundred bytes of JSON: compression gains nothing, and consumers in other languages (email-service uses
		// the pure-Python kafka-python, which has no codec libraries) cannot read compressed batches (UnsupportedCodecError).
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		// Safety net: the first Publish creates the topic itself (ensureTopic); where that is not allowed the broker's
		// auto-creation (compose: KAFKA_NUM_PARTITIONS=3) still takes care of it.
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &KafkaPublisher{client: cl, creator: cl, topic: topic}, nil
}

// buildRecord turns the message into a record and starts the PRODUCER span, a child of
// the PlaceOrder span in ctx; its W3C traceparent/tracestate go into the record headers so
// order-service and email-service continue the same trace (OBS-61).
func buildRecord(ctx context.Context, topic string, msg Message) (context.Context, *kgo.Record, trace.Span) {
	hs := make([]obs.KafkaHeader, len(msg.Headers))
	for i, h := range msg.Headers {
		hs[i] = obs.KafkaHeader{Key: h.Key, Value: []byte(h.Value)}
	}
	ctx, span := obs.ProducerSpan(ctx, topic, &hs)
	headers := make([]kgo.RecordHeader, len(hs))
	for i, h := range hs {
		headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}
	return ctx, &kgo.Record{Topic: topic, Key: []byte(msg.Key), Value: msg.Value, Headers: headers}, span
}

// Publish sends the record and waits for the acknowledgement of all in-sync replicas.
func (p *KafkaPublisher) Publish(ctx context.Context, msg Message) error {
	p.ensureTopicOnce(ctx)
	ctx, rec, span := buildRecord(ctx, p.topic, msg)
	err := p.client.ProduceSync(ctx, rec).FirstErr()
	obs.EndSpan(span, err)
	return err
}

// Close flushes what is still buffered (until ctx is done) and closes the client.
func (p *KafkaPublisher) Close(ctx context.Context) error {
	err := p.client.Flush(ctx)
	p.client.Close()
	return err
}
