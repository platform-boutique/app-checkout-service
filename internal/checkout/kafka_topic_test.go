package checkout

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// fakeBroker answers CreateTopics with a fixed error code (or fails the request) and remembers what was asked.
type fakeBroker struct {
	code  int16
	err   error
	calls int
	got   *kmsg.CreateTopicsRequest
}

func (f *fakeBroker) Request(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
	f.calls++
	f.got = req.(*kmsg.CreateTopicsRequest)
	if f.err != nil {
		return nil, f.err
	}
	resp := kmsg.NewCreateTopicsResponse()
	rt := kmsg.NewCreateTopicsResponseTopic()
	rt.Topic = f.got.Topics[0].Topic
	rt.ErrorCode = f.code
	resp.Topics = append(resp.Topics, rt)
	return &resp, nil
}

func TestEnsureTopicAsksForTheBrokerDefaults(t *testing.T) {
	b := &fakeBroker{}
	if err := ensureTopic(context.Background(), b, "dev.orders.placed"); err != nil {
		t.Fatal(err)
	}
	r := b.got.Topics[0]
	if r.Topic != "dev.orders.placed" || r.NumPartitions != -1 || r.ReplicationFactor != -1 {
		t.Fatalf("request: %+v", r)
	}
	if b.got.TimeoutMillis <= 0 {
		t.Errorf("no broker-side timeout: %d", b.got.TimeoutMillis)
	}
}

func TestEnsureTopicExistingTopicIsSuccess(t *testing.T) {
	if err := ensureTopic(context.Background(), &fakeBroker{code: kerr.TopicAlreadyExists.Code}, "orders.placed"); err != nil {
		t.Fatalf("existing topic: %v", err)
	}
}

func TestEnsureTopicErrors(t *testing.T) {
	err := ensureTopic(context.Background(), &fakeBroker{code: kerr.TopicAuthorizationFailed.Code}, "orders.placed")
	if !errors.Is(err, errTopicNotAllowed) {
		t.Errorf("authorization failure = %v, want errTopicNotAllowed", err)
	}
	err = ensureTopic(context.Background(), &fakeBroker{code: kerr.InvalidReplicationFactor.Code}, "orders.placed")
	if err == nil || errors.Is(err, errTopicNotAllowed) {
		t.Errorf("replication error = %v, want a plain error", err)
	}
	boom := errors.New("broker unreachable")
	if err := ensureTopic(context.Background(), &fakeBroker{err: boom}, "orders.placed"); !errors.Is(err, boom) {
		t.Errorf("request error = %v, want the broker error wrapped", err)
	}
}

// While the broker cannot be asked, every Publish tries again; after a success it is not asked any more.
func TestPublisherCreatesTheTopicOnceBrokerAnswers(t *testing.T) {
	b := &fakeBroker{err: errors.New("down")}
	p := &KafkaPublisher{creator: b, topic: "orders.placed"}
	p.ensureTopicOnce(context.Background())
	if p.topicReady.Load() || b.calls != 1 {
		t.Fatalf("down broker: ready=%v calls=%d", p.topicReady.Load(), b.calls)
	}
	b.err = nil
	p.ensureTopicOnce(context.Background())
	if !p.topicReady.Load() || b.calls != 2 {
		t.Fatalf("up broker: ready=%v calls=%d", p.topicReady.Load(), b.calls)
	}
	p.ensureTopicOnce(context.Background())
	if b.calls != 2 {
		t.Fatalf("asked again after success: calls=%d", b.calls)
	}
}

// An ACL that forbids creating topics must not stop the publish: the topic may exist already.
func TestPublisherGoesOnWhenCreatingIsNotAllowed(t *testing.T) {
	b := &fakeBroker{code: kerr.TopicAuthorizationFailed.Code}
	p := &KafkaPublisher{creator: b, topic: "orders.placed"}
	p.ensureTopicOnce(context.Background())
	p.ensureTopicOnce(context.Background())
	if !p.topicReady.Load() || b.calls != 1 {
		t.Fatalf("ready=%v calls=%d: want one attempt, then assume the topic exists", p.topicReady.Load(), b.calls)
	}
}
