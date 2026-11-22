package checkout

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// createTopicTimeout bounds the CreateTopics request of the first publish. It stays well below publishTimeout, so a
// slow broker cannot use up the whole publish budget before the record is even sent.
const createTopicTimeout = 2 * time.Second

// requester is what ensureTopic needs from the client (*kgo.Client); tests inject a fake.
type requester interface {
	Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error)
}

// errTopicNotAllowed means the broker refused to create the topic for this client (ACL). The topic may exist anyway.
var errTopicNotAllowed = errors.New("not allowed to create the topic")

// ensureTopic creates the topic when it is missing (CODE-09: the service that publishes makes sure the topic is there;
// order-service does the same on the consuming side). Partition count and replication factor are the broker's
// defaults (-1); an existing topic is left as it is. Concurrent starters are fine: TOPIC_ALREADY_EXISTS is success.
func ensureTopic(ctx context.Context, cl requester, topic string) error {
	req := kmsg.NewCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	req.Topics = append(req.Topics, rt)
	req.TimeoutMillis = int32(createTopicTimeout / time.Millisecond)
	resp, err := cl.Request(ctx, &req)
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	r, ok := resp.(*kmsg.CreateTopicsResponse)
	if !ok {
		return fmt.Errorf("create topic %s: unexpected response %T", topic, resp)
	}
	for _, t := range r.Topics {
		switch ke := kerr.ErrorForCode(t.ErrorCode); {
		case ke == nil, errors.Is(ke, kerr.TopicAlreadyExists):
		case errors.Is(ke, kerr.TopicAuthorizationFailed):
			return fmt.Errorf("%w %s: %v", errTopicNotAllowed, topic, ke)
		default:
			return fmt.Errorf("create topic %s: %w", topic, ke)
		}
	}
	return nil
}

// ensureTopicOnce runs before a publish until it has worked once (or the broker said this client may not create the
// topic). It never fails the publish: when the topic cannot be created the record is sent anyway, and the broker's
// own auto-creation (kgo.AllowAutoTopicCreation) or an existing topic decides.
func (p *KafkaPublisher) ensureTopicOnce(ctx context.Context) {
	if p.topicReady.Load() {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, createTopicTimeout)
	defer cancel()
	switch err := ensureTopic(cctx, p.creator, p.topic); {
	case err == nil:
		p.topicReady.Store(true)
	case errors.Is(err, errTopicNotAllowed):
		log.WithContext(ctx).Warnf("%v; assuming it exists", err)
		p.topicReady.Store(true)
	default:
		log.WithContext(ctx).WithError(err).Warn("kafka topic could not be checked, publishing anyway")
	}
}
