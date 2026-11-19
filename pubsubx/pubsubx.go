// Package pubsubx runs kakarot queue consumers and the outbox relay on
// Google Cloud Pub/Sub. A Broker is a queue.Subscriber and an
// outbox.QueuePublisher.
//
//	client, _ := pubsub.NewClient(ctx, "my-project")
//	broker := &pubsubx.Broker{Client: client, Group: "billing"}
//	defer broker.Close()
//
//	consumer := &queue.Consumer{Service: "billing", Source: broker}
//	relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
//
// A topic is a Pub/Sub topic ID. Each group reads a topic through its own
// subscription, named group + "." + topic, so instances that share a group
// share the work and different groups each receive every message. Delivery
// is at least once: a handler error asks Pub/Sub to deliver the message
// again, after the subscription's retry policy.
package pubsubx

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/gsoultan/kakarot/queue"
)

const (
	idAttribute  = "Kakarot-Id"
	keyAttribute = "Kakarot-Key"
)

// Broker publishes to and consumes from Pub/Sub topics of one project.
type Broker struct {
	Client *pubsub.Client
	// Group names the subscriptions: "billing" reads topic "order.placed"
	// through subscription "billing.order.placed".
	Group string
	// Create, when set, creates missing topics and subscriptions with the
	// settings below, which suits development and the emulator. Leave it
	// off when they are managed elsewhere; the settings below then only
	// describe what the subscriptions should have.
	Create bool
	// Ordered publishes a message's Key as its ordering key, so messages
	// with one key are delivered in order, and creates subscriptions with
	// message ordering on.
	Ordered bool
	// Concurrency caps the messages one subscription handles at once. The
	// default is 10.
	Concurrency int
	// Backoff is the shortest pause before a failed message is delivered
	// again: the minimum backoff of a created subscription's retry policy.
	// The default is one second, and Pub/Sub allows at most ten minutes.
	Backoff time.Duration
	// DeadLetter, when set, is appended to the topic to name the topic that
	// takes a message after MaxDeliver deliveries ("order.placed" +
	// ".dead"). Without it Pub/Sub keeps delivering a failing message until
	// its retention ends.
	DeadLetter string
	// MaxDeliver caps deliveries of one message when DeadLetter is set.
	// Pub/Sub allows 5 to 100; the default is 10.
	MaxDeliver int

	mu      sync.Mutex
	pubs    map[string]*pubsub.Publisher
	created map[string]bool
}

// Publish sends m to its topic and waits for Pub/Sub to store it. Headers,
// the ID and the key travel as attributes. Pub/Sub assigns its own message
// IDs and does not deduplicate on the event's ID, so make handlers
// idempotent.
func (b *Broker) Publish(ctx context.Context, m queue.Message) error {
	if b.Client == nil {
		return errors.New("pubsubx: Broker.Client is nil")
	}
	if b.Create {
		if err := b.createTopic(ctx, m.Topic); err != nil {
			return err
		}
	}
	msg := &pubsub.Message{Data: m.Payload, Attributes: map[string]string{}}
	for k, v := range m.Headers {
		msg.Attributes[k] = v
	}
	if m.ID != "" {
		msg.Attributes[idAttribute] = m.ID
	}
	if m.Key != "" {
		msg.Attributes[keyAttribute] = m.Key
		if b.Ordered {
			msg.OrderingKey = m.Key
		}
	}
	p := b.publisher(m.Topic)
	if _, err := p.Publish(ctx, msg).Get(ctx); err != nil {
		if msg.OrderingKey != "" {
			p.ResumePublish(msg.OrderingKey) // a failure pauses the key until resumed
		}
		return err
	}
	return nil
}

func (b *Broker) publisher(topic string) *pubsub.Publisher {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p := b.pubs[topic]; p != nil {
		return p
	}
	if b.pubs == nil {
		b.pubs = map[string]*pubsub.Publisher{}
	}
	p := b.Client.Publisher(topic)
	p.EnableMessageOrdering = b.Ordered
	b.pubs[topic] = p
	return p
}

// Subscribe delivers messages of the group's subscription to topic to h
// until ctx is cancelled, waits for handlers in flight, then returns nil.
// A handled message is acknowledged; a failed one is delivered again.
func (b *Broker) Subscribe(ctx context.Context, topic string, h queue.Handler) error {
	if b.Client == nil {
		return errors.New("pubsubx: Broker.Client is nil")
	}
	if b.Group == "" {
		return errors.New("pubsubx: Broker.Group is empty")
	}
	name := subscriptionID(b.Group, topic)
	if b.Create {
		if err := b.createSubscription(ctx, name, topic); err != nil {
			return err
		}
	}
	s := b.Client.Subscriber(name)
	s.ReceiveSettings.MaxOutstandingMessages = b.concurrency()
	err := s.Receive(ctx, func(ctx context.Context, raw *pubsub.Message) {
		if err := h(ctx, message(topic, raw)); err != nil {
			raw.Nack()
			return
		}
		raw.Ack()
	})
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// Close sends what is still batched and stops the publishers. It does not
// close the Client.
func (b *Broker) Close() error {
	b.mu.Lock()
	pubs := b.pubs
	b.pubs = nil
	b.mu.Unlock()
	for _, p := range pubs {
		p.Stop()
	}
	return nil
}

func (b *Broker) createTopic(ctx context.Context, topic string) error {
	name := "projects/" + b.Client.Project() + "/topics/" + topic
	if b.wasCreated(name) {
		return nil
	}
	_, err := b.Client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return err
	}
	b.markCreated(name)
	return nil
}

func (b *Broker) createSubscription(ctx context.Context, id, topic string) error {
	name := "projects/" + b.Client.Project() + "/subscriptions/" + id
	if b.wasCreated(name) {
		return nil
	}
	if err := b.createTopic(ctx, topic); err != nil {
		return err
	}
	sub := subscription(b.Client.Project(), id, topic, b.Ordered, b.Backoff, b.DeadLetter, b.MaxDeliver)
	if b.DeadLetter != "" {
		if err := b.createTopic(ctx, topic+b.DeadLetter); err != nil {
			return err
		}
	}
	_, err := b.Client.SubscriptionAdminClient.CreateSubscription(ctx, sub)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return err
	}
	b.markCreated(name)
	return nil
}

func (b *Broker) wasCreated(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.created[name]
}

func (b *Broker) markCreated(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.created == nil {
		b.created = map[string]bool{}
	}
	b.created[name] = true
}

// subscriptionID names the subscription a group reads topic through.
func subscriptionID(group, topic string) string {
	return group + "." + topic
}

// subscription describes the subscription Create makes.
func subscription(project, id, topic string, ordered bool, backoff time.Duration, deadLetter string, maxDeliver int) *pubsubpb.Subscription {
	const maxBackoff = 10 * time.Minute
	if backoff <= 0 {
		backoff = time.Second
	}
	backoff = min(backoff, maxBackoff)
	sub := &pubsubpb.Subscription{
		Name:                  "projects/" + project + "/subscriptions/" + id,
		Topic:                 "projects/" + project + "/topics/" + topic,
		EnableMessageOrdering: ordered,
		RetryPolicy:           &pubsubpb.RetryPolicy{MinimumBackoff: durationpb.New(backoff), MaximumBackoff: durationpb.New(maxBackoff)},
	}
	if deadLetter != "" {
		if maxDeliver <= 0 {
			maxDeliver = 10
		}
		sub.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     "projects/" + project + "/topics/" + topic + deadLetter,
			MaxDeliveryAttempts: int32(min(max(maxDeliver, 5), 100)),
		}
	}
	return sub
}

func message(topic string, raw *pubsub.Message) queue.Message {
	m := queue.Message{ID: raw.ID, Topic: topic, Key: raw.OrderingKey, Payload: raw.Data, Headers: map[string]string{}}
	for k, v := range raw.Attributes {
		switch k {
		case idAttribute:
			m.ID = v
		case keyAttribute:
			m.Key = v
		default:
			m.Headers[k] = v
		}
	}
	// Pub/Sub counts deliveries only on a subscription with a dead-letter
	// policy.
	if raw.DeliveryAttempt != nil && *raw.DeliveryAttempt > 1 {
		m.Attempts = int32(*raw.DeliveryAttempt - 1)
	}
	return m
}

func (b *Broker) concurrency() int {
	if b.Concurrency > 0 {
		return b.Concurrency
	}
	return 10
}
