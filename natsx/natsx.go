// Package natsx runs kakarot queue consumers and the outbox relay on NATS
// JetStream. A Broker is a queue.Subscriber and an outbox.QueuePublisher.
//
//	nc, _ := nats.Connect(url)
//	broker := &natsx.Broker{Conn: nc, Stream: "ORDERS", Subjects: []string{"order.>"}, Group: "billing"}
//
//	consumer := &queue.Consumer{Service: "billing", Source: broker}
//	relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
//
// A topic is a NATS subject. Delivery is at least once: a handler error asks
// JetStream to deliver the message again, up to MaxDeliver times.
package natsx

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gsoultan/kakarot/queue"
)

// keyHeader carries queue.Message.Key, which NATS has no field for.
const keyHeader = "Kakarot-Key"

// Broker publishes to and consumes from one JetStream stream.
type Broker struct {
	Conn   *nats.Conn
	Stream string // stream name
	// Subjects, when set, creates the stream or updates it to capture these
	// subjects. Leave it empty when the stream is managed elsewhere.
	Subjects []string
	// Group names the durable consumers. Instances that share a group share
	// the work; different groups each receive every message.
	Group string
	// AckWait is how long a handler may run before the message is
	// delivered again. The default is 30 seconds.
	AckWait time.Duration
	// MaxDeliver caps deliveries of one message. The default is 10.
	MaxDeliver int
	// Backoff is the pause before a failed message is delivered again. The
	// default is one second.
	Backoff time.Duration

	once   sync.Once
	js     jetstream.JetStream
	stream jetstream.Stream
	err    error
}

func (b *Broker) init(ctx context.Context) error {
	b.once.Do(func() {
		if b.Conn == nil || b.Stream == "" {
			b.err = errors.New("natsx: Broker needs Conn and Stream")
			return
		}
		if b.js, b.err = jetstream.New(b.Conn); b.err != nil {
			return
		}
		if len(b.Subjects) > 0 {
			b.stream, b.err = b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: b.Stream, Subjects: b.Subjects})
			return
		}
		b.stream, b.err = b.js.Stream(ctx, b.Stream)
	})
	return b.err
}

// Publish stores m in the stream. A message with an ID is deduplicated by
// JetStream, so a relay that publishes the same event twice delivers it once.
func (b *Broker) Publish(ctx context.Context, m queue.Message) error {
	if err := b.init(ctx); err != nil {
		return err
	}
	msg := &nats.Msg{Subject: m.Topic, Data: m.Payload, Header: nats.Header{}}
	for k, v := range m.Headers {
		msg.Header.Set(k, v)
	}
	if m.Key != "" {
		msg.Header.Set(keyHeader, m.Key)
	}
	var opts []jetstream.PublishOpt
	if m.ID != "" {
		opts = append(opts, jetstream.WithMsgID(m.ID))
	}
	_, err := b.js.PublishMsg(ctx, msg, opts...)
	return err
}

// Subscribe delivers messages of topic to h until ctx is cancelled, waits
// for handlers in flight, then returns nil.
func (b *Broker) Subscribe(ctx context.Context, topic string, h queue.Handler) error {
	if err := b.init(ctx); err != nil {
		return err
	}
	if b.Group == "" {
		return errors.New("natsx: Broker.Group is empty")
	}
	ackWait, maxDeliver, backoff := b.AckWait, b.MaxDeliver, b.Backoff
	if ackWait <= 0 {
		ackWait = 30 * time.Second
	}
	if maxDeliver <= 0 {
		maxDeliver = 10
	}
	if backoff <= 0 {
		backoff = time.Second
	}
	cons, err := b.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable(b.Group, topic),
		FilterSubject: topic,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    maxDeliver,
	})
	if err != nil {
		return err
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		if err := h(ctx, message(msg)); err != nil {
			_ = msg.NakWithDelay(backoff)
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	cc.Drain()
	<-cc.Closed()
	return nil
}

func message(msg jetstream.Msg) queue.Message {
	m := queue.Message{Topic: msg.Subject(), Payload: msg.Data(), Headers: map[string]string{}}
	for k, v := range msg.Headers() {
		if len(v) == 0 {
			continue
		}
		switch k {
		case keyHeader:
			m.Key = v[0]
		case jetstream.MsgIDHeader:
			m.ID = v[0]
		default:
			m.Headers[k] = v[0]
		}
	}
	if meta, err := msg.Metadata(); err == nil {
		if m.ID == "" {
			m.ID = meta.Stream + "/" + strconv.FormatUint(meta.Sequence.Stream, 10)
		}
		if meta.NumDelivered > 1 {
			m.Attempts = int32(min(meta.NumDelivered-1, 1<<31-1))
		}
	}
	return m
}

// durable builds a consumer name, which may not hold the characters a
// subject uses.
func durable(group, topic string) string {
	return strings.NewReplacer(".", "_", "*", "any", ">", "all", " ", "_").Replace(group + "-" + topic)
}
