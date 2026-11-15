// Package kafkax runs kakarot queue consumers and the outbox relay on Kafka.
// A Broker is a queue.Subscriber and an outbox.QueuePublisher.
//
//	broker := &kafkax.Broker{Brokers: []string{"localhost:9092"}, Group: "billing"}
//	defer broker.Close()
//
//	consumer := &queue.Consumer{Service: "billing", Source: broker}
//	relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
//
// A topic is a Kafka topic and a message's Key is the record key, so
// messages with one key stay in order. Delivery is at least once: an offset
// is committed only after its handler returned.
package kafkax

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gsoultan/kakarot/queue"
)

const (
	idHeader    = "Kakarot-Id"
	errorHeader = "Kakarot-Error"
)

// Broker publishes to and consumes from a Kafka cluster.
type Broker struct {
	Brokers []string // seed brokers, host:port
	// Group is the consumer group. Instances that share a group share the
	// work; different groups each receive every message.
	Group string
	// Options are added to every client: TLS, SASL, a client ID.
	Options []kgo.Opt
	// MaxDeliver caps how often one message is handed to a handler. Kafka
	// cannot redeliver one record, so a failed message is tried again in
	// place, which holds back its partition until it succeeds or the cap is
	// reached. The default is 10.
	MaxDeliver int
	// Backoff is the pause between those tries. The default is one second.
	Backoff time.Duration
	// DeadLetter, when set, is appended to the topic to name where a
	// message goes after MaxDeliver failures ("order.placed" + ".dead").
	// Without it such a message is logged and skipped.
	DeadLetter string
	Logger     *slog.Logger

	mu       sync.Mutex
	producer *kgo.Client
}

func (b *Broker) client(extra ...kgo.Opt) (*kgo.Client, error) {
	if len(b.Brokers) == 0 {
		return nil, errors.New("kafkax: Broker.Brokers is empty")
	}
	opts := append([]kgo.Opt{kgo.SeedBrokers(b.Brokers...)}, b.Options...)
	return kgo.NewClient(append(opts, extra...)...)
}

func (b *Broker) producing() (*kgo.Client, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.producer == nil {
		cl, err := b.client()
		if err != nil {
			return nil, err
		}
		b.producer = cl
	}
	return b.producer, nil
}

// Publish writes m to its topic and waits for the cluster to accept it.
func (b *Broker) Publish(ctx context.Context, m queue.Message) error {
	cl, err := b.producing()
	if err != nil {
		return err
	}
	return cl.ProduceSync(ctx, record(m)).FirstErr()
}

func record(m queue.Message) *kgo.Record {
	r := &kgo.Record{Topic: m.Topic, Value: m.Payload}
	if m.Key != "" {
		r.Key = []byte(m.Key)
	}
	for k, v := range m.Headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	if m.ID != "" {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: idHeader, Value: []byte(m.ID)})
	}
	return r
}

func message(r *kgo.Record) queue.Message {
	m := queue.Message{Topic: r.Topic, Key: string(r.Key), Payload: r.Value, Headers: map[string]string{}}
	for _, h := range r.Headers {
		if h.Key == idHeader {
			m.ID = string(h.Value)
			continue
		}
		m.Headers[h.Key] = string(h.Value)
	}
	if m.ID == "" {
		m.ID = r.Topic + "/" + strconv.Itoa(int(r.Partition)) + "/" + strconv.FormatInt(r.Offset, 10)
	}
	return m
}

// Subscribe delivers messages of topic to h until ctx is cancelled, then
// leaves the group and returns nil. A new group starts at the oldest
// message, so events published before the first start are not missed.
func (b *Broker) Subscribe(ctx context.Context, topic string, h queue.Handler) error {
	if b.Group == "" {
		return errors.New("kafkax: Broker.Group is empty")
	}
	cl, err := b.client(
		kgo.ConsumerGroup(b.Group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(), // a partition is not taken away in the middle of a batch
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			cl.AllowRebalance()
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			b.log().ErrorContext(ctx, "kafka fetch failed", "topic", t, "partition", p, "err", err)
		})
		stopped := false
		fetches.EachRecord(func(r *kgo.Record) {
			if stopped || !b.handle(ctx, cl, r, h) {
				stopped = true
				return
			}
			if err := cl.CommitRecords(ctx, r); err != nil && ctx.Err() == nil {
				b.log().ErrorContext(ctx, "kafka commit failed", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "err", err)
			}
		})
		cl.AllowRebalance()
		if stopped {
			return nil
		}
	}
}

// handle runs h on r until it succeeds or the message is given up on. It
// reports false when ctx ended first: the record is then not committed and
// is delivered again after a restart.
func (b *Broker) handle(ctx context.Context, cl *kgo.Client, r *kgo.Record, h queue.Handler) bool {
	maxDeliver, backoff := b.MaxDeliver, b.Backoff
	if maxDeliver <= 0 {
		maxDeliver = 10
	}
	if backoff <= 0 {
		backoff = time.Second
	}
	m := message(r)
	for {
		err := h(ctx, m)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		m.Attempts++
		if int(m.Attempts) >= maxDeliver {
			if b.DeadLetter == "" {
				b.log().ErrorContext(ctx, "message skipped after too many failures", "topic", m.Topic, "id", m.ID, "attempts", m.Attempts, "err", err)
				return true
			}
			dead := record(m)
			dead.Topic += b.DeadLetter
			dead.Headers = append(dead.Headers, kgo.RecordHeader{Key: errorHeader, Value: []byte(err.Error())})
			if perr := cl.ProduceSync(ctx, dead).FirstErr(); perr == nil {
				return true
			} else if ctx.Err() == nil {
				// The message is neither handled nor parked: keep it.
				b.log().ErrorContext(ctx, "dead letter publish failed", "topic", dead.Topic, "id", m.ID, "err", perr)
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
	}
}

// Close releases the publishing connection. Subscriptions end with their
// context.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.producer != nil {
		b.producer.Close()
		b.producer = nil
	}
}

func (b *Broker) log() *slog.Logger {
	if b.Logger != nil {
		return b.Logger
	}
	return slog.Default()
}
