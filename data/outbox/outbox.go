// Package outbox implements the transactional outbox on storm: events are
// written in the same transaction as the business change, and a relay
// publishes them afterwards, at least once.
//
//	var OrderPlaced = outbox.Topic[domain.Order]("order.placed")
//
//	// inside TxManager.Within, next to the repository write:
//	err := OrderPlaced.Add(ctx, db, o.ID, o)
//
//	// a consumer decodes with the same typed topic:
//	o, err := OrderPlaced.Decode(msg)
//
// Add &model.OutboxEvent{} to your storm.Build / storm diff model so the
// outbox_events table is created with your schema.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/kakarot/data/internal/store/outboxevent"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/storm"
)

// Topic names an event stream and fixes its payload type, so producers and
// consumers cannot disagree about what a topic carries.
type Topic[T any] string

// Add writes an event to the outbox using the transaction in ctx (or db).
func (t Topic[T]) Add(ctx context.Context, db storm.Executor, key string, payload T) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return kakarot.E(kakarot.CodeInternal, "encode event", err)
	}
	ins := outboxevent.Create()
	ins.SetTopic(string(t))
	ins.SetKey(key)
	ins.SetPayload(b)
	_, err = ins.Insert(ctx, data.Executor(ctx, db))
	return data.MapErr(err)
}

// Decode reads a message's payload as this topic's type.
func (t Topic[T]) Decode(m Message) (T, error) {
	var v T
	if m.Topic != string(t) {
		return v, kakarot.E(kakarot.CodeInvalid, "message topic "+m.Topic+" is not "+string(t), nil)
	}
	if err := json.Unmarshal(m.Payload, &v); err != nil {
		return v, kakarot.E(kakarot.CodeInvalid, "decode event", err)
	}
	return v, nil
}

// Queue returns the same topic for the consuming side (package queue).
func (t Topic[T]) Queue() queue.Topic[T] { return queue.Topic[T](t) }

// QueuePublisher is the publishing side of a broker that speaks
// queue.Message; queue.Memory is one.
type QueuePublisher interface {
	Publish(ctx context.Context, m queue.Message) error
}

// ToQueue adapts a queue broker to a Publisher, so the relay delivers
// outbox events to queue consumers.
func ToQueue(b QueuePublisher) Publisher {
	return PublisherFunc(func(ctx context.Context, m Message) error {
		return b.Publish(ctx, queue.Message{ID: m.ID, Topic: m.Topic, Key: m.Key, Payload: m.Payload, Attempts: m.Attempts})
	})
}

// Message is an outbox event as handed to a Publisher.
type Message struct {
	ID        string
	Topic     string
	Key       string
	Payload   []byte
	Attempts  int32
	CreatedAt time.Time
}

// Publisher delivers messages to a broker (Kafka, NATS, SNS, a webhook).
// It must be safe to call again for the same message: delivery is at least
// once, and consumers should deduplicate by Message.ID.
type Publisher interface {
	Publish(ctx context.Context, m Message) error
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(ctx context.Context, m Message) error

func (f PublisherFunc) Publish(ctx context.Context, m Message) error { return f(ctx, m) }

// Relay polls the outbox and publishes pending events in creation order.
// Several relays may run at once: rows are claimed with
// FOR UPDATE SKIP LOCKED, so each event goes to one relay at a time.
type Relay struct {
	DB          storm.DB
	Publisher   Publisher
	Batch       int           // events per poll; default 100
	Interval    time.Duration // poll interval when idle; default 1s
	MaxAttempts int32         // stop retrying an event after this many failures; default 10
	Logger      *slog.Logger

	mu      sync.Mutex
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	started bool
}

// channels returns the stop and done channels. serving marks that Serve has
// started, so Shutdown knows whether there is anything to wait for.
func (r *Relay) channels(serving bool) (stop, done chan struct{}, started bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stop == nil {
		r.stop, r.done = make(chan struct{}), make(chan struct{})
	}
	r.started = r.started || serving
	return r.stop, r.done, r.started
}

// RunOnce publishes up to Batch pending events and reports how many were
// published.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	batch, maxAttempts := r.Batch, r.MaxAttempts
	if batch <= 0 {
		batch = 100
	}
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	published := 0
	err := storm.InTx(ctx, r.DB, func(ex storm.Executor) error {
		rows, err := outboxevent.New().
			Where(outboxevent.PublishedAt.IsNull(), outboxevent.Attempts.Lt(maxAttempts)).
			Order(outboxevent.CreatedAt.Asc()).
			Limit(int64(batch)).
			ForUpdateSkipLocked().
			List(ctx, ex)
		if err != nil {
			return err
		}
		for _, row := range rows {
			m := outboxevent.Mutate(row)
			perr := r.Publisher.Publish(ctx, Message{
				ID:        uuid.UUID(row.ID).String(),
				Topic:     row.Topic,
				Key:       row.Key,
				Payload:   row.Payload,
				Attempts:  row.Attempts,
				CreatedAt: row.CreatedAt,
			})
			if perr != nil {
				m.IncAttempts()
				m.SetLastError(perr.Error())
				r.log().WarnContext(ctx, "outbox publish failed", "topic", row.Topic, "id", uuid.UUID(row.ID).String(), "err", perr)
			} else {
				m.SetPublishedAtNow()
				published++
			}
			if err := m.Update(ctx, ex); err != nil {
				return err
			}
		}
		return nil
	})
	return published, data.MapErr(err)
}

func (r *Relay) log() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}

// Serve runs the relay until Shutdown or ctx ends; it implements
// kakarot.Server so App can run it beside the HTTP and gRPC servers.
func (r *Relay) Serve(ctx context.Context) error {
	stop, done, _ := r.channels(true)
	defer close(done)
	interval := r.Interval
	if interval <= 0 {
		interval = time.Second
	}
	for {
		n, err := r.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.log().ErrorContext(ctx, "outbox relay", "err", err)
		}
		if n > 0 {
			continue // more may be waiting
		}
		select {
		case <-ctx.Done():
			return nil
		case <-stop:
			return nil
		case <-time.After(interval):
		}
	}
}

// Shutdown stops the relay after its current batch.
func (r *Relay) Shutdown(ctx context.Context) error {
	stop, done, started := r.channels(false)
	r.once.Do(func() { close(stop) })
	if !started {
		return nil // Serve never ran; if it starts later it stops at once
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
