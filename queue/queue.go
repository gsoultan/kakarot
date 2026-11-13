// Package queue runs message consumers on the same typed endpoints as HTTP
// and gRPC. A broker is anything that implements Subscriber; Memory is an
// in-process one for tests, development and single-process event handling.
//
//	var OrderPlaced = queue.Topic[domain.Order]("order.placed")
//
//	c := &queue.Consumer{Service: "billing", Source: broker}
//	queue.Handle(c, OrderPlaced, "ChargeOrder", eps.ChargeOrder, middleware.Default(log, time.Minute))
//	app := &kakarot.App{Servers: []kakarot.Server{httpServer, c}}
//
// Delivery is at least once: make handlers idempotent.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"

	"github.com/gsoultan/kakarot"
)

// Topic names a message stream and fixes its payload type, so a producer
// and a consumer cannot disagree about what the topic carries.
type Topic[T any] string

// Encode builds a message for this topic.
func (t Topic[T]) Encode(key string, payload T) (Message, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return Message{}, kakarot.E(kakarot.CodeInternal, "encode message", err)
	}
	return Message{Topic: string(t), Key: key, Payload: b}, nil
}

// Decode reads a message's payload as this topic's type.
func (t Topic[T]) Decode(m Message) (T, error) {
	var v T
	if m.Topic != string(t) {
		return v, kakarot.E(kakarot.CodeInvalid, "message topic "+m.Topic+" is not "+string(t), nil)
	}
	if err := json.Unmarshal(m.Payload, &v); err != nil {
		return v, kakarot.E(kakarot.CodeInvalid, "decode message", err)
	}
	return v, nil
}

// Message is one delivery from a broker.
type Message struct {
	ID       string
	Topic    string
	Key      string // partition or ordering key
	Payload  []byte // JSON
	Attempts int32  // earlier failed deliveries, when the broker knows
}

// Handler processes one message. Returning nil acknowledges it; returning an
// error asks the broker to deliver it again.
type Handler func(ctx context.Context, m Message) error

// Subscriber is the consuming side of a broker. Subscribe delivers messages
// of topic to h until ctx is cancelled, then returns nil. Implement it over
// your broker's client (Kafka, NATS, SQS, Pub/Sub, ...).
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, h Handler) error
}

// Consumer binds topics to endpoints. It is a kakarot.Server: add it to
// App.Servers and it runs and stops with the application.
type Consumer struct {
	Service    string               // operation service name seen by middleware
	Source     Subscriber           // the broker
	Middleware []kakarot.Middleware // shared by every handler
	Logger     *slog.Logger
	// Retryable decides whether a failed message is redelivered. The default
	// redelivers everything except errors a retry cannot fix: CodeInvalid,
	// CodeNotFound, CodeUnauthorized and CodeForbidden.
	Retryable func(err error) bool
	// DeadLetter receives messages that will not be retried: undecodable
	// payloads and non-retryable errors. The default logs them.
	DeadLetter func(ctx context.Context, m Message, err error)

	mu     sync.Mutex
	subs   []subscription
	cancel context.CancelFunc
	done   chan struct{}
}

type subscription struct {
	topic string
	h     Handler
}

// Handle binds topic to an endpoint. The payload type is fixed by the topic,
// so the endpoint's request type is checked at compile time. name is the
// operation name seen by middleware. Call it before the consumer serves.
func Handle[T, Resp any](c *Consumer, topic Topic[T], name string, e kakarot.Endpoint[T, Resp], mws ...kakarot.Middleware) {
	op := kakarot.Op{Service: c.Service, Method: name, Transport: "queue"}
	e = kakarot.Wrap(op, e, append(append([]kakarot.Middleware{}, c.Middleware...), mws...)...)
	h := func(ctx context.Context, m Message) error {
		in, err := topic.Decode(m)
		if err != nil {
			c.deadLetter(ctx, m, err)
			return nil // redelivery cannot fix a payload that does not decode
		}
		if _, err := e(ctx, in); err != nil {
			if c.retryable(err) {
				return err
			}
			c.deadLetter(ctx, m, err)
		}
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs = append(c.subs, subscription{topic: string(topic), h: h})
}

func (c *Consumer) retryable(err error) bool {
	if c.Retryable != nil {
		return c.Retryable(err)
	}
	switch kakarot.CodeOf(err) {
	case kakarot.CodeInvalid, kakarot.CodeNotFound, kakarot.CodeUnauthorized, kakarot.CodeForbidden:
		return false
	}
	return true
}

func (c *Consumer) deadLetter(ctx context.Context, m Message, err error) {
	if c.DeadLetter != nil {
		c.DeadLetter(ctx, m, err)
		return
	}
	log := c.Logger
	if log == nil {
		log = slog.Default()
	}
	log.ErrorContext(ctx, "message dropped", "topic", m.Topic, "id", m.ID, "key", m.Key, "err", err)
}

// Serve subscribes every bound topic and blocks until Shutdown or until a
// subscription fails.
func (c *Consumer) Serve(ctx context.Context) error {
	if c.Source == nil {
		return errors.New("queue: Consumer.Source is nil")
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.mu.Lock()
	subs := append([]subscription{}, c.subs...)
	c.cancel, c.done = cancel, make(chan struct{})
	done := c.done
	c.mu.Unlock()
	defer close(done)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, len(subs))
	for _, s := range subs {
		wg.Go(func() {
			if err := c.Source.Subscribe(ctx, s.topic, s.h); err != nil && !errors.Is(err, context.Canceled) {
				errs <- err
				cancel() // one broken subscription stops the consumer
			}
		})
	}
	wg.Wait()
	close(errs)
	return <-errs // nil when no subscription failed
}

// Shutdown stops consuming and waits for in-flight handlers, or for ctx.
func (c *Consumer) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
