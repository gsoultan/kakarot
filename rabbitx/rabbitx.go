// Package rabbitx runs kakarot queue consumers and the outbox relay on
// RabbitMQ. A Broker is a queue.Subscriber and an outbox.QueuePublisher.
//
//	broker := &rabbitx.Broker{URL: "amqp://guest:guest@localhost:5672/", Exchange: "orders", Group: "billing"}
//	defer broker.Close()
//
//	consumer := &queue.Consumer{Service: "billing", Source: broker}
//	relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
//
// A topic is the routing key on a durable topic exchange. Each group gets a
// quorum queue per topic, bound to the exchange, so instances that share a
// group share the work and different groups each receive every message.
// Delivery is at least once: a handler error returns the message to its
// queue after Backoff, up to MaxDeliver deliveries, then the message goes to
// the DeadLetter exchange.
package rabbitx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/queue"
)

// keyHeader carries queue.Message.Key, which AMQP has no field for.
const keyHeader = "Kakarot-Key"

// Broker publishes to and consumes from one RabbitMQ topic exchange. It
// dials the server on first use and dials again after the connection drops.
type Broker struct {
	URL string // amqp:// or amqps:// URL
	// Config sets TLS, credentials, heartbeat and the like. The zero value
	// uses the library's defaults.
	Config amqp.Config
	// Exchange is the durable topic exchange to publish to and bind queues
	// to. The broker declares it.
	Exchange string
	// Group names the queues: "billing" consumes topic "order.placed" from
	// queue "billing.order.placed". Instances that share a group share the
	// work; different groups each receive every message.
	Group string
	// Prefetch caps the messages one subscription holds unacknowledged.
	// The default is 10.
	Prefetch int
	// MaxDeliver caps deliveries of one message. The broker rejects a
	// message that failed that often, and RabbitMQ then routes it to
	// DeadLetter, or drops it. It is also the queue's delivery limit. The
	// default is 10.
	MaxDeliver int
	// Backoff is the pause before a failed message goes back to its queue.
	// The default is one second.
	Backoff time.Duration
	// DeadLetter, when set, names the exchange that takes a message after
	// MaxDeliver failures, with its original routing key. The broker does
	// not declare it.
	DeadLetter string
	Logger     *slog.Logger

	mu     sync.Mutex
	conn   *amqp.Connection
	closed bool

	pubMu   sync.Mutex
	pub     *amqp.Channel
	returns chan amqp.Return
}

func (b *Broker) check() error {
	if b.URL == "" || b.Exchange == "" {
		return errors.New("rabbitx: Broker needs URL and Exchange")
	}
	return nil
}

// connection returns the open connection, dialing a new one when there is
// none or the last one dropped.
func (b *Broker) connection() (*amqp.Connection, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, errors.New("rabbitx: Broker is closed")
	}
	if b.conn != nil && !b.conn.IsClosed() {
		return b.conn, nil
	}
	conn, err := amqp.DialConfig(b.URL, b.Config)
	if err != nil {
		return nil, err
	}
	b.conn = conn
	return conn, nil
}

func (b *Broker) declareExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(b.Exchange, amqp.ExchangeTopic, true, false, false, false, nil)
}

// publishing returns the publishing channel, opening one in confirm mode
// when there is none or the last one closed. Call it with pubMu held.
func (b *Broker) publishing() (*amqp.Channel, error) {
	if b.pub != nil && !b.pub.IsClosed() {
		return b.pub, nil
	}
	conn, err := b.connection()
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, err
	}
	if err := b.declareExchange(ch); err != nil {
		_ = ch.Close()
		return nil, err
	}
	b.pub, b.returns = ch, ch.NotifyReturn(make(chan amqp.Return, 1))
	return ch, nil
}

// Publish sends m to the exchange with its topic as the routing key and
// waits for RabbitMQ to confirm it. A message no queue is bound to fails
// with CodeUnavailable instead of being dropped, so the outbox relay keeps
// it until a consumer has declared its queue. The ID is the AMQP message
// ID; RabbitMQ does not deduplicate on it, so make handlers idempotent.
func (b *Broker) Publish(ctx context.Context, m queue.Message) error {
	if err := b.check(); err != nil {
		return err
	}
	headers := amqp.Table{}
	for k, v := range m.Headers {
		headers[k] = v
	}
	if m.Key != "" {
		headers[keyHeader] = m.Key
	}
	msg := amqp.Publishing{
		Headers:      headers,
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    m.ID,
		Timestamp:    time.Now(),
		Body:         m.Payload,
	}

	// One publish at a time: RabbitMQ sends a return before the confirm of
	// the same message, so a return seen after the confirm is this one's.
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	ch, err := b.publishing()
	if err != nil {
		return err
	}
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, b.Exchange, m.Topic, true, false, msg)
	if err != nil {
		return err
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		// The confirm may still arrive; a fresh channel keeps it apart
		// from the next publish.
		_ = ch.Close()
		return err
	}
	select {
	case r := <-b.returns:
		return kakarot.E(kakarot.CodeUnavailable, "no queue is bound to topic "+m.Topic+": "+r.ReplyText, nil)
	default:
	}
	if !acked {
		return kakarot.E(kakarot.CodeUnavailable, "rabbitmq did not accept the message for topic "+m.Topic, nil)
	}
	return nil
}

// Subscribe declares the group's queue for topic, binds it to the exchange
// and delivers its messages to h until ctx is cancelled, then returns nil.
// When the connection drops it dials again and resumes after Backoff. A
// first setup that fails, such as a queue declared elsewhere with other
// arguments, returns the error.
func (b *Broker) Subscribe(ctx context.Context, topic string, h queue.Handler) error {
	if err := b.check(); err != nil {
		return err
	}
	if b.Group == "" {
		return errors.New("rabbitx: Broker.Group is empty")
	}
	started := false
	for {
		err := b.consume(ctx, topic, h, func() { started = true })
		if ctx.Err() != nil || b.isClosed() {
			return nil
		}
		if !started {
			return err
		}
		b.log().ErrorContext(ctx, "rabbitmq subscription interrupted, reconnecting", "topic", topic, "err", err)
		select { // do not spin on a server that is unreachable
		case <-ctx.Done():
			return nil
		case <-time.After(b.backoff()):
		}
	}
}

// consume runs one channel's subscription until ctx ends or the channel
// closes. ready is called once the queue is declared and consuming.
func (b *Broker) consume(ctx context.Context, topic string, h queue.Handler, ready func()) error {
	conn, err := b.connection()
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	var deliveries <-chan amqp.Delivery
	defer func() {
		// Closing returns unacknowledged messages to the queue. The client
		// waits for the server's reply while it may still be handing over
		// deliveries, so take them until the channel is gone.
		closed := make(chan struct{})
		go func() { _ = ch.Close(); close(closed) }()
		for {
			select {
			case _, ok := <-deliveries:
				if !ok {
					deliveries = nil // closed: stop selecting it
				}
			case <-closed:
				return
			}
		}
	}()

	prefetch, maxDeliver := b.Prefetch, b.MaxDeliver
	if prefetch <= 0 {
		prefetch = 10
	}
	if maxDeliver <= 0 {
		maxDeliver = 10
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	if err := b.declareExchange(ch); err != nil {
		return err
	}
	name := queueName(b.Group, topic)
	if _, err := ch.QueueDeclare(name, true, false, false, false, queueArgs(maxDeliver, b.DeadLetter)); err != nil {
		return fmt.Errorf("rabbitx: declare queue %s: %w", name, err)
	}
	if err := ch.QueueBind(name, topic, b.Exchange, false, nil); err != nil {
		return err
	}
	deliveries, err = ch.Consume(name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	ready()

	backoff := b.backoff()
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("rabbitx: channel closed")
			}
			m := message(d)
			if err := h(ctx, m); err != nil {
				if int(m.Attempts)+1 >= maxDeliver {
					// Rejected without requeue, RabbitMQ routes the message
					// to the queue's dead-letter exchange, or drops it.
					b.log().ErrorContext(ctx, "rabbitmq message failed too often, rejecting it", "topic", topic, "id", m.ID, "deliveries", m.Attempts+1, "dead_letter", b.DeadLetter, "err", err)
					_ = d.Nack(false, false)
					continue
				}
				// Hold the message for Backoff without blocking the next
				// one. If the channel closes first, RabbitMQ requeues it.
				time.AfterFunc(backoff, func() { _ = d.Nack(false, true) })
				continue
			}
			// A handler that finished is acknowledged even when ctx ended meanwhile.
			if err := d.Ack(false); err != nil {
				b.log().ErrorContext(ctx, "rabbitmq ack failed, the message will be delivered again", "topic", topic, "id", d.MessageId, "err", err)
			}
		}
	}
}

// Close closes the connection. Subscriptions still running return.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	if b.conn == nil || b.conn.IsClosed() {
		return nil
	}
	return b.conn.Close()
}

func (b *Broker) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// queueName names the queue a group consumes topic from.
func queueName(group, topic string) string {
	return group + "." + topic
}

// queueArgs makes a quorum queue, the RabbitMQ queue type that is
// replicated and counts deliveries, with a delivery limit and an optional
// dead-letter exchange.
func queueArgs(maxDeliver int, deadLetter string) amqp.Table {
	args := amqp.Table{"x-queue-type": "quorum", "x-delivery-limit": int64(maxDeliver)}
	if deadLetter != "" {
		args["x-dead-letter-exchange"] = deadLetter
	}
	return args
}

func message(d amqp.Delivery) queue.Message {
	m := queue.Message{ID: d.MessageId, Topic: d.RoutingKey, Payload: d.Body, Headers: map[string]string{}}
	for k, v := range d.Headers {
		switch {
		case k == "x-delivery-count" || k == "x-acquired-count":
			// RabbitMQ 4 counts every earlier delivery in x-acquired-count;
			// x-delivery-count counts only some of them.
			if n, ok := count(v); ok && n > int64(m.Attempts) {
				m.Attempts = int32(min(n, 1<<31-1))
			}
		case k == keyHeader:
			m.Key, _ = v.(string)
		default:
			if s, ok := v.(string); ok {
				m.Headers[k] = s
			}
		}
	}
	if m.Attempts == 0 && d.Redelivered {
		m.Attempts = 1 // delivered before, but the server did not say how often
	}
	return m
}

// count reads a header RabbitMQ sends as some integer type.
func count(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	}
	return 0, false
}

func (b *Broker) backoff() time.Duration {
	if b.Backoff > 0 {
		return b.Backoff
	}
	return time.Second
}

func (b *Broker) log() *slog.Logger {
	if b.Logger != nil {
		return b.Logger
	}
	return slog.Default()
}
