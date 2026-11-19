package rabbitx_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/rabbitx"
)

type placed struct {
	OrderID string `json:"order_id"`
}

const orderPlaced queue.Topic[placed] = "order.placed"

var exchanges atomic.Int32

// live returns a broker on a fresh exchange of the server at
// KAKAROT_TEST_AMQP, and deletes what the test declared afterwards.
func live(t *testing.T, group string) *rabbitx.Broker {
	t.Helper()
	url := os.Getenv("KAKAROT_TEST_AMQP")
	if url == "" {
		t.Skip("KAKAROT_TEST_AMQP is not set")
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.Itoa(int(exchanges.Add(1)))
	b := &rabbitx.Broker{URL: url, Exchange: "kakarot-test-" + suffix, Group: group + "-" + suffix, Backoff: 20 * time.Millisecond}
	t.Cleanup(func() {
		_ = b.Close()
		conn, err := amqp.Dial(url)
		if err != nil {
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		_, _ = ch.QueueDelete(b.Group+"."+string(orderPlaced), false, false, false)
		_ = ch.ExchangeDelete(b.Exchange, false, false)
	})
	return b
}

func TestLiveConsumerReceivesWhatWasPublished(t *testing.T) {
	broker := live(t, "billing")

	type seen struct {
		in          placed
		id, key, tp string
	}
	got := make(chan seen, 2)
	c := &queue.Consumer{Service: "billing", Source: broker}
	queue.Handle(c, orderPlaced, "Charge", func(ctx context.Context, in placed) (kakarottest.None, error) {
		got <- seen{in: in}
		return kakarottest.None{}, nil
	})
	var raw atomic.Pointer[queue.Message]
	c.Extract = func(ctx context.Context, h map[string]string) context.Context {
		m := queue.Message{Headers: h}
		raw.Store(&m)
		return ctx
	}
	kakarottest.Serve(t, c)

	m, err := orderPlaced.Encode("customer-1", placed{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	m.ID, m.Headers = "evt-1", map[string]string{"traceparent": "00-abc-def-01"}
	kakarottest.Eventually(t, 5*time.Second, "the consumer's queue is bound", func() bool {
		return broker.Publish(t.Context(), m) == nil
	})
	select {
	case s := <-got:
		if s.in.OrderID != "o-1" {
			t.Fatalf("got %+v", s)
		}
		if h := raw.Load(); h == nil || h.Headers["traceparent"] != "00-abc-def-01" {
			t.Fatalf("headers = %+v", h)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message arrived")
	}
}

func TestLivePublishWithoutAQueueFails(t *testing.T) {
	broker := live(t, "nobody")
	err := broker.Publish(t.Context(), queue.Message{Topic: "order.placed", Payload: []byte(`{}`)})
	if kakarot.CodeOf(err) != kakarot.CodeUnavailable {
		t.Fatalf("err = %v, want unavailable", err)
	}
}

func TestLiveFailedMessageIsDeliveredAgain(t *testing.T) {
	broker := live(t, "billing")

	var calls atomic.Int32
	type seen struct {
		id, key  string
		attempts int32
	}
	got := make(chan seen, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- broker.Subscribe(ctx, "order.placed", func(_ context.Context, m queue.Message) error {
			if calls.Add(1) == 1 {
				return kakarot.E(kakarot.CodeUnavailable, "payment provider is down", errors.New("boom"))
			}
			got <- seen{m.ID, m.Key, m.Attempts}
			return nil
		})
	}()
	kakarottest.Eventually(t, 5*time.Second, "the queue is bound", func() bool {
		return broker.Publish(t.Context(), queue.Message{ID: "evt-7", Topic: "order.placed", Key: "customer-1", Payload: []byte(`{}`)}) == nil
	})
	select {
	case s := <-got:
		if s.attempts != 1 || s.key != "customer-1" || s.id != "evt-7" {
			t.Fatalf("got %+v, want attempt 1 of evt-7 for customer-1", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the message was not delivered again")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return after cancel")
	}
}

func TestLiveGroupsEachReceiveEveryMessage(t *testing.T) {
	billing, mail := live(t, "billing"), live(t, "mail")
	mail.Exchange = billing.Exchange
	t.Cleanup(func() { // live deletes only the billing queue on the shared exchange
		conn, err := amqp.Dial(os.Getenv("KAKAROT_TEST_AMQP"))
		if err == nil {
			defer conn.Close()
			if ch, err := conn.Channel(); err == nil {
				_, _ = ch.QueueDelete(mail.Group+"."+string(orderPlaced), false, false, false)
			}
		}
	})
	var nb, nm atomic.Int32
	for b, n := range map[*rabbitx.Broker]*atomic.Int32{billing: &nb, mail: &nm} {
		c := &queue.Consumer{Service: b.Group, Source: b}
		queue.Handle(c, orderPlaced, "On", func(context.Context, placed) (kakarottest.None, error) {
			n.Add(1)
			return kakarottest.None{}, nil
		})
		kakarottest.Serve(t, c)
	}
	m, _ := orderPlaced.Encode("k", placed{OrderID: "o-1"})
	kakarottest.Eventually(t, 5*time.Second, "both groups handled the message", func() bool {
		if nb.Load() == 0 || nm.Load() == 0 {
			_ = billing.Publish(t.Context(), m) // until both queues are bound
		}
		return nb.Load() >= 1 && nm.Load() >= 1
	})
}

func TestLiveMessageThatKeepsFailingIsDeadLettered(t *testing.T) {
	broker := live(t, "billing")
	broker.MaxDeliver, broker.DeadLetter = 2, broker.Exchange+".dead"

	conn, err := amqp.Dial(os.Getenv("KAKAROT_TEST_AMQP"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.ExchangeDeclare(broker.DeadLetter, amqp.ExchangeFanout, false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}
	dq, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind(dq.Name, "", broker.DeadLetter, false, nil); err != nil {
		t.Fatal(err)
	}
	dead, err := ch.Consume(dq.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = broker.Subscribe(ctx, "order.placed", func(context.Context, queue.Message) error {
			calls.Add(1)
			return errors.New("always fails")
		})
	}()
	kakarottest.Eventually(t, 5*time.Second, "the queue is bound", func() bool {
		return broker.Publish(t.Context(), queue.Message{ID: "evt-9", Topic: "order.placed", Payload: []byte(`{}`)}) == nil
	})
	select {
	case d := <-dead:
		if d.MessageId != "evt-9" || d.RoutingKey != "order.placed" {
			t.Fatalf("dead-lettered %q with key %q", d.MessageId, d.RoutingKey)
		}
		if n := calls.Load(); n != 2 {
			t.Fatalf("handled %d times, want MaxDeliver = 2", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("not dead-lettered after %d calls", calls.Load())
	}
}
