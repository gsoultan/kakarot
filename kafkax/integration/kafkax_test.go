package integration_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kafkax"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/queue"
)

// cluster starts an in-memory Kafka with the given topics.
func cluster(t *testing.T, topics ...string) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topics...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func broker(t *testing.T, addrs []string, group string) *kafkax.Broker {
	b := &kafkax.Broker{Brokers: addrs, Group: group, Backoff: 10 * time.Millisecond}
	t.Cleanup(b.Close)
	return b
}

type placed struct {
	OrderID string `json:"order_id"`
}

const orderPlaced queue.Topic[placed] = "order.placed"

type traceKey struct{}

func TestConsumerReceivesWhatWasPublished(t *testing.T) {
	b := broker(t, cluster(t, "order.placed"), "billing")

	type seen struct {
		in    placed
		trace string
	}
	got := make(chan seen, 1)
	c := &queue.Consumer{Service: "billing", Source: b,
		Extract: func(ctx context.Context, h map[string]string) context.Context {
			return context.WithValue(ctx, traceKey{}, h["traceparent"])
		}}
	queue.Handle(c, orderPlaced, "Charge", func(ctx context.Context, in placed) (kakarottest.None, error) {
		trace, _ := ctx.Value(traceKey{}).(string)
		got <- seen{in, trace}
		return kakarottest.None{}, nil
	})
	kakarottest.Serve(t, c)

	m, err := orderPlaced.Encode("customer-1", placed{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	m.ID, m.Headers = "evt-1", map[string]string{"traceparent": "00-abc-def-01"}
	if err := b.Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s.in.OrderID != "o-1" || s.trace != "00-abc-def-01" {
			t.Fatalf("got %+v", s)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("no message arrived")
	}
}

func TestFailedMessageIsTriedAgainThenCommitted(t *testing.T) {
	addrs := cluster(t, "order.placed")
	b := broker(t, addrs, "billing")
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-1", Topic: "order.placed", Key: "customer-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	second := make(chan queue.Message, 1)
	run := func(ctx context.Context, h queue.Handler) chan error {
		done := make(chan error, 1)
		go func() { done <- b.Subscribe(ctx, "order.placed", h) }()
		return done
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, func(_ context.Context, m queue.Message) error {
		if calls.Add(1) == 1 {
			return kakarot.E(kakarot.CodeUnavailable, "payment provider is down", errors.New("boom"))
		}
		second <- m
		return nil
	})
	select {
	case m := <-second:
		if m.Attempts != 1 || m.Key != "customer-1" || m.ID != "evt-1" {
			t.Fatalf("got %+v", m)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the message was not tried again")
	}
	time.Sleep(200 * time.Millisecond) // let the offset commit land
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// The same group, restarted, does not see the handled message again.
	ctx2, cancel2 := context.WithCancel(t.Context())
	done2 := run(ctx2, func(context.Context, queue.Message) error {
		calls.Add(1)
		return nil
	})
	time.Sleep(time.Second)
	cancel2()
	if err := <-done2; err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("handler ran %d times, want 2", n)
	}
}

func TestMessageGoesToTheDeadLetterTopicAfterMaxDeliver(t *testing.T) {
	addrs := cluster(t, "order.placed", "order.placed.dead")
	b := broker(t, addrs, "billing")
	b.MaxDeliver, b.DeadLetter = 3, ".dead"

	var calls atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go b.Subscribe(ctx, "order.placed", func(context.Context, queue.Message) error {
		calls.Add(1)
		return errors.New("always fails")
	})
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-9", Topic: "order.placed", Payload: []byte(`{"order_id":"o-9"}`)}); err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.ConsumeTopics("order.placed.dead"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pollCtx, stop := context.WithTimeout(t.Context(), 15*time.Second)
	defer stop()
	recs := cl.PollFetches(pollCtx).Records()
	if len(recs) != 1 || string(recs[0].Value) != `{"order_id":"o-9"}` {
		t.Fatalf("dead letter topic holds %d records", len(recs))
	}
	headers := map[string]string{}
	for _, h := range recs[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["Kakarot-Id"] != "evt-9" || headers["Kakarot-Error"] != "always fails" {
		t.Fatalf("headers: %v", headers)
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("handler ran %d times, want 3", n)
	}
}

func TestGroupsEachReceiveEveryMessage(t *testing.T) {
	addrs := cluster(t, "order.placed")
	var billing, mail atomic.Int32
	for group, n := range map[string]*atomic.Int32{"billing": &billing, "mail": &mail} {
		c := &queue.Consumer{Service: group, Source: broker(t, addrs, group)}
		queue.Handle(c, orderPlaced, "On", func(context.Context, placed) (kakarottest.None, error) {
			n.Add(1)
			return kakarottest.None{}, nil
		})
		kakarottest.Serve(t, c)
	}
	m, _ := orderPlaced.Encode("k", placed{OrderID: "o-1"})
	if err := broker(t, addrs, "").Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	kakarottest.Eventually(t, 15*time.Second, "both groups handled the message", func() bool {
		return billing.Load() == 1 && mail.Load() == 1
	})
}
