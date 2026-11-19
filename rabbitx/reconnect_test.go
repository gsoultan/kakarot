package rabbitx

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/gsoultan/kakarot/queue"
)

func TestLiveSubscriptionSurvivesADroppedConnection(t *testing.T) {
	url := os.Getenv("KAKAROT_TEST_AMQP")
	if url == "" {
		t.Skip("KAKAROT_TEST_AMQP is not set")
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	b := &Broker{URL: url, Exchange: "kakarot-test-" + suffix, Group: "billing-" + suffix, Backoff: 20 * time.Millisecond}
	t.Cleanup(func() {
		_ = b.Close()
		if conn, err := amqp.Dial(url); err == nil {
			defer conn.Close()
			if ch, err := conn.Channel(); err == nil {
				_, _ = ch.QueueDelete(queueName(b.Group, "order.placed"), false, false, false)
				_ = ch.ExchangeDelete(b.Exchange, false, false)
			}
		}
	})

	got := make(chan string, 10)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = b.Subscribe(ctx, "order.placed", func(_ context.Context, m queue.Message) error {
			got <- m.ID
			return nil
		})
	}()
	publish := func(id string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			err := b.Publish(t.Context(), queue.Message{ID: id, Topic: "order.placed", Payload: []byte(`{}`)})
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("publish %s: %v", id, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		select {
		case g := <-got:
			if g != id {
				t.Fatalf("got %s, want %s", g, id)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not arrive", id)
		}
	}

	publish("before")
	b.mu.Lock()
	_ = b.conn.Close() // as if the server went away
	b.mu.Unlock()
	publish("after")
}

func TestLiveSubscriptionEndsWhenTheBrokerCloses(t *testing.T) {
	url := os.Getenv("KAKAROT_TEST_AMQP")
	if url == "" {
		t.Skip("KAKAROT_TEST_AMQP is not set")
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	b := &Broker{URL: url, Exchange: "kakarot-test-" + suffix, Group: "billing-" + suffix, Backoff: 20 * time.Millisecond}
	t.Cleanup(func() {
		if conn, err := amqp.Dial(url); err == nil {
			defer conn.Close()
			if ch, err := conn.Channel(); err == nil {
				_, _ = ch.QueueDelete(queueName(b.Group, "order.placed"), false, false, false)
				_ = ch.ExchangeDelete(b.Exchange, false, false)
			}
		}
	})
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(t.Context(), "order.placed", func(context.Context, queue.Message) error { return nil })
	}()
	for {
		b.mu.Lock()
		up := b.conn != nil
		b.mu.Unlock()
		if up {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // let the subscription start consuming
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return after Close")
	}
}
