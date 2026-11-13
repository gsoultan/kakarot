package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/queue"
)

type placed struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

var orderPlaced = queue.Topic[placed]("order.placed")

func start(t *testing.T, c *queue.Consumer, b *queue.Memory, topic string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Serve(context.Background()) }()
	for b.Subscribers(topic) == 0 {
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() {
		if err := c.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

func TestConsumeTypedWithMiddleware(t *testing.T) {
	b := &queue.Memory{}
	var got []placed
	var ops []string
	spy := func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		ops = append(ops, op.String()+"@"+op.Transport)
		return next(ctx)
	}
	c := &queue.Consumer{Service: "billing", Source: b, Middleware: []kakarot.Middleware{spy}}
	queue.Handle(c, orderPlaced, "Charge", func(_ context.Context, p placed) (struct{}, error) {
		got = append(got, p)
		return struct{}{}, nil
	})
	start(t, c, b, "order.placed")

	m, err := orderPlaced.Encode("o-1", placed{ID: "o-1", Total: 7})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Total != 7 || len(ops) != 1 || ops[0] != "billing.Charge@queue" {
		t.Fatalf("got=%+v ops=%v", got, ops)
	}
}

func TestRetryableErrorIsRedeliveredOthersDeadLettered(t *testing.T) {
	b := &queue.Memory{}
	var dead []string
	var result error
	c := &queue.Consumer{Service: "billing", Source: b,
		DeadLetter: func(_ context.Context, m queue.Message, err error) { dead = append(dead, m.ID+":"+kakarot.CodeOf(err).String()) }}
	queue.Handle(c, orderPlaced, "Charge", func(context.Context, placed) (struct{}, error) { return struct{}{}, result })
	start(t, c, b, "order.placed")

	msg := func(id string) queue.Message {
		m, _ := orderPlaced.Encode(id, placed{ID: id})
		m.ID = id
		return m
	}
	result = errors.New("database is down") // internal: retry
	if err := b.Publish(t.Context(), msg("a")); err == nil {
		t.Fatal("retryable failure must be returned to the broker")
	}
	result = kakarot.NotFound("order", "b") // a retry cannot fix it
	if err := b.Publish(t.Context(), msg("b")); err != nil {
		t.Fatalf("non-retryable failure must be acknowledged: %v", err)
	}
	bad := queue.Message{ID: "c", Topic: "order.placed", Payload: []byte("{not json")}
	if err := b.Publish(t.Context(), bad); err != nil {
		t.Fatalf("undecodable payload must be acknowledged: %v", err)
	}
	if len(dead) != 2 || dead[0] != "b:not_found" || dead[1] != "c:invalid" {
		t.Fatalf("dead = %v", dead)
	}
}

func TestTopicMismatch(t *testing.T) {
	if _, err := orderPlaced.Decode(queue.Message{Topic: "other", Payload: []byte("{}")}); kakarot.CodeOf(err) != kakarot.CodeInvalid {
		t.Fatal(err)
	}
}

type failing struct{}

func (failing) Subscribe(context.Context, string, queue.Handler) error {
	return errors.New("broker unreachable")
}

func TestServeReturnsSubscriptionError(t *testing.T) {
	c := &queue.Consumer{Source: failing{}}
	queue.Handle(c, orderPlaced, "Charge", func(context.Context, placed) (struct{}, error) { return struct{}{}, nil })
	if err := c.Serve(t.Context()); err == nil || err.Error() != "broker unreachable" {
		t.Fatalf("err = %v", err)
	}
}
