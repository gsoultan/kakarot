package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/sqsx"
)

// env is the SQS endpoint a test runs against.
type env struct {
	t   *testing.T
	sqs *sqs.Client
	run string // makes queue names unique
}

func connect(t *testing.T) *env {
	t.Helper()
	target := os.Getenv("KAKAROT_TEST_SQS")
	if target == "" {
		t.Skip("KAKAROT_TEST_SQS is not set: set it to an endpoint URL (LocalStack) or to \"aws\"")
	}
	ctx := t.Context()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var opts []func(*sqs.Options)
	if target != "aws" {
		if cfg.Region == "" {
			cfg.Region = "us-east-1"
		}
		cfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
		opts = append(opts, func(o *sqs.Options) { o.BaseEndpoint = aws.String(target) })
	}
	return &env{t: t, sqs: sqs.NewFromConfig(cfg, opts...), run: strconv.FormatInt(time.Now().UnixNano(), 36)}
}

// queue creates a queue for the test and deletes it when the test ends.
func (e *env) queue(name string, attrs map[string]string) string {
	e.t.Helper()
	fifo := len(name) > 5 && name[len(name)-5:] == ".fifo"
	if fifo {
		name = name[:len(name)-5]
	}
	name = "kakarot-" + name + "-" + e.run
	if fifo {
		name += ".fifo"
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs[string(types.QueueAttributeNameFifoQueue)] = "true"
	}
	out, err := e.sqs.CreateQueue(e.t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	if err != nil {
		e.t.Fatalf("create queue %s: %v", name, err)
	}
	url := aws.ToString(out.QueueUrl)
	e.t.Cleanup(func() {
		e.sqs.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(url)})
	})
	return url
}

func (e *env) arn(url string) string {
	e.t.Helper()
	out, err := e.sqs.GetQueueAttributes(e.t.Context(), &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		e.t.Fatal(err)
	}
	return out.Attributes[string(types.QueueAttributeNameQueueArn)]
}

// depth is how many messages the queue holds, visible or in flight.
func (e *env) depth(url string) int {
	e.t.Helper()
	out, err := e.sqs.GetQueueAttributes(e.t.Context(), &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{
		types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
	}})
	if err != nil {
		e.t.Fatal(err)
	}
	visible, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	hidden, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return visible + hidden
}

// subscribe runs b.Subscribe until the test ends.
func subscribe(t *testing.T, b *sqsx.Broker, topic string, h queue.Handler) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Subscribe(ctx, topic, h) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(30 * time.Second):
			t.Error("Subscribe did not return after its context ended")
		}
	})
}

type placed struct {
	OrderID string `json:"order_id"`
}

const orderPlaced queue.Topic[placed] = "order.placed"

type traceKey struct{}

func TestConsumerReceivesWhatWasPublishedAndDeletesIt(t *testing.T) {
	e := connect(t)
	url := e.queue("billing-order-placed", nil)
	b := &sqsx.Broker{Client: e.sqs, Queues: map[string]string{"order.placed": url}, Wait: time.Second}

	type seen struct {
		in    placed
		key   string
		id    string
		trace string
	}
	got := make(chan seen, 1)
	c := &queue.Consumer{Service: "billing", Source: b,
		Extract: func(ctx context.Context, h map[string]string) context.Context {
			return context.WithValue(ctx, traceKey{}, h["traceparent"])
		}}
	queue.Handle(c, orderPlaced, "Charge", func(ctx context.Context, in placed) (kakarottest.None, error) {
		trace, _ := ctx.Value(traceKey{}).(string)
		got <- seen{in: in, trace: trace}
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
	case <-time.After(30 * time.Second):
		t.Fatal("no message arrived")
	}
	kakarottest.Eventually(t, 30*time.Second, "the handled message is deleted", func() bool { return e.depth(url) == 0 })
}

func TestMessageKeepsItsIDKeyAndHeaders(t *testing.T) {
	e := connect(t)
	url := e.queue("billing-order-placed", nil)
	b := &sqsx.Broker{Client: e.sqs, Queues: map[string]string{"order.placed": url}, Wait: time.Second}
	got := make(chan queue.Message, 1)
	subscribe(t, b, "order.placed", func(_ context.Context, m queue.Message) error {
		got <- m
		return nil
	})
	in := queue.Message{ID: "evt-1", Topic: "order.placed", Key: "customer-1", Payload: []byte(`{"order_id":"o-1"}`),
		Headers: map[string]string{"traceparent": "00-abc-def-01", "tracestate": "k=v"}}
	if err := b.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m.ID != "evt-1" || m.Key != "customer-1" || m.Topic != "order.placed" || string(m.Payload) != `{"order_id":"o-1"}` || m.Attempts != 0 ||
			len(m.Headers) != 2 || m.Headers["traceparent"] != "00-abc-def-01" || m.Headers["tracestate"] != "k=v" {
			t.Fatalf("got %+v", m)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no message arrived")
	}
}

func TestFailedMessageComesBackWithItsAttemptCount(t *testing.T) {
	e := connect(t)
	url := e.queue("billing-order-placed", nil)
	b := &sqsx.Broker{Client: e.sqs, Queues: map[string]string{"order.placed": url}, Backoff: time.Second, Wait: time.Second}
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-1", Topic: "order.placed", Key: "customer-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	second := make(chan queue.Message, 1)
	subscribe(t, b, "order.placed", func(_ context.Context, m queue.Message) error {
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
	case <-time.After(30 * time.Second):
		t.Fatal("the message was not delivered again")
	}
	kakarottest.Eventually(t, 30*time.Second, "the handled message is deleted", func() bool { return e.depth(url) == 0 })
}

func TestMessageMovesToTheDeadLetterQueueOfTheRedrivePolicy(t *testing.T) {
	e := connect(t)
	dead := e.queue("billing-order-placed-dead", nil)
	policy, _ := json.Marshal(map[string]string{"deadLetterTargetArn": e.arn(dead), "maxReceiveCount": "2"})
	url := e.queue("billing-order-placed", map[string]string{string(types.QueueAttributeNameRedrivePolicy): string(policy)})
	b := &sqsx.Broker{Client: e.sqs, Queues: map[string]string{"order.placed": url}, Backoff: time.Second, Wait: time.Second}

	var calls atomic.Int32
	subscribe(t, b, "order.placed", func(context.Context, queue.Message) error {
		calls.Add(1)
		return errors.New("always fails")
	})
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-9", Topic: "order.placed", Payload: []byte(`{"order_id":"o-9"}`)}); err != nil {
		t.Fatal(err)
	}
	kakarottest.Eventually(t, 60*time.Second, "the message is in the dead-letter queue", func() bool { return e.depth(dead) == 1 })
	if n := calls.Load(); n != 2 {
		t.Fatalf("handler ran %d times, want 2", n)
	}
}

func TestFIFOQueueKeepsOneKeysOrderAndDeduplicatesByID(t *testing.T) {
	e := connect(t)
	url := e.queue("billing-order-placed.fifo", nil)
	b := &sqsx.Broker{Client: e.sqs, Queues: map[string]string{"order.placed": url}, Wait: time.Second}
	const n = 20
	for i := range n {
		m := queue.Message{ID: "evt-" + strconv.Itoa(i), Topic: "order.placed", Key: "customer-" + strconv.Itoa(i%2), Payload: []byte(strconv.Itoa(i))}
		if err := b.Publish(t.Context(), m); err != nil {
			t.Fatal(err)
		}
	}
	// The outbox may publish an event twice; the queue keeps one copy.
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-0", Topic: "order.placed", Key: "customer-0", Payload: []byte("0")}); err != nil {
		t.Fatal(err)
	}
	got := make(chan queue.Message, 2*n)
	subscribe(t, b, "order.placed", func(_ context.Context, m queue.Message) error {
		got <- m
		return nil
	})
	last := map[string]int{"customer-0": -1, "customer-1": -1}
	for range n {
		select {
		case m := <-got:
			i, _ := strconv.Atoi(string(m.Payload))
			if i <= last[m.Key] {
				t.Fatalf("key %s: message %d after %d", m.Key, i, last[m.Key])
			}
			last[m.Key] = i
		case <-time.After(30 * time.Second):
			t.Fatal("not every message arrived")
		}
	}
	select {
	case m := <-got:
		t.Fatalf("a message arrived twice: %+v", m)
	case <-time.After(3 * time.Second):
	}
}

func TestFIFOQueueTakesAMessageWithoutIDOrKey(t *testing.T) {
	e := connect(t)
	url := e.queue("billing-order-placed.fifo", nil)
	b := &sqsx.Broker{Client: e.sqs, Queues: map[string]string{"order.placed": url}, Wait: time.Second}
	// Without an ID the payload deduplicates: the repeated one is dropped.
	for _, p := range []string{`{"n":1}`, `{"n":2}`, `{"n":1}`} {
		if err := b.Publish(t.Context(), queue.Message{Topic: "order.placed", Payload: []byte(p)}); err != nil {
			t.Fatal(err)
		}
	}
	kakarottest.Eventually(t, 30*time.Second, "two messages are queued", func() bool { return e.depth(url) == 2 })
	time.Sleep(time.Second)
	if n := e.depth(url); n != 2 {
		t.Fatalf("the queue holds %d messages, want 2", n)
	}
}

func TestPayloadThatIsNotTextArrivesUnchanged(t *testing.T) {
	e := connect(t)
	url := e.queue("billing-order-placed", nil)
	b := &sqsx.Broker{Client: e.sqs, Queues: map[string]string{"order.placed": url}, Wait: time.Second}
	got := make(chan queue.Message, 1)
	subscribe(t, b, "order.placed", func(_ context.Context, m queue.Message) error {
		got <- m
		return nil
	})
	payload := []byte{0x0a, 0x03, 'o', '-', '1', 0x00, 0xff, 0xfe, 0x10}
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-1", Topic: "order.placed", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if string(m.Payload) != string(payload) {
			t.Fatalf("payload %x, want %x", m.Payload, payload)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no message arrived")
	}
}
