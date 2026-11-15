package sqsx_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/sqsx"
)

// fakeSQS keeps queues in memory with SQS's visibility rules: a received
// message is hidden until it is deleted or its timeout passes.
type fakeSQS struct {
	mu      sync.Mutex
	next    int
	queues  map[string][]*stored
	sent    []*sqs.SendMessageInput
	failing atomic.Bool
}

type stored struct {
	msg      types.Message
	hidden   time.Time
	received int
}

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.queues == nil {
		f.queues = map[string][]*stored{}
	}
	f.next++
	id := "m-" + strconv.Itoa(f.next)
	f.sent = append(f.sent, in)
	f.queues[*in.QueueUrl] = append(f.queues[*in.QueueUrl], &stored{msg: types.Message{
		MessageId: aws.String(id), ReceiptHandle: aws.String(id), Body: in.MessageBody, MessageAttributes: in.MessageAttributes,
	}})
	return &sqs.SendMessageOutput{MessageId: aws.String(id)}, nil
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if f.failing.Load() {
		return nil, errors.New("sqs is unreachable")
	}
	f.mu.Lock()
	out := &sqs.ReceiveMessageOutput{}
	for _, s := range f.queues[*in.QueueUrl] {
		if time.Now().Before(s.hidden) {
			continue
		}
		s.received++
		s.hidden = time.Now().Add(30 * time.Second)
		m := s.msg
		m.Attributes = map[string]string{"ApproximateReceiveCount": strconv.Itoa(s.received)}
		out.Messages = append(out.Messages, m)
	}
	f.mu.Unlock()
	if len(out.Messages) == 0 { // a short long-poll
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return out, nil
}

func (f *fakeSQS) find(url, handle string) (int, *stored) {
	for i, s := range f.queues[url] {
		if *s.msg.ReceiptHandle == handle {
			return i, s
		}
	}
	return -1, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i, s := f.find(*in.QueueUrl, *in.ReceiptHandle); s != nil {
		q := f.queues[*in.QueueUrl]
		f.queues[*in.QueueUrl] = append(q[:i], q[i+1:]...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, s := f.find(*in.QueueUrl, *in.ReceiptHandle); s != nil {
		s.hidden = time.Now().Add(time.Duration(in.VisibilityTimeout) * 20 * time.Millisecond) // test seconds are short
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) depth(url string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queues[url])
}

type placed struct {
	OrderID string `json:"order_id"`
}

const (
	orderPlaced queue.Topic[placed] = "order.placed"
	queueURL                        = "https://sqs.test/1/billing-order-placed"
)

type traceKey struct{}

func TestConsumerReceivesWhatWasPublishedAndDeletesIt(t *testing.T) {
	fake := &fakeSQS{}
	b := &sqsx.Broker{Client: fake, Queues: map[string]string{"order.placed": queueURL}}

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
	case <-time.After(5 * time.Second):
		t.Fatal("no message arrived")
	}
	kakarottest.Eventually(t, 5*time.Second, "the handled message is deleted", func() bool { return fake.depth(queueURL) == 0 })
}

func TestFailedMessageComesBackWithItsAttemptCount(t *testing.T) {
	fake := &fakeSQS{}
	b := &sqsx.Broker{Client: fake, Queues: map[string]string{"order.placed": queueURL}, Backoff: time.Second}
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-1", Topic: "order.placed", Key: "customer-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	second := make(chan queue.Message, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, "order.placed", func(_ context.Context, m queue.Message) error {
			if calls.Add(1) == 1 {
				return kakarot.E(kakarot.CodeUnavailable, "payment provider is down", errors.New("boom"))
			}
			second <- m
			return nil
		})
	}()
	select {
	case m := <-second:
		if m.Attempts != 1 || m.Key != "customer-1" || m.ID != "evt-1" {
			t.Fatalf("got %+v", m)
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

func TestFIFOQueueGroupsByKeyAndDeduplicatesByID(t *testing.T) {
	fake := &fakeSQS{}
	b := &sqsx.Broker{Client: fake, Queues: map[string]string{"order.placed": queueURL + ".fifo", "plain": queueURL}}
	for _, m := range []queue.Message{
		{ID: "evt-1", Topic: "order.placed", Key: "customer-1"},
		{Topic: "order.placed"},
		{ID: "evt-2", Topic: "plain", Key: "customer-1"},
	} {
		if err := b.Publish(t.Context(), m); err != nil {
			t.Fatal(err)
		}
	}
	keyed, bare, plain := fake.sent[0], fake.sent[1], fake.sent[2]
	if aws.ToString(keyed.MessageGroupId) != "customer-1" || aws.ToString(keyed.MessageDeduplicationId) != "evt-1" {
		t.Fatalf("keyed: %v %v", keyed.MessageGroupId, keyed.MessageDeduplicationId)
	}
	if aws.ToString(bare.MessageGroupId) != "order.placed" || bare.MessageDeduplicationId != nil {
		t.Fatalf("bare: %v %v", bare.MessageGroupId, bare.MessageDeduplicationId)
	}
	if plain.MessageGroupId != nil || plain.MessageDeduplicationId != nil {
		t.Fatal("a standard queue takes no group or deduplication id")
	}
}

func TestSubscribeSurvivesAnUnreachableQueue(t *testing.T) {
	fake := &fakeSQS{}
	fake.failing.Store(true)
	b := &sqsx.Broker{Client: fake, Queues: map[string]string{"order.placed": queueURL}, Backoff: 10 * time.Millisecond}
	got := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go b.Subscribe(ctx, "order.placed", func(context.Context, queue.Message) error {
		got <- struct{}{}
		return nil
	})
	if err := b.Publish(t.Context(), queue.Message{Topic: "order.placed", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	fake.failing.Store(false)
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer did not recover")
	}
}

func TestBrokerReportsMissingSettings(t *testing.T) {
	if err := (&sqsx.Broker{}).Publish(t.Context(), queue.Message{Topic: "x"}); err == nil {
		t.Fatal("want an error without a client")
	}
	b := &sqsx.Broker{Client: &fakeSQS{}}
	if err := b.Subscribe(t.Context(), "x", nil); err == nil {
		t.Fatal("want an error for a topic with no queue")
	}
}
