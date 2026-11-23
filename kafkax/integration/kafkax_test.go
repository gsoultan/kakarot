package integration_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kafkax"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/queue"
)

// kafka is the cluster a test runs against: a real one when
// KAKAROT_TEST_KAFKA names its seed brokers (host:port, comma separated),
// otherwise an in-memory cluster started inside the test.
type kafka struct {
	t     *testing.T
	addrs []string
	admin *kadm.Client
	run   string // makes topic and group names unique on a real cluster
}

func cluster(t *testing.T) *kafka {
	t.Helper()
	k := &kafka{t: t}
	if seeds := os.Getenv("KAKAROT_TEST_KAFKA"); seeds != "" {
		k.addrs = strings.Split(seeds, ",")
		k.run = "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	} else {
		c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.AllowAutoTopicCreation()) // as a Kafka broker does by default
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		k.addrs = c.ListenAddrs()
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(k.addrs...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	k.admin = kadm.NewClient(cl)
	return k
}

// topic creates a topic with the given number of partitions and returns
// its name, which on a real cluster carries a suffix unique to the test.
func (k *kafka) topic(name string, partitions int32) string {
	k.t.Helper()
	return k.create(name+k.run, partitions)
}

// create creates the topic name as it is.
func (k *kafka) create(name string, partitions int32) string {
	k.t.Helper()
	res, err := k.admin.CreateTopic(k.t.Context(), partitions, 1, nil, name)
	if err == nil {
		err = res.Err
	}
	if err != nil {
		k.t.Fatalf("create topic %s: %v", name, err)
	}
	if k.run != "" {
		k.t.Cleanup(func() { k.admin.DeleteTopics(context.Background(), name) })
	}
	return name
}

// group returns a consumer group name unique to the test.
func (k *kafka) group(name string) string { return name + k.run }

func (k *kafka) broker(group string) *kafkax.Broker {
	b := &kafkax.Broker{Brokers: k.addrs, Group: group, Backoff: 10 * time.Millisecond}
	k.t.Cleanup(b.Close)
	return b
}

type placed struct {
	OrderID string `json:"order_id"`
}

type traceKey struct{}

func TestConsumerReceivesWhatWasPublished(t *testing.T) {
	k := cluster(t)
	orderPlaced := queue.Topic[placed](k.topic("order.placed", 3))
	b := k.broker(k.group("billing"))

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
	case <-time.After(30 * time.Second):
		t.Fatal("no message arrived")
	}
}

func TestMessagesWithOneKeyArriveInOrder(t *testing.T) {
	k := cluster(t)
	topic := k.topic("order.placed", 3)
	b := k.broker(k.group("billing"))
	const n = 50
	for i := range n {
		key := "customer-" + strconv.Itoa(i%3)
		if err := b.Publish(t.Context(), queue.Message{Topic: topic, Key: key, Payload: []byte(strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	last := map[string]int{}
	seen := 0
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go b.Subscribe(ctx, topic, func(_ context.Context, m queue.Message) error {
		i, _ := strconv.Atoi(string(m.Payload))
		mu.Lock()
		defer mu.Unlock()
		if prev, ok := last[m.Key]; ok && prev >= i {
			t.Errorf("key %s: message %d after %d", m.Key, i, prev)
		}
		last[m.Key] = i
		seen++
		return nil
	})
	kakarottest.Eventually(t, 30*time.Second, "all messages handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen == n
	})
}

func TestFailedMessageIsTriedAgainThenCommitted(t *testing.T) {
	k := cluster(t)
	topic := k.topic("order.placed", 1)
	b := k.broker(k.group("billing"))
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-1", Topic: topic, Key: "customer-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	handled := make(chan queue.Message, 2)
	run := func(ctx context.Context, h queue.Handler) chan error {
		done := make(chan error, 1)
		go func() { done <- b.Subscribe(ctx, topic, h) }()
		return done
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, func(_ context.Context, m queue.Message) error {
		if calls.Add(1) == 1 {
			return kakarot.E(kakarot.CodeUnavailable, "payment provider is down", errors.New("boom"))
		}
		handled <- m
		return nil
	})
	select {
	case m := <-handled:
		if m.Attempts != 1 || m.Key != "customer-1" || m.ID != "evt-1" {
			t.Fatalf("got %+v", m)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the message was not tried again")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// The same group, restarted, continues after the handled message: it
	// receives the next one and not the first again.
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-2", Topic: topic, Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(t.Context())
	done2 := run(ctx2, func(_ context.Context, m queue.Message) error {
		handled <- m
		return nil
	})
	select {
	case m := <-handled:
		if m.ID != "evt-2" {
			t.Fatalf("after a restart the group received %s, want evt-2", m.ID)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the restarted group received nothing")
	}
	cancel2()
	if err := <-done2; err != nil {
		t.Fatal(err)
	}
}

func TestMessageGoesToTheDeadLetterTopicAfterMaxDeliver(t *testing.T) {
	k := cluster(t)
	topic := k.topic("order.placed", 1)
	k.create(topic+".dead", 1)
	b := k.broker(k.group("billing"))
	b.MaxDeliver, b.DeadLetter = 3, ".dead"

	var calls atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go b.Subscribe(ctx, topic, func(context.Context, queue.Message) error {
		calls.Add(1)
		return errors.New("always fails")
	})
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-9", Topic: topic, Payload: []byte(`{"order_id":"o-9"}`)}); err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(k.addrs...), kgo.ConsumeTopics(topic+".dead"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pollCtx, stop := context.WithTimeout(t.Context(), 30*time.Second)
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

// On a cluster that creates topics on first use, which is Kafka's default,
// neither the topic nor its dead-letter topic has to exist beforehand.
func TestMissingTopicsAreCreatedOnFirstUse(t *testing.T) {
	k := cluster(t)
	topic := "order.placed.auto" + k.run
	if k.run != "" {
		t.Cleanup(func() { k.admin.DeleteTopics(context.Background(), topic, topic+".dead") })
	}
	b := k.broker(k.group("billing"))
	b.MaxDeliver, b.DeadLetter = 2, ".dead"
	if err := b.Publish(t.Context(), queue.Message{ID: "evt-1", Topic: topic, Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go b.Subscribe(ctx, topic, func(context.Context, queue.Message) error { return errors.New("always fails") })

	cl, err := kgo.NewClient(kgo.SeedBrokers(k.addrs...), kgo.ConsumeTopics(topic+".dead"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pollCtx, stop := context.WithTimeout(t.Context(), 30*time.Second)
	defer stop()
	if recs := cl.PollFetches(pollCtx).Records(); len(recs) != 1 {
		t.Fatalf("dead letter topic holds %d records, want 1", len(recs))
	}
}

func TestGroupsEachReceiveEveryMessage(t *testing.T) {
	k := cluster(t)
	orderPlaced := queue.Topic[placed](k.topic("order.placed", 3))
	var billing, mail atomic.Int32
	for group, n := range map[string]*atomic.Int32{"billing": &billing, "mail": &mail} {
		c := &queue.Consumer{Service: group, Source: k.broker(k.group(group))}
		queue.Handle(c, orderPlaced, "On", func(context.Context, placed) (kakarottest.None, error) {
			n.Add(1)
			return kakarottest.None{}, nil
		})
		kakarottest.Serve(t, c)
	}
	m, _ := orderPlaced.Encode("k", placed{OrderID: "o-1"})
	if err := k.broker("").Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	kakarottest.Eventually(t, 30*time.Second, "both groups handled the message", func() bool {
		return billing.Load() == 1 && mail.Load() == 1
	})
}
