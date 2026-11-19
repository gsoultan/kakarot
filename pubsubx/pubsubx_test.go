package pubsubx_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/pubsubx"
	"github.com/gsoultan/kakarot/queue"
)

// connect starts an in-memory Pub/Sub server inside the test.
func connect(t *testing.T) (*pubsub.Client, *pstest.Server) {
	t.Helper()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := pubsub.NewClient(t.Context(), "kakarot", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, srv
}

type placed struct {
	OrderID string `json:"order_id"`
}

const orderPlaced queue.Topic[placed] = "order.placed"

func TestConsumerReceivesWhatWasPublished(t *testing.T) {
	client, _ := connect(t)
	broker := &pubsubx.Broker{Client: client, Group: "billing", Create: true}
	t.Cleanup(func() { _ = broker.Close() })

	type seen struct {
		in    placed
		trace string
	}
	got := make(chan seen, 1)
	type traceKey struct{}
	c := &queue.Consumer{Service: "billing", Source: broker,
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
	// The subscription exists once the consumer started; publish until then.
	kakarottest.Eventually(t, 5*time.Second, "the message arrived", func() bool {
		if err := broker.Publish(t.Context(), m); err != nil {
			t.Fatal(err)
		}
		select {
		case s := <-got:
			if s.in.OrderID != "o-1" || s.trace != "00-abc-def-01" {
				t.Fatalf("got %+v", s)
			}
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	})
}

// subscribe runs broker.Subscribe until the test ends and returns once the
// subscription exists.
func subscribe(t *testing.T, client *pubsub.Client, broker *pubsubx.Broker, h queue.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- broker.Subscribe(ctx, "order.placed", h) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Subscribe did not return after cancel")
		}
	})
	name := "projects/kakarot/subscriptions/" + broker.Group + ".order.placed"
	kakarottest.Eventually(t, 5*time.Second, "the subscription exists", func() bool {
		_, err := client.SubscriptionAdminClient.GetSubscription(t.Context(), &pubsubpb.GetSubscriptionRequest{Subscription: name})
		return err == nil
	})
}

func TestFailedMessageIsDeliveredAgain(t *testing.T) {
	client, _ := connect(t)
	broker := &pubsubx.Broker{Client: client, Group: "billing", Create: true, DeadLetter: ".dead", Backoff: time.Millisecond}

	var calls atomic.Int32
	type seen struct {
		id, key  string
		attempts int32
	}
	got := make(chan seen, 1)
	subscribe(t, client, broker, func(_ context.Context, m queue.Message) error {
		if calls.Add(1) == 1 {
			return kakarot.E(kakarot.CodeUnavailable, "payment provider is down", errors.New("boom"))
		}
		got <- seen{m.ID, m.Key, m.Attempts}
		return nil
	})
	if err := broker.Publish(t.Context(), queue.Message{ID: "evt-7", Topic: "order.placed", Key: "customer-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s.attempts != 1 || s.key != "customer-1" || s.id != "evt-7" {
			t.Fatalf("got %+v, want attempt 1 of evt-7 for customer-1", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the message was not delivered again")
	}
}

func TestGroupsEachReceiveEveryMessage(t *testing.T) {
	client, _ := connect(t)
	var billing, mail atomic.Int32
	for group, n := range map[string]*atomic.Int32{"billing": &billing, "mail": &mail} {
		subscribe(t, client, &pubsubx.Broker{Client: client, Group: group, Create: true}, func(context.Context, queue.Message) error {
			n.Add(1)
			return nil
		})
	}
	pub := &pubsubx.Broker{Client: client}
	m, _ := orderPlaced.Encode("k", placed{OrderID: "o-1"})
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	kakarottest.Eventually(t, 5*time.Second, "both groups handled the message", func() bool {
		return billing.Load() == 1 && mail.Load() == 1
	})
}

func TestMessageThatKeepsFailingIsDeadLettered(t *testing.T) {
	client, _ := connect(t)
	broker := &pubsubx.Broker{Client: client, Group: "billing", Create: true, DeadLetter: ".dead", MaxDeliver: 5, Backoff: time.Millisecond}

	// Read the dead-letter topic through a subscription of its own.
	dead := &pubsubx.Broker{Client: client, Group: "ops", Create: true}
	got := make(chan queue.Message, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_ = dead.Subscribe(ctx, "order.placed.dead", func(_ context.Context, m queue.Message) error {
			got <- m
			return nil
		})
	}()

	var calls atomic.Int32
	subscribe(t, client, broker, func(context.Context, queue.Message) error {
		calls.Add(1)
		return errors.New("always fails")
	})
	kakarottest.Eventually(t, 5*time.Second, "the dead-letter subscription exists", func() bool {
		_, err := client.SubscriptionAdminClient.GetSubscription(t.Context(), &pubsubpb.GetSubscriptionRequest{Subscription: "projects/kakarot/subscriptions/ops.order.placed.dead"})
		return err == nil
	})
	if err := broker.Publish(t.Context(), queue.Message{ID: "evt-9", Topic: "order.placed", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m.ID != "evt-9" {
			t.Fatalf("dead-lettered %+v", m)
		}
		if n := calls.Load(); n != 5 {
			t.Fatalf("handled %d times, want MaxDeliver = 5", n)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("not dead-lettered after %d calls", calls.Load())
	}
}

func TestSubscribeWithoutASubscriptionFails(t *testing.T) {
	client, _ := connect(t)
	err := (&pubsubx.Broker{Client: client, Group: "billing"}).Subscribe(t.Context(), "order.placed", func(context.Context, queue.Message) error { return nil })
	if err == nil {
		t.Fatal("want an error: the subscription does not exist and Create is off")
	}
}

func TestBrokerReportsMissingSettings(t *testing.T) {
	if err := (&pubsubx.Broker{}).Publish(t.Context(), queue.Message{Topic: "x"}); err == nil {
		t.Fatal("Publish: want an error without Client")
	}
	client, _ := connect(t)
	if err := (&pubsubx.Broker{Client: client}).Subscribe(t.Context(), "x", nil); err == nil {
		t.Fatal("Subscribe: want an error without Group")
	}
}

func TestOrderedBrokerKeepsOneKeyInOrder(t *testing.T) {
	client, _ := connect(t)
	broker := &pubsubx.Broker{Client: client, Group: "billing", Create: true, Ordered: true}
	t.Cleanup(func() { _ = broker.Close() })

	got := make(chan string, 10)
	subscribe(t, client, broker, func(_ context.Context, m queue.Message) error {
		if m.Key != "customer-1" {
			t.Errorf("Key = %q", m.Key)
		}
		got <- m.ID
		return nil
	})
	want := []string{"e1", "e2", "e3", "e4", "e5"}
	for _, id := range want {
		if err := broker.Publish(t.Context(), queue.Message{ID: id, Topic: "order.placed", Key: "customer-1", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range want {
		select {
		case g := <-got:
			if g != id {
				t.Fatalf("got %s, want %s", g, id)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not arrive", id)
		}
	}
}
