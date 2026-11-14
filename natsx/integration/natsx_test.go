package integration_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/natsx"
	"github.com/gsoultan/kakarot/queue"
)

// connect starts a NATS server with JetStream inside the test.
func connect(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not start")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

type placed struct {
	OrderID string `json:"order_id"`
}

const orderPlaced queue.Topic[placed] = "order.placed"

func TestConsumerReceivesWhatWasPublished(t *testing.T) {
	broker := &natsx.Broker{Conn: connect(t), Stream: "ORDERS", Subjects: []string{"order.>"}, Group: "billing"}

	type seen struct {
		in    placed
		trace string
	}
	got := make(chan seen, 1)
	type traceKey struct{}
	c := &queue.Consumer{Service: "billing", Source: broker,
		Extract: func(ctx context.Context, h map[string]string) context.Context {
			return context.WithValue(ctx, traceKey{}, h["Traceparent"])
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
	m.ID, m.Headers = "evt-1", map[string]string{"Traceparent": "00-abc-def-01"}
	for range 2 { // the second publish has the same ID and is dropped
		if err := broker.Publish(t.Context(), m); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case s := <-got:
		if s.in.OrderID != "o-1" || s.trace != "00-abc-def-01" {
			t.Fatalf("got %+v", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message arrived")
	}
	select {
	case s := <-got:
		t.Fatalf("duplicate delivered: %+v", s)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestFailedMessageIsDeliveredAgain(t *testing.T) {
	broker := &natsx.Broker{Conn: connect(t), Stream: "ORDERS", Subjects: []string{"order.>"}, Group: "billing", Backoff: 10 * time.Millisecond}

	var calls atomic.Int32
	attempts := make(chan int32, 1)
	keys := make(chan string, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- broker.Subscribe(ctx, "order.placed", func(_ context.Context, m queue.Message) error {
			if calls.Add(1) == 1 {
				return kakarot.E(kakarot.CodeUnavailable, "payment provider is down", errors.New("boom"))
			}
			attempts <- m.Attempts
			keys <- m.Key
			return nil
		})
	}()
	if err := broker.Publish(t.Context(), queue.Message{Topic: "order.placed", Key: "customer-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case a := <-attempts:
		if a != 1 {
			t.Fatalf("Attempts = %d, want 1", a)
		}
		if k := <-keys; k != "customer-1" {
			t.Fatalf("Key = %q", k)
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

func TestGroupsEachReceiveEveryMessage(t *testing.T) {
	nc := connect(t)
	var billing, mail atomic.Int32
	for group, n := range map[string]*atomic.Int32{"billing": &billing, "mail": &mail} {
		b := &natsx.Broker{Conn: nc, Stream: "ORDERS", Subjects: []string{"order.>"}, Group: group}
		c := &queue.Consumer{Service: group, Source: b}
		queue.Handle(c, orderPlaced, "On", func(context.Context, placed) (kakarottest.None, error) {
			n.Add(1)
			return kakarottest.None{}, nil
		})
		kakarottest.Serve(t, c)
	}
	pub := &natsx.Broker{Conn: nc, Stream: "ORDERS", Subjects: []string{"order.>"}}
	m, _ := orderPlaced.Encode("k", placed{OrderID: "o-1"})
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	kakarottest.Eventually(t, 5*time.Second, "both groups handled the message", func() bool {
		return billing.Load() == 1 && mail.Load() == 1
	})
}

func TestBrokerReportsMissingSettings(t *testing.T) {
	if err := (&natsx.Broker{}).Publish(t.Context(), queue.Message{Topic: "x"}); err == nil {
		t.Fatal("want an error without Conn and Stream")
	}
	b := &natsx.Broker{Conn: connect(t), Stream: "ORDERS", Subjects: []string{"order.>"}}
	if err := b.Subscribe(t.Context(), "order.placed", nil); err == nil {
		t.Fatal("want an error without Group")
	}
}
