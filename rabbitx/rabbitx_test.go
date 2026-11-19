package rabbitx

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/gsoultan/kakarot/queue"
)

// The tests that need a RabbitMQ server are in live_test.go.

func TestDeliveryBecomesAMessage(t *testing.T) {
	m := message(amqp.Delivery{
		MessageId:  "evt-1",
		RoutingKey: "order.placed",
		Body:       []byte(`{}`),
		Headers: amqp.Table{
			keyHeader:          "customer-1",
			"traceparent":      "00-a-b-01",
			"x-delivery-count": int64(1),
			"x-acquired-count": int64(2),
			"x-not-a-string":   int32(7),
		},
	})
	if m.ID != "evt-1" || m.Topic != "order.placed" || m.Key != "customer-1" || string(m.Payload) != "{}" || m.Attempts != 2 {
		t.Fatalf("got %+v", m)
	}
	if len(m.Headers) != 1 || m.Headers["traceparent"] != "00-a-b-01" {
		t.Fatalf("Headers = %v", m.Headers)
	}
	// A redelivery without a count is known to be at least the second try.
	if got := message(amqp.Delivery{Redelivered: true}).Attempts; got != 1 {
		t.Fatalf("Attempts = %d, want 1", got)
	}
}

func TestQueuesAreQuorumQueuesWithADeliveryLimit(t *testing.T) {
	if got := queueName("billing", "order.placed"); got != "billing.order.placed" {
		t.Fatalf("queueName = %q", got)
	}
	args := queueArgs(5, "dead")
	if args["x-queue-type"] != "quorum" || args["x-delivery-limit"] != int64(5) || args["x-dead-letter-exchange"] != "dead" {
		t.Fatalf("args = %v", args)
	}
	if _, ok := queueArgs(5, "")["x-dead-letter-exchange"]; ok {
		t.Fatal("dead-letter exchange set without DeadLetter")
	}
	if err := args.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerReportsMissingSettings(t *testing.T) {
	if err := (&Broker{}).Publish(t.Context(), queue.Message{Topic: "x"}); err == nil {
		t.Fatal("Publish: want an error without URL and Exchange")
	}
	if err := (&Broker{URL: "amqp://localhost:1/", Exchange: "e"}).Subscribe(t.Context(), "x", nil); err == nil {
		t.Fatal("Subscribe: want an error without Group")
	}
	b := &Broker{URL: "amqp://localhost:1/", Exchange: "e"}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(t.Context(), queue.Message{Topic: "x"}); err == nil {
		t.Fatal("Publish: want an error after Close")
	}
}
