package pubsubx

import (
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
)

func TestPubSubMessageBecomesAMessage(t *testing.T) {
	attempt := 3
	m := message("order.placed", &pubsub.Message{
		ID:              "server-id",
		Data:            []byte(`{}`),
		OrderingKey:     "ordering",
		DeliveryAttempt: &attempt,
		Attributes:      map[string]string{idAttribute: "evt-1", keyAttribute: "customer-1", "traceparent": "00-a-b-01"},
	})
	if m.ID != "evt-1" || m.Topic != "order.placed" || m.Key != "customer-1" || string(m.Payload) != "{}" || m.Attempts != 2 {
		t.Fatalf("got %+v", m)
	}
	if len(m.Headers) != 1 || m.Headers["traceparent"] != "00-a-b-01" {
		t.Fatalf("Headers = %v", m.Headers)
	}
	// Without the broker's attributes the server's ID and ordering key are used.
	if m := message("t", &pubsub.Message{ID: "server-id", OrderingKey: "k"}); m.ID != "server-id" || m.Key != "k" || m.Attempts != 0 {
		t.Fatalf("got %+v", m)
	}
}

func TestCreatedSubscriptionsFollowTheSettings(t *testing.T) {
	s := subscription("p", "billing.order.placed", "order.placed", true, time.Hour, ".dead", 500)
	if s.Name != "projects/p/subscriptions/billing.order.placed" || s.Topic != "projects/p/topics/order.placed" || !s.EnableMessageOrdering {
		t.Fatalf("got %v", s)
	}
	if got := s.RetryPolicy.MinimumBackoff.AsDuration(); got != 10*time.Minute {
		t.Fatalf("minimum backoff = %v, want the 10m Pub/Sub allows", got)
	}
	if s.DeadLetterPolicy.DeadLetterTopic != "projects/p/topics/order.placed.dead" || s.DeadLetterPolicy.MaxDeliveryAttempts != 100 {
		t.Fatalf("dead letter = %v", s.DeadLetterPolicy)
	}
	if s := subscription("p", "s", "t", false, 0, ".dead", 2); s.DeadLetterPolicy.MaxDeliveryAttempts != 5 || s.RetryPolicy.MinimumBackoff.AsDuration() != time.Second {
		t.Fatalf("got %v", s)
	}
	if s := subscription("p", "s", "t", false, 0, "", 0); s.DeadLetterPolicy != nil {
		t.Fatal("dead-letter policy without DeadLetter")
	}
}
