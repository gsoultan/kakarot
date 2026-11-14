package natsx

import (
	"testing"

	"github.com/gsoultan/kakarot/queue"
)

// The tests that need a NATS server are in ./integration.

func TestDurableNamesHoldNoSubjectCharacters(t *testing.T) {
	for in, want := range map[[2]string]string{
		{"billing", "order.placed"}: "billing-order_placed",
		{"mail", "order.*"}:         "mail-order_any",
		{"audit", "order.>"}:        "audit-order_all",
	} {
		if got := durable(in[0], in[1]); got != want {
			t.Errorf("durable(%q, %q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func TestBrokerNeedsAConnectionAndAStream(t *testing.T) {
	b := &Broker{}
	if err := b.Publish(t.Context(), queue.Message{Topic: "x"}); err == nil {
		t.Fatal("Publish: want an error")
	}
	if err := b.Subscribe(t.Context(), "x", nil); err == nil {
		t.Fatal("Subscribe: want an error")
	}
}
