package kafkax

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gsoultan/kakarot/queue"
)

// The tests that need a Kafka cluster are in ./integration.

func TestRecordAndMessageAreInverse(t *testing.T) {
	in := queue.Message{ID: "evt-1", Topic: "order.placed", Key: "c-1", Payload: []byte(`{}`), Headers: map[string]string{"traceparent": "00-a-b-01"}}
	out := message(record(in))
	if out.ID != in.ID || out.Topic != in.Topic || out.Key != in.Key || string(out.Payload) != "{}" || out.Headers["traceparent"] != "00-a-b-01" || len(out.Headers) != 1 {
		t.Fatalf("got %+v", out)
	}
	// Without an ID the record's position names the message.
	if got := message(&kgo.Record{Topic: "t", Partition: 2, Offset: 7}).ID; got != "t/2/7" {
		t.Fatalf("ID = %q", got)
	}
}

func TestBrokerReportsMissingSettings(t *testing.T) {
	if err := (&Broker{}).Publish(t.Context(), queue.Message{Topic: "x"}); err == nil {
		t.Fatal("Publish: want an error without Brokers")
	}
	if err := (&Broker{Brokers: []string{"localhost:1"}}).Subscribe(t.Context(), "x", nil); err == nil {
		t.Fatal("Subscribe: want an error without Group")
	}
}
