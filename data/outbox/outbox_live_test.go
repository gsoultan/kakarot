package outbox_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	idemmodel "github.com/gsoultan/kakarot/data/idempotency/model"
	"github.com/gsoultan/kakarot/data/outbox"
	"github.com/gsoultan/kakarot/data/outbox/model"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/migrate"
	"github.com/gsoultan/storm/runtime/pgxdrv"
)

// Runs against a real PostgreSQL when KAKAROT_TEST_DSN is set.
func setup(t *testing.T) storm.DB {
	t.Helper()
	dsn := os.Getenv("KAKAROT_TEST_DSN")
	if dsn == "" {
		t.Skip("KAKAROT_TEST_DSN not set")
	}
	pool, err := pgxdrv.NewPool(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s, err := storm.Build(&model.OutboxEvent{}, &idemmodel.IdempotencyRecord{}) // every table of the test database
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.AutoPool(t.Context(), pool, s, migrate.AutoOptions{}); err != nil {
		t.Fatal(err)
	}
	db := pgxdrv.Pool{P: pool}
	if _, err := db.Exec(t.Context(), "TRUNCATE outbox_events", nil); err != nil {
		t.Fatal(err)
	}
	return db
}

type greeted struct {
	Name string `json:"name"`
}

const greetings outbox.Topic[greeted] = "greeted"

type traceKey struct{}

// The headers of the call that added an event reach the consumer's handler.
func TestLiveHeadersTravelFromTheCallToTheConsumer(t *testing.T) {
	db := setup(t)
	inject := func(ctx context.Context) map[string]string {
		id, _ := ctx.Value(traceKey{}).(string)
		return map[string]string{"traceparent": id}
	}

	// A call under the Carry middleware adds an event.
	add := kakarot.Wrap(kakarot.Op{Service: "hello", Method: "Greet"},
		func(ctx context.Context, name string) (string, error) {
			return name, greetings.Add(ctx, db, name, greeted{Name: name})
		}, outbox.Carry(inject))
	if _, err := add(context.WithValue(t.Context(), traceKey{}, "00-abc-def-01"), "ada"); err != nil {
		t.Fatal(err)
	}
	// One without it adds a second, with no headers.
	if err := greetings.Add(t.Context(), db, "bob", greeted{Name: "bob"}); err != nil {
		t.Fatal(err)
	}

	seen := map[string]string{}
	broker := &queue.Memory{}
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		done <- broker.Subscribe(ctx, "greeted", func(_ context.Context, m queue.Message) error {
			seen[m.Key] = m.Headers["traceparent"]
			return nil
		})
	}()
	kakarottest.Eventually(t, time.Second, "subscribed", func() bool { return broker.Subscribers("greeted") > 0 })
	relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
	if n, err := relay.RunOnce(t.Context()); err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if seen["ada"] != "00-abc-def-01" {
		t.Fatalf("ada's headers: %q", seen["ada"])
	}
	if h, ok := seen["bob"]; !ok || h != "" {
		t.Fatalf("bob's headers: %q %v", h, ok)
	}
}
