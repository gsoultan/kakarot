package data_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/gsoultan/kakarot"
	kdata "github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/kakarot/data/outbox"
	outboxmodel "github.com/gsoultan/kakarot/data/outbox/model"
	"github.com/gsoultan/kakarot/examples/orders/data"
	"github.com/gsoultan/kakarot/examples/orders/data/model"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/usecase"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/migrate"
	"github.com/gsoultan/storm/runtime/pgxdrv"
)

// Runs against a real PostgreSQL when KAKAROT_TEST_DSN is set.
func setup(t *testing.T) (*data.OrderRepo, *kdata.TxManager) {
	t.Helper()
	dsn := os.Getenv("KAKAROT_TEST_DSN")
	if dsn == "" {
		t.Skip("KAKAROT_TEST_DSN not set")
	}
	ctx := t.Context()
	pool, err := pgxdrv.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s, err := storm.Build(&model.Order{}, &model.OrderItem{}, &outboxmodel.OutboxEvent{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.AutoPool(ctx, pool, s, migrate.AutoOptions{}); err != nil {
		t.Fatal(err)
	}
	db := pgxdrv.Pool{P: pool}
	if _, err := db.Exec(ctx, "TRUNCATE orders, order_items, outbox_events", nil); err != nil {
		t.Fatal(err)
	}
	return &data.OrderRepo{DB: db}, kdata.NewTxManager(db)
}

func place(t *testing.T, repo *data.OrderRepo, tx *kdata.TxManager, customer string) domain.Order {
	t.Helper()
	uc := &usecase.PlaceOrder{Orders: repo, Events: &data.Events{DB: repo.DB}, Tx: tx}
	o, err := uc.Execute(t.Context(), domain.PlaceOrder{CustomerID: customer, Items: []domain.LineItem{{SKU: "pen", Quantity: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestLiveOutboxAndRelay(t *testing.T) {
	repo, tx := setup(t)
	o := place(t, repo, tx, "c-ev")

	var got []domain.Order
	relay := &outbox.Relay{DB: repo.DB, Publisher: outbox.PublisherFunc(func(_ context.Context, m outbox.Message) error {
		ev, err := data.OrderPlaced.Decode(m) // typed decode
		got = append(got, ev)
		return err
	})}
	n, err := relay.RunOnce(t.Context())
	if err != nil || n != 1 || len(got) != 1 || got[0].ID != o.ID {
		t.Fatalf("n=%d err=%v got=%+v", n, err, got)
	}
	if n, _ := relay.RunOnce(t.Context()); n != 0 {
		t.Fatalf("event published twice: n=%d", n)
	}
}

func TestLiveRelayRetriesFailedPublish(t *testing.T) {
	repo, tx := setup(t)
	place(t, repo, tx, "c-retry")
	fail := true
	relay := &outbox.Relay{DB: repo.DB, Publisher: outbox.PublisherFunc(func(context.Context, outbox.Message) error {
		if fail {
			return errors.New("broker down")
		}
		return nil
	})}
	if n, err := relay.RunOnce(t.Context()); n != 0 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	fail = false
	if n, err := relay.RunOnce(t.Context()); n != 1 || err != nil {
		t.Fatalf("retry: n=%d err=%v", n, err)
	}
}

func TestLivePagination(t *testing.T) {
	repo, tx := setup(t)
	var ids []string
	for range 5 {
		ids = append(ids, place(t, repo, tx, "c-page").ID)
	}
	place(t, repo, tx, "someone-else")

	var seen []string
	cursor := ""
	for pages := 0; ; pages++ {
		p, err := repo.ListByCustomer(t.Context(), "c-page", cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range p.Items {
			seen = append(seen, o.ID)
		}
		if p.Next == "" {
			if pages != 2 {
				t.Fatalf("pages=%d", pages+1)
			}
			break
		}
		cursor = p.Next
	}
	if len(seen) != 5 {
		t.Fatalf("seen %d orders: %v", len(seen), seen)
	}
	for i := range ids {
		if seen[i] != ids[i] {
			t.Fatalf("order mismatch at %d: %v vs %v", i, seen, ids)
		}
	}
}

func TestLivePlaceAndGet(t *testing.T) {
	repo, tx := setup(t)
	uc := &usecase.PlaceOrder{Orders: repo, Events: &data.Events{DB: repo.DB}, Tx: tx}
	o, err := uc.Execute(t.Context(), domain.PlaceOrder{
		CustomerID: "c-1",
		Items:      []domain.LineItem{{SKU: "pen", Quantity: 2}, {SKU: "ink", Quantity: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.Get(t.Context(), o.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.CustomerID != "c-1" || got.Status != domain.StatusPlaced || len(got.Items) != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestLiveRollback(t *testing.T) {
	repo, tx := setup(t)
	var id string
	boom := errors.New("boom")
	err := tx.Within(t.Context(), func(ctx context.Context) error {
		o := domain.Order{CustomerID: "c-2", Status: domain.StatusPlaced,
			Items: []domain.LineItem{{SKU: "pen", Quantity: 1}}}
		if err := repo.Create(ctx, &o); err != nil {
			return err
		}
		if err := (&data.Events{DB: repo.DB}).OrderPlaced(ctx, o); err != nil {
			return err
		}
		id = o.ID
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if _, err := repo.Get(t.Context(), id); kakarot.CodeOf(err) != kakarot.CodeNotFound {
		t.Fatalf("order survived rollback: err = %v", err)
	}
	relay := &outbox.Relay{DB: repo.DB, Publisher: outbox.PublisherFunc(func(context.Context, outbox.Message) error { return nil })}
	if n, _ := relay.RunOnce(t.Context()); n != 0 {
		t.Fatal("event survived rollback")
	}
}

func TestLiveGetMissing(t *testing.T) {
	repo, _ := setup(t)
	_, err := repo.Get(t.Context(), "00000000-0000-0000-0000-000000000000")
	if kakarot.CodeOf(err) != kakarot.CodeNotFound {
		t.Fatalf("err = %v", err)
	}
}
