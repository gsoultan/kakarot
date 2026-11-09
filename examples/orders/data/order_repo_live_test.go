package data_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/gsoultan/kakarot"
	kdata "github.com/gsoultan/kakarot/data"
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
	s, err := storm.Build(&model.Order{}, &model.OrderItem{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.AutoPool(ctx, pool, s, migrate.AutoOptions{}); err != nil {
		t.Fatal(err)
	}
	db := pgxdrv.Pool{P: pool}
	return &data.OrderRepo{DB: db}, kdata.NewTxManager(db)
}

func TestLivePlaceAndGet(t *testing.T) {
	repo, tx := setup(t)
	place := &usecase.PlaceOrder{Orders: repo, Tx: tx}
	o, err := place.Execute(t.Context(), domain.PlaceOrder{
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
		id = o.ID
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if _, err := repo.Get(t.Context(), id); kakarot.CodeOf(err) != kakarot.CodeNotFound {
		t.Fatalf("order survived rollback: err = %v", err)
	}
}

func TestLiveGetMissing(t *testing.T) {
	repo, _ := setup(t)
	_, err := repo.Get(t.Context(), "00000000-0000-0000-0000-000000000000")
	if kakarot.CodeOf(err) != kakarot.CodeNotFound {
		t.Fatalf("err = %v", err)
	}
}
