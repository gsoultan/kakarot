package idempotency_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/data/idempotency"
	"github.com/gsoultan/kakarot/data/idempotency/model"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/migrate"
	"github.com/gsoultan/storm/runtime/pgxdrv"
)

type payReq struct {
	Key    string
	Amount int
}

func (p payReq) IdempotencyKey() string { return p.Key }

type receipt struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

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
	s, err := storm.Build(&model.IdempotencyRecord{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.AutoPool(t.Context(), pool, s, migrate.AutoOptions{}); err != nil {
		t.Fatal(err)
	}
	db := pgxdrv.Pool{P: pool}
	if _, err := db.Exec(t.Context(), "TRUNCATE idempotency_records", nil); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestLiveReplayAndRelease(t *testing.T) {
	db := setup(t)
	store := &idempotency.Store[receipt]{DB: db}
	var charges atomic.Int32
	fail := false
	pay := kakarot.Decorate(kakarot.Op{Service: "pay", Method: "Charge"},
		func(_ context.Context, r payReq) (receipt, error) {
			if fail {
				return receipt{}, errors.New("card declined")
			}
			charges.Add(1)
			return receipt{ID: "r-" + r.Key, Amount: r.Amount}, nil
		}, middleware.Idempotent[payReq](store))

	r1, err := pay(t.Context(), payReq{Key: "k1", Amount: 5})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := pay(t.Context(), payReq{Key: "k1", Amount: 5})
	if err != nil || r2 != r1 || charges.Load() != 1 {
		t.Fatalf("replay: r1=%+v r2=%+v err=%v charges=%d", r1, r2, err, charges.Load())
	}

	fail = true
	if _, err := pay(t.Context(), payReq{Key: "k2", Amount: 1}); err == nil {
		t.Fatal("want failure")
	}
	fail = false
	if r, err := pay(t.Context(), payReq{Key: "k2", Amount: 1}); err != nil || r.ID != "r-k2" {
		t.Fatalf("failed call must release its key: %+v %v", r, err)
	}
}

func TestLiveConcurrentSameKeyRunsOnce(t *testing.T) {
	db := setup(t)
	store := &idempotency.Store[receipt]{DB: db}
	var charges atomic.Int32
	pay := kakarot.Decorate(kakarot.Op{Service: "pay", Method: "Charge"},
		func(_ context.Context, r payReq) (receipt, error) {
			charges.Add(1)
			time.Sleep(50 * time.Millisecond)
			return receipt{ID: "r"}, nil
		}, middleware.Idempotent[payReq](store))

	var wg sync.WaitGroup
	var ok, conflict atomic.Int32
	for range 8 {
		wg.Go(func() {
			_, err := pay(context.Background(), payReq{Key: "same"})
			switch {
			case err == nil:
				ok.Add(1)
			case kakarot.CodeOf(err) == kakarot.CodeConflict:
				conflict.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		})
	}
	wg.Wait()
	if charges.Load() != 1 || ok.Load() < 1 || ok.Load()+conflict.Load() != 8 {
		t.Fatalf("charges=%d ok=%d conflict=%d", charges.Load(), ok.Load(), conflict.Load())
	}
}

func TestLiveExpiredKeyIsTakenOverAndPurged(t *testing.T) {
	db := setup(t)
	store := &idempotency.Store[receipt]{DB: db, TTL: time.Millisecond}
	if _, done, err := store.Begin(t.Context(), "x"); err != nil || done {
		t.Fatal(done, err)
	}
	if err := store.Finish(t.Context(), "x", receipt{ID: "old"}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, done, err := store.Begin(t.Context(), "x"); err != nil || done {
		t.Fatalf("expired record must not replay: done=%v err=%v", done, err)
	}
	if err := store.Finish(t.Context(), "x", receipt{ID: "new"}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if n, err := store.Purge(t.Context()); err != nil || n != 1 {
		t.Fatalf("purge n=%d err=%v", n, err)
	}
}
