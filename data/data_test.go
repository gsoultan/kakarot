package data_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/runtime"
)

// fakeDB satisfies storm.DB; only StartTx is exercised.
type fakeDB struct {
	storm.Executor
	started, committed, rolledBack int
}

type fakeTx struct {
	storm.Executor
	db *fakeDB
}

func (d *fakeDB) StartTx(context.Context) (storm.Tx, error) {
	d.started++
	return &fakeTx{db: d}, nil
}
func (t *fakeTx) Commit(context.Context) error   { t.db.committed++; return nil }
func (t *fakeTx) Rollback(context.Context) error { t.db.rolledBack++; return nil }

func TestWithinCommitsAndCarriesTx(t *testing.T) {
	db := &fakeDB{}
	m := data.NewTxManager(db)
	err := m.Within(t.Context(), func(ctx context.Context) error {
		if !data.InTx(ctx) {
			t.Fatal("no tx in ctx")
		}
		if data.Executor(ctx, db) == storm.Executor(db) {
			t.Fatal("Executor returned the pool inside a tx")
		}
		// nested Within joins, does not start a second tx
		return m.Within(ctx, func(context.Context) error { return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	if db.started != 1 || db.committed != 1 {
		t.Fatalf("started=%d committed=%d", db.started, db.committed)
	}
}

func TestWithinRollsBackOnError(t *testing.T) {
	db := &fakeDB{}
	boom := errors.New("boom")
	err := data.NewTxManager(db).Within(t.Context(), func(context.Context) error { return boom })
	if !errors.Is(err, boom) || db.rolledBack != 1 || db.committed != 0 {
		t.Fatalf("err=%v rolledBack=%d committed=%d", err, db.rolledBack, db.committed)
	}
}

func TestExecutorOutsideTxIsPool(t *testing.T) {
	db := &fakeDB{}
	if data.Executor(t.Context(), db) != storm.Executor(db) {
		t.Fatal("want pool")
	}
}

func TestMapErr(t *testing.T) {
	cases := map[error]kakarot.Code{
		runtime.ErrNoRow:               kakarot.CodeNotFound,
		runtime.ErrUniqueViolation:     kakarot.CodeConflict,
		runtime.ErrForeignKeyViolation: kakarot.CodeInvalid,
		runtime.ErrCheckViolation:      kakarot.CodeInvalid,
		errors.New("other"):            kakarot.CodeInternal,
	}
	for in, want := range cases {
		if got := kakarot.CodeOf(data.MapErr(in)); got != want {
			t.Errorf("MapErr(%v) = %v, want %v", in, got, want)
		}
	}
	if data.MapErr(nil) != nil {
		t.Error("MapErr(nil) != nil")
	}
}
