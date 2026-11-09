// Package data connects kakarot repositories to storm
// (github.com/gsoultan/storm).
//
// storm passes its Executor explicitly. kakarot carries the open transaction
// in context.Context instead, so a usecase can run several repository calls in
// one transaction without importing storm:
//
//	// usecase
//	err := u.Tx.Within(ctx, func(ctx context.Context) error {
//	    return u.Orders.Create(ctx, &o)
//	})
//
//	// repository (storm-generated package "order")
//	row, err := c.Insert(ctx, data.Executor(ctx, r.db))
package data

import (
	"context"
	"errors"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/runtime"
)

type txKey struct{}

// TxManager implements kakarot.TxManager over a storm.DB.
type TxManager struct {
	DB storm.DB
}

var _ kakarot.TxManager = (*TxManager)(nil)

// NewTxManager returns a TxManager for db (for example pgxdrv.Pool{P: pool}).
func NewTxManager(db storm.DB) *TxManager { return &TxManager{DB: db} }

// Within runs fn in one storm transaction. A nested Within joins the
// transaction already in ctx instead of opening a second one.
func (m *TxManager) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(storm.Executor); ok {
		return fn(ctx)
	}
	return storm.InTx(ctx, m.DB, func(ex storm.Executor) error {
		return fn(context.WithValue(ctx, txKey{}, ex))
	})
}

// Executor returns the transaction carried by ctx, or db when there is none.
// Every storm-generated call takes the result.
func Executor(ctx context.Context, db storm.Executor) storm.Executor {
	if ex, ok := ctx.Value(txKey{}).(storm.Executor); ok {
		return ex
	}
	return db
}

// InTx reports whether ctx carries a transaction.
func InTx(ctx context.Context) bool {
	_, ok := ctx.Value(txKey{}).(storm.Executor)
	return ok
}

// MapErr turns storm errors into kakarot errors, so callers above the
// repository never see a storm type:
//
//	runtime.ErrNoRow               -> CodeNotFound
//	runtime.ErrUniqueViolation     -> CodeConflict
//	runtime.ErrForeignKeyViolation -> CodeInvalid
//	runtime.ErrCheckViolation      -> CodeInvalid
//	runtime.ErrNotNullViolation    -> CodeInvalid
//	serialization / deadlock       -> CodeConflict (retryable)
//
// Anything else stays internal.
func MapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, runtime.ErrNoRow):
		return kakarot.E(kakarot.CodeNotFound, "not found", err)
	case errors.Is(err, runtime.ErrUniqueViolation):
		return kakarot.Conflict("already exists", err)
	case errors.Is(err, runtime.ErrForeignKeyViolation),
		errors.Is(err, runtime.ErrCheckViolation),
		errors.Is(err, runtime.ErrNotNullViolation):
		return kakarot.E(kakarot.CodeInvalid, "violates a data constraint", err)
	case runtime.Retryable(err):
		return kakarot.Conflict("concurrent update, retry", err)
	case errors.Is(err, context.DeadlineExceeded):
		return kakarot.E(kakarot.CodeTimeout, "database timeout", err)
	}
	return err
}
