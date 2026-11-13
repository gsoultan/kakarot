// Package idempotency is a storm-backed middleware.IdempotencyStore, shared
// by every instance of a service: a repeated Idempotency-Key replays the
// stored response whichever instance receives it.
//
//	store := &idempotency.Store[domain.Order]{DB: db}
//	ep := kakarot.Decorate(op, eps.PlaceOrder, middleware.Idempotent[domain.PlaceOrder, domain.Order](store))
//
// Add &model.IdempotencyRecord{} to your storm.Build / storm diff model so
// the idempotency_records table is created with your schema. Call Purge from
// a scheduled job to delete expired records.
package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/data"
	rec "github.com/gsoultan/kakarot/data/internal/store/idempotencyrecord"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/runtime"
)

// Store keeps responses of type Resp as JSON in PostgreSQL.
type Store[Resp any] struct {
	DB storm.DB
	// TTL is how long a finished response is replayed; default 24h.
	TTL time.Duration
	// Lock is how long an unfinished call holds its key before another may
	// take over (the first caller is presumed dead); default 1m.
	Lock time.Duration
}

var _ middleware.IdempotencyStore[struct{}] = (*Store[struct{}])(nil)

// Begin claims key, or returns the stored response.
func (s *Store[Resp]) Begin(ctx context.Context, key string) (Resp, bool, error) {
	var (
		resp Resp
		done bool
	)
	lock := s.Lock
	if lock <= 0 {
		lock = time.Minute
	}
	err := storm.InTx(ctx, s.DB, func(ex storm.Executor) error {
		row, found, err := rec.New().Where(rec.Key.Eq(key)).ForUpdate().One(ctx, ex)
		if err != nil {
			return err
		}
		now := time.Now()
		switch {
		case !found:
			ins := rec.Create()
			ins.SetKey(key)
			ins.SetResponse(runtime.JSON("null"))
			ins.SetExpiresAt(now.Add(lock))
			_, err := ins.Insert(ctx, ex)
			return err
		case !now.Before(row.ExpiresAt): // expired: take the key over
			m := rec.Mutate(row)
			m.SetDone(false)
			m.SetResponse(runtime.JSON("null"))
			m.SetExpiresAt(now.Add(lock))
			return m.Update(ctx, ex)
		case !row.Done:
			return errInProgress
		}
		done = true
		return json.Unmarshal(row.Response, &resp)
	})
	switch {
	case err == nil:
		return resp, done, nil
	case errors.Is(err, errInProgress), errors.Is(err, runtime.ErrUniqueViolation):
		// A unique violation means another caller inserted the key first.
		var zero Resp
		return zero, false, errInProgress
	}
	var zero Resp
	return zero, false, data.MapErr(err)
}

var errInProgress = kakarot.Conflict("request with this idempotency key is in progress", nil)

// Finish stores resp for key, or releases the key when the call failed so
// the client can retry.
func (s *Store[Resp]) Finish(ctx context.Context, key string, resp Resp, callErr error) error {
	// The caller's context may already be cancelled; the record must still
	// be settled, within a bound.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if callErr != nil {
		_, err := rec.New().Where(rec.Key.Eq(key)).DeleteAll(ctx, s.DB)
		return data.MapErr(err)
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return kakarot.E(kakarot.CodeInternal, "encode idempotent response", err)
	}
	ttl := s.TTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return data.MapErr(storm.InTx(ctx, s.DB, func(ex storm.Executor) error {
		row, found, err := rec.New().Where(rec.Key.Eq(key)).ForUpdate().One(ctx, ex)
		if err != nil || !found {
			return err
		}
		m := rec.Mutate(row)
		m.SetDone(true)
		m.SetResponse(b)
		m.SetExpiresAt(time.Now().Add(ttl))
		return m.Update(ctx, ex)
	}))
}

// Purge deletes expired records and reports how many were removed.
func (s *Store[Resp]) Purge(ctx context.Context) (int64, error) {
	n, err := rec.New().Where(rec.ExpiresAt.Lt(time.Now())).DeleteAll(ctx, s.DB)
	return n, data.MapErr(err)
}
