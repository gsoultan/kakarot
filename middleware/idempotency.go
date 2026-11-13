package middleware

import (
	"context"
	"sync"
	"time"

	"github.com/gsoultan/kakarot"
)

// Keyed is implemented by requests that carry an idempotency key, usually a
// field tagged `header:"Idempotency-Key"`.
type Keyed interface {
	IdempotencyKey() string
}

// IdempotencyStore remembers responses by key. It is typed by the response,
// so a stored value can never come back as the wrong type.
type IdempotencyStore[Resp any] interface {
	// Begin claims key. If a response is already stored it returns it with
	// done=true. If another call holds the key it returns CodeConflict.
	Begin(ctx context.Context, key string) (resp Resp, done bool, err error)
	// Finish stores the response for key, or releases the key on error.
	Finish(ctx context.Context, key string, resp Resp, err error) error
}

// MaxIdempotencyKeyLen bounds client-supplied keys.
const MaxIdempotencyKeyLen = 255

// Idempotent replays the stored response for a repeated key instead of
// running the operation again. Requests with an empty key run normally.
//
// Keys are scoped to the operation, so one key never replays another
// operation's response. They are not scoped to the caller: when responses
// are private, make IdempotencyKey include the caller's identity (for
// example tenant + "/" + header key), so one caller cannot replay another's
// response by guessing a key.
func Idempotent[Req Keyed, Resp any](store IdempotencyStore[Resp]) kakarot.TypedMiddleware[Req, Resp] {
	return func(op kakarot.Op, next kakarot.Endpoint[Req, Resp]) kakarot.Endpoint[Req, Resp] {
		scope := op.Service + "." + op.Method + "/"
		return func(ctx context.Context, req Req) (Resp, error) {
			key := req.IdempotencyKey()
			if key == "" {
				return next(ctx, req)
			}
			if len(key) > MaxIdempotencyKeyLen || !printable(key) {
				var zero Resp
				return zero, kakarot.Invalid("idempotency key must be at most 255 printable ASCII characters", nil)
			}
			key = scope + key
			prev, done, err := store.Begin(ctx, key)
			if err != nil || done {
				return prev, err
			}
			resp, err := next(ctx, req)
			if ferr := store.Finish(ctx, key, resp, err); ferr != nil && err == nil {
				return resp, ferr
			}
			return resp, err
		}
	}
}

// MemoryIdempotencyStore keeps responses in process memory for TTL. Use a
// shared store (database, Redis) when running more than one instance.
type MemoryIdempotencyStore[Resp any] struct {
	TTL time.Duration // default 24h
	// MaxEntries bounds memory, since keys come from clients; default
	// 100000. When full, new keys are refused with CodeUnavailable.
	MaxEntries int

	mu        sync.Mutex
	entries   map[string]idemEntry[Resp]
	nextSweep time.Time
}

type idemEntry[Resp any] struct {
	resp    Resp
	done    bool
	expires time.Time
}

func (s *MemoryIdempotencyStore[Resp]) Begin(_ context.Context, key string) (Resp, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = map[string]idemEntry[Resp]{}
	}
	var zero Resp
	now := time.Now()
	if e, ok := s.entries[key]; ok && now.Before(e.expires) {
		if !e.done {
			return zero, false, kakarot.Conflict("request with this idempotency key is in progress", nil)
		}
		return e.resp, true, nil
	}
	limit := s.MaxEntries
	if limit <= 0 {
		limit = 100000
	}
	// Drop expired entries once a minute, or at once when full.
	if now.After(s.nextSweep) || len(s.entries) >= limit {
		for k, e := range s.entries {
			if !now.Before(e.expires) {
				delete(s.entries, k)
			}
		}
		s.nextSweep = now.Add(time.Minute)
	}
	if _, ok := s.entries[key]; !ok && len(s.entries) >= limit {
		return zero, false, kakarot.E(kakarot.CodeUnavailable, "idempotency store is full", nil)
	}
	s.entries[key] = idemEntry[Resp]{expires: time.Now().Add(time.Minute)}
	return zero, false, nil
}

func (s *MemoryIdempotencyStore[Resp]) Finish(_ context.Context, key string, resp Resp, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		delete(s.entries, key) // let the client retry
		return nil
	}
	ttl := s.TTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	s.entries[key] = idemEntry[Resp]{resp: resp, done: true, expires: time.Now().Add(ttl)}
	return nil
}

// printable reports whether s is visible ASCII, the alphabet of an HTTP
// header value and safe for any store.
func printable(s string) bool {
	for i := range len(s) {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}
