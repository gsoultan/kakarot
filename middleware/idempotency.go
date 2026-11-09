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

// Idempotent replays the stored response for a repeated key instead of
// running the operation again. Requests with an empty key run normally.
func Idempotent[Req Keyed, Resp any](store IdempotencyStore[Resp]) kakarot.TypedMiddleware[Req, Resp] {
	return func(_ kakarot.Op, next kakarot.Endpoint[Req, Resp]) kakarot.Endpoint[Req, Resp] {
		return func(ctx context.Context, req Req) (Resp, error) {
			key := req.IdempotencyKey()
			if key == "" {
				return next(ctx, req)
			}
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

	mu      sync.Mutex
	entries map[string]idemEntry[Resp]
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
	if e, ok := s.entries[key]; ok && time.Now().Before(e.expires) {
		if !e.done {
			return zero, false, kakarot.Conflict("request with this idempotency key is in progress", nil)
		}
		return e.resp, true, nil
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
