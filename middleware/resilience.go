package middleware

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/gsoultan/kakarot"
)

// RetryPolicy configures Retry.
type RetryPolicy struct {
	Attempts  int                  // total tries, including the first; default 3
	BaseDelay time.Duration        // first backoff; doubles each try with jitter; default 50ms
	MaxDelay  time.Duration        // cap on one backoff; default 2s
	Retryable func(err error) bool // default: CodeUnavailable, CodeTimeout, CodeConflict
}

// Retry re-runs a failed call with exponential backoff and full jitter. Only
// put it on idempotent operations, or behind Idempotent.
func Retry(p RetryPolicy) kakarot.Middleware {
	if p.Attempts <= 0 {
		p.Attempts = 3
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 50 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 2 * time.Second
	}
	if p.Retryable == nil {
		p.Retryable = func(err error) bool {
			switch kakarot.CodeOf(err) {
			case kakarot.CodeUnavailable, kakarot.CodeTimeout, kakarot.CodeConflict:
				return true
			}
			return false
		}
	}
	return func(ctx context.Context, _ kakarot.Op, next kakarot.Next) error {
		delay := p.BaseDelay
		var err error
		for attempt := 1; ; attempt++ {
			if err = next(ctx); err == nil || attempt >= p.Attempts || !p.Retryable(err) {
				return err
			}
			sleep := time.Duration(rand.Int64N(int64(delay) + 1))
			select {
			case <-ctx.Done():
				return err
			case <-time.After(sleep):
			}
			delay = min(delay*2, p.MaxDelay)
		}
	}
}

// BreakerPolicy configures CircuitBreaker.
type BreakerPolicy struct {
	Failures int                  // consecutive failures that open the breaker; default 5
	OpenFor  time.Duration        // how long it stays open before one probe; default 10s
	Counts   func(err error) bool // which errors count; default: internal, unavailable, timeout
}

// CircuitBreaker fails fast with CodeUnavailable after repeated failures, then
// lets one probe through after OpenFor. State is kept per operation.
func CircuitBreaker(p BreakerPolicy) kakarot.Middleware {
	if p.Failures <= 0 {
		p.Failures = 5
	}
	if p.OpenFor <= 0 {
		p.OpenFor = 10 * time.Second
	}
	if p.Counts == nil {
		p.Counts = func(err error) bool {
			switch kakarot.CodeOf(err) {
			case kakarot.CodeInternal, kakarot.CodeUnavailable, kakarot.CodeTimeout:
				return true
			}
			return false
		}
	}
	var mu sync.Mutex
	states := map[kakarot.Op]*breakerState{}
	return func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		mu.Lock()
		st := states[op]
		if st == nil {
			st = &breakerState{}
			states[op] = st
		}
		now := time.Now()
		if st.openUntil.After(now) || st.probing {
			mu.Unlock()
			return kakarot.E(kakarot.CodeUnavailable, op.String()+" is temporarily unavailable", nil)
		}
		probe := !st.openUntil.IsZero() // half-open: this call is the probe
		st.probing = probe
		mu.Unlock()

		err := next(ctx)

		mu.Lock()
		defer mu.Unlock()
		st.probing = false
		if err != nil && p.Counts(err) {
			st.failures++
			if probe || st.failures >= p.Failures {
				st.openUntil = time.Now().Add(p.OpenFor)
			}
		} else {
			st.failures, st.openUntil = 0, time.Time{}
		}
		return err
	}
}

type breakerState struct {
	failures  int
	openUntil time.Time
	probing   bool
}

// RateLimit allows rate calls per second per operation with bursts up to
// burst (token bucket). Excess calls fail with CodeUnavailable; register
// http.StatusTooManyRequests for a dedicated code if you prefer.
func RateLimit(rate float64, burst int) kakarot.Middleware {
	var mu sync.Mutex
	buckets := map[kakarot.Op]*bucket{}
	return func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		mu.Lock()
		b := buckets[op]
		now := time.Now()
		if b == nil {
			b = &bucket{tokens: float64(burst), last: now}
			buckets[op] = b
		}
		b.tokens = min(float64(burst), b.tokens+now.Sub(b.last).Seconds()*rate)
		b.last = now
		ok := b.tokens >= 1
		if ok {
			b.tokens--
		}
		mu.Unlock()
		if !ok {
			return kakarot.E(CodeRateLimited, op.String()+" rate limit exceeded", nil)
		}
		return next(ctx)
	}
}

type bucket struct {
	tokens float64
	last   time.Time
}

// CodeRateLimited is returned by RateLimit. httpx maps it to 429.
var CodeRateLimited = kakarot.NewCode("rate_limited")
