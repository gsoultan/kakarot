package middleware_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/middleware"
)

var errUnavail = kakarot.E(kakarot.CodeUnavailable, "down", nil)

func TestRetry(t *testing.T) {
	calls := 0
	ep := func(context.Context, int) (int, error) {
		calls++
		if calls < 3 {
			return 0, errUnavail
		}
		return 7, nil
	}
	e := kakarot.Wrap(kakarot.Op{}, ep, middleware.Retry(middleware.RetryPolicy{BaseDelay: time.Millisecond}))
	if v, err := e(t.Context(), 0); err != nil || v != 7 || calls != 3 {
		t.Fatalf("v=%d err=%v calls=%d", v, err, calls)
	}

	calls = 0
	bad := func(context.Context, int) (int, error) { calls++; return 0, kakarot.Invalid("no", nil) }
	_, _ = kakarot.Wrap(kakarot.Op{}, bad, middleware.Retry(middleware.RetryPolicy{}))(t.Context(), 0)
	if calls != 1 {
		t.Fatalf("non-retryable retried: calls=%d", calls)
	}
}

func TestCircuitBreaker(t *testing.T) {
	fail := true
	calls := 0
	ep := func(context.Context, int) (int, error) {
		calls++
		if fail {
			return 0, errUnavail
		}
		return 1, nil
	}
	e := kakarot.Wrap(kakarot.Op{Method: "X"}, ep,
		middleware.CircuitBreaker(middleware.BreakerPolicy{Failures: 2, OpenFor: 20 * time.Millisecond}))
	for range 2 {
		_, _ = e(t.Context(), 0)
	}
	if _, err := e(t.Context(), 0); kakarot.CodeOf(err) != kakarot.CodeUnavailable || calls != 2 {
		t.Fatalf("breaker not open: err=%v calls=%d", err, calls)
	}
	time.Sleep(25 * time.Millisecond)
	fail = false
	if v, err := e(t.Context(), 0); err != nil || v != 1 {
		t.Fatalf("probe failed: %v", err)
	}
	if _, err := e(t.Context(), 0); err != nil {
		t.Fatalf("breaker did not close: %v", err)
	}
}

func TestRateLimit(t *testing.T) {
	e := kakarot.Wrap(kakarot.Op{Method: "R"}, func(context.Context, int) (int, error) { return 1, nil },
		middleware.RateLimit(1, 2))
	var limited int
	for range 4 {
		if _, err := e(t.Context(), 0); err != nil {
			if kakarot.CodeOf(err) != middleware.CodeRateLimited {
				t.Fatal(err)
			}
			limited++
		}
	}
	if limited != 2 {
		t.Fatalf("limited=%d, want 2", limited)
	}
}

type payReq struct {
	Key    string
	Amount int
}

func (p payReq) IdempotencyKey() string { return p.Key }

func TestIdempotent(t *testing.T) {
	var mu sync.Mutex
	charges := 0
	pay := func(_ context.Context, r payReq) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		charges++
		return "receipt-" + r.Key, nil
	}
	store := &middleware.MemoryIdempotencyStore[string]{}
	e := kakarot.Decorate(kakarot.Op{}, pay, middleware.Idempotent[payReq](store))

	r1, err1 := e(t.Context(), payReq{Key: "k1", Amount: 5})
	r2, err2 := e(t.Context(), payReq{Key: "k1", Amount: 5})
	if err1 != nil || err2 != nil || r1 != r2 || charges != 1 {
		t.Fatalf("r1=%q r2=%q charges=%d", r1, r2, charges)
	}
	_, _ = e(t.Context(), payReq{Amount: 5}) // no key: runs
	if charges != 2 {
		t.Fatalf("charges=%d", charges)
	}

	failing := kakarot.Decorate(kakarot.Op{}, func(context.Context, payReq) (string, error) {
		return "", errors.New("card declined")
	}, middleware.Idempotent[payReq](store))
	_, _ = failing(t.Context(), payReq{Key: "k2"})
	if _, done, _ := store.Begin(t.Context(), "./k2"); done {
		t.Fatal("failed call was stored")
	}
}

func TestIdempotentScopeAndLimits(t *testing.T) {
	store := &middleware.MemoryIdempotencyStore[string]{MaxEntries: 2}
	mk := func(method string) kakarot.Endpoint[payReq, string] {
		return kakarot.Decorate(kakarot.Op{Service: "pay", Method: method},
			func(_ context.Context, r payReq) (string, error) { return method + "-" + r.Key, nil },
			middleware.Idempotent[payReq](store))
	}
	charge, refund := mk("Charge"), mk("Refund")
	a, _ := charge(t.Context(), payReq{Key: "k"})
	b, _ := refund(t.Context(), payReq{Key: "k"})
	if a != "Charge-k" || b != "Refund-k" {
		t.Fatalf("same key crossed operations: %q %q", a, b)
	}
	if _, err := charge(t.Context(), payReq{Key: strings.Repeat("x", 300)}); kakarot.CodeOf(err) != kakarot.CodeInvalid {
		t.Fatalf("long key: %v", err)
	}
	if _, err := charge(t.Context(), payReq{Key: "a\x00b"}); kakarot.CodeOf(err) != kakarot.CodeInvalid {
		t.Fatalf("control character in key: %v", err)
	}
	if _, err := charge(t.Context(), payReq{Key: "third"}); kakarot.CodeOf(err) != kakarot.CodeUnavailable {
		t.Fatalf("full store: %v", err)
	}
	if again, err := charge(t.Context(), payReq{Key: "k"}); err != nil || again != a {
		t.Fatalf("stored key must still replay when full: %q %v", again, err)
	}
}

func TestLoadShed(t *testing.T) {
	shed := middleware.LoadShed(1)
	hold, entered := make(chan struct{}), make(chan struct{})
	slow := kakarot.Wrap(kakarot.Op{Method: "Slow"}, func(context.Context, int) (int, error) {
		close(entered)
		<-hold
		return 1, nil
	}, shed)
	fast := kakarot.Wrap(kakarot.Op{Method: "Fast"}, func(context.Context, int) (int, error) { return 2, nil }, shed)

	done := make(chan error, 1)
	go func() { _, err := slow(context.Background(), 0); done <- err }()
	<-entered
	if _, err := fast(t.Context(), 0); kakarot.CodeOf(err) != kakarot.CodeUnavailable {
		t.Fatalf("second call was not shed: %v", err)
	}
	close(hold)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if v, err := fast(t.Context(), 0); err != nil || v != 2 {
		t.Fatalf("slot not released: %v %v", v, err)
	}
}
