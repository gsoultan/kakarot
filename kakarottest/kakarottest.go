// Package kakarottest is the test kit: small helpers for testing each layer
// of a kakarot service with the standard testing package.
//
// Usecases need no kit at all: give them in-memory ports and call Execute.
// The helpers here cover what is repetitive around them:
//
//	Tx          a TxManager for usecase tests, with commit and rollback hooks
//	Do          one HTTP request against a router, decoded and typed
//	WantCode    assert an error's code
//	Eventually  wait for something asynchronous
//	Serve       run servers (consumer, relay, job) for the length of a test
//	Publish     send a typed message to an in-process broker
//	Clock       a clock the test moves by hand
package kakarottest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/queue"
)

// Tx is a kakarot.TxManager for usecase tests. It runs the function
// directly, counts what happened, and can simulate a failed commit.
//
//	tx := &kakarottest.Tx{OnRollback: repo.restore}
//	uc := &usecase.Transfer{Accounts: repo, Tx: tx}
type Tx struct {
	// FailCommit, when set, is returned after the function succeeds, as a
	// database that fails at COMMIT would. OnRollback runs.
	FailCommit error
	// OnBegin, OnCommit and OnRollback let a fake repository snapshot and
	// restore its state, to give the fake real transaction semantics.
	OnBegin, OnCommit, OnRollback func()

	mu                 sync.Mutex
	commits, rollbacks int
	depth              int
}

// Within implements kakarot.TxManager. A nested call joins the outer
// transaction, as the storm TxManager does.
func (t *Tx) Within(ctx context.Context, fn func(ctx context.Context) error) error {
	t.mu.Lock()
	outer := t.depth == 0
	t.depth++
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		t.depth--
		t.mu.Unlock()
	}()
	if !outer {
		return fn(ctx)
	}
	if t.OnBegin != nil {
		t.OnBegin()
	}
	err := fn(ctx)
	if err == nil {
		err = t.FailCommit
	}
	t.mu.Lock()
	if err != nil {
		t.rollbacks++
	} else {
		t.commits++
	}
	t.mu.Unlock()
	switch {
	case err != nil && t.OnRollback != nil:
		t.OnRollback()
	case err == nil && t.OnCommit != nil:
		t.OnCommit()
	}
	return err
}

// Commits reports how many transactions committed.
func (t *Tx) Commits() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commits
}

// Rollbacks reports how many transactions rolled back.
func (t *Tx) Rollbacks() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rollbacks
}

// Response is the result of Do.
type Response[T any] struct {
	Status int
	Header http.Header
	Body   T      // decoded when the status is below 400
	Raw    []byte // the body as sent
	// Error is the decoded error body when the status is 400 or above.
	Error ErrorBody
}

// ErrorBody is kakarot's default error shape: {"error":{code,message,fields}}.
type ErrorBody struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields"`
}

// Do sends one request to h and decodes the answer as T. body may be nil, a
// string or []byte sent as is, an io.Reader, or any value sent as JSON.
// headers are name, value pairs.
//
//	res := kakarottest.Do[domain.Order](t, router, "POST", "/orders", domain.PlaceOrder{...},
//		"Idempotency-Key", "cart-1")
//	if res.Status != http.StatusCreated || res.Body.Total != 2700 { ... }
//
// Use Do[kakarottest.None] when the body does not matter.
func Do[T any](t testing.TB, h http.Handler, method, target string, body any, headers ...string) Response[T] {
	t.Helper()
	if len(headers)%2 != 0 {
		t.Fatalf("kakarottest.Do: headers must be name, value pairs, got %d strings", len(headers))
	}
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	case []byte:
		rd = bytes.NewReader(b)
	case io.Reader:
		rd = b
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatalf("kakarottest.Do: encode body: %v", err)
		}
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequestWithContext(t.Context(), method, target, rd)
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	res := Response[T]{Status: w.Code, Header: w.Header(), Raw: w.Body.Bytes()}
	if len(res.Raw) == 0 {
		return res
	}
	if res.Status >= 400 {
		var wrap struct {
			Error ErrorBody `json:"error"`
		}
		_ = json.Unmarshal(res.Raw, &wrap) // a custom error encoder may use another shape: see Raw
		res.Error = wrap.Error
		return res
	}
	if _, skip := any(res.Body).(None); skip {
		return res
	}
	if err := json.Unmarshal(res.Raw, &res.Body); err != nil {
		t.Fatalf("kakarottest.Do: %s %s answered %d with a body that is not %T: %v\n%s",
			method, target, res.Status, res.Body, err, res.Raw)
	}
	return res
}

// None is the body type for Do when the response body is not needed.
type None struct{}

// WantCode fails the test unless err carries code.
func WantCode(t testing.TB, err error, code kakarot.Code) {
	t.Helper()
	switch {
	case err == nil:
		t.Fatalf("want error %s, got nil", code)
	case !kakarot.Is(err, code):
		t.Fatalf("want error %s, got %s: %v", code, kakarot.CodeOf(err), err)
	}
}

// WantField fails the test unless err reports a problem for field.
func WantField(t testing.TB, err error, field string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want a problem with field %q, got nil", field)
	}
	if e := kakarot.AsError(err); e.Fields[field] == "" {
		t.Fatalf("want a problem with field %q, got fields %v (%v)", field, e.Fields, err)
	}
}

// Eventually polls ok until it returns true, and fails the test with what
// when within runs out. Use it for results that arrive asynchronously.
//
//	kakarottest.Eventually(t, 5*time.Second, "order shipped", func() bool { return status(id) == "shipped" })
func Eventually(t testing.TB, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for wait := time.Millisecond; ; wait = min(wait*2, 50*time.Millisecond) {
		if ok() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not within %s: %s", within, what)
		}
		time.Sleep(wait)
	}
}

// Serve starts the servers and shuts them down when the test ends. A server
// that stops with an error fails the test.
//
//	kakarottest.Serve(t, consumer, relay)
func Serve(t testing.TB, servers ...kakarot.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Go(func() {
			if err := s.Serve(ctx); err != nil && ctx.Err() == nil {
				t.Errorf("kakarottest.Serve: %T stopped: %v", s, err)
			}
		})
	}
	t.Cleanup(func() {
		stop, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		for _, s := range servers {
			_ = s.Shutdown(stop)
		}
		cancel()
		wg.Wait()
	})
}

// Publish sends payload to topic on an in-process broker and waits for the
// handlers. It returns the handlers' error, which is what a real broker
// would use to decide on redelivery. It waits for a consumer started with
// Serve to subscribe first.
func Publish[T any](t testing.TB, b *queue.Memory, topic queue.Topic[T], key string, payload T) error {
	t.Helper()
	m, err := topic.Encode(key, payload)
	if err != nil {
		t.Fatalf("kakarottest.Publish: %v", err)
	}
	Eventually(t, 5*time.Second, "a subscriber for "+string(topic), func() bool { return b.Subscribers(string(topic)) > 0 })
	return b.Publish(t.Context(), m)
}

// Clock is a clock the test moves by hand. Pass its Now method wherever the
// code under test takes a func() time.Time.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock returns a clock set to start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now returns the clock's time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
