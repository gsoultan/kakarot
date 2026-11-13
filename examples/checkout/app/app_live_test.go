package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/checkout/app"
	"github.com/gsoultan/kakarot/examples/checkout/domain"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/migrate"
	"github.com/gsoultan/storm/runtime/pgxdrv"
)

// provider is a payment provider the tests control by token:
//
//	ok        approved
//	declined  declined
//	down      fails with unavailable while provider.down > 0, counting down
//	hold      waits until the test calls release, then approves
type provider struct {
	mu      sync.Mutex
	charges map[string]string
	calls   int
	down    int
	refunds []string
	holding chan struct{} // closed when a hold charge has started
	release chan struct{}
}

func newProvider() *provider {
	return &provider{charges: map[string]string{}, holding: make(chan struct{}), release: make(chan struct{})}
}

func (p *provider) Charge(ctx context.Context, key, token string, _ int64) (string, error) {
	p.mu.Lock()
	p.calls++
	if id, ok := p.charges[key]; ok {
		p.mu.Unlock()
		return id, nil
	}
	if token == "down" && p.down > 0 {
		p.down--
		p.mu.Unlock()
		return "", kakarot.E(kakarot.CodeUnavailable, "provider down", nil)
	}
	p.mu.Unlock()
	switch token {
	case "declined":
		return "", kakarot.E(domain.CodeDeclined, "card declined", nil)
	case "hold":
		close(p.holding)
		select {
		case <-p.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.charges[key] = "ch_" + key
	return p.charges[key], nil
}

func (p *provider) Refund(_ context.Context, chargeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !slices.Contains(p.refunds, chargeID) { // idempotent, like a real provider
		p.refunds = append(p.refunds, chargeID)
	}
	return nil
}

func (p *provider) snapshot() (charges int, refunds []string, calls int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.charges), slices.Clone(p.refunds), p.calls
}

type inbox struct {
	mu   sync.Mutex
	seen []domain.OrderUpdated
}

func (i *inbox) Notify(_ context.Context, e domain.OrderUpdated) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.seen = append(i.seen, e)
	return nil
}

func (i *inbox) notes(orderID string) []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	var out []string
	for _, e := range i.seen {
		if e.OrderID == orderID {
			out = append(out, e.Note)
		}
	}
	return out
}

// stack is the real checkout against a real PostgreSQL: HTTP, outbox relay,
// queue consumer and expiry job all running. It needs KAKAROT_TEST_DSN.
type stack struct {
	t     *testing.T
	http  http.Handler
	pay   *provider
	inbox *inbox
}

func start(t *testing.T, window time.Duration) *stack {
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
	schema, err := storm.Build(app.Models()...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.AutoPool(ctx, pool, schema, migrate.AutoOptions{}); err != nil {
		t.Fatal(err)
	}
	db := pgxdrv.Pool{P: pool}
	if _, err := db.Exec(ctx, "TRUNCATE order_steps, order_lines, orders, products, outbox_events, idempotency_records", nil); err != nil {
		t.Fatal(err)
	}

	s := &stack{t: t, pay: newProvider(), inbox: &inbox{}}
	c := app.Build(app.Config{
		DB:            db,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Payments:      s.pay,
		Warehouse:     app.DemoWarehouse{},
		Notifier:      s.inbox,
		PaymentWindow: window,
		RelayInterval: 20 * time.Millisecond,
	})
	s.http = c.HTTP
	servers := []kakarot.Server{c.Consumer, c.Relay, c.Expiry}
	for _, srv := range servers {
		go srv.Serve(context.Background())
	}
	t.Cleanup(func() {
		for _, srv := range servers {
			_ = srv.Shutdown(context.Background())
		}
	})
	return s
}

func call[T any](s *stack, method, path string, body any, hdr ...string) (int, T) {
	s.t.Helper()
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rd)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	s.http.ServeHTTP(w, req)
	var out T
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil && w.Body.Len() > 0 {
		s.t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body)
	}
	return w.Code, out
}

type errBody struct {
	Error struct {
		Code   string            `json:"code"`
		Fields map[string]string `json:"fields"`
	} `json:"error"`
}

func (s *stack) product(sku string, price, stock int64) {
	s.t.Helper()
	if code, _ := call[domain.Product](s, "POST", "/products", domain.AddProduct{SKU: sku, Name: sku, Price: price, Stock: stock}); code != http.StatusCreated {
		s.t.Fatalf("add product: %d", code)
	}
}

func (s *stack) stock(sku string) int64 {
	s.t.Helper()
	_, p := call[domain.Product](s, "GET", "/products/"+sku, nil)
	return p.Stock
}

func (s *stack) place(token string, items ...domain.Item) domain.Order {
	s.t.Helper()
	code, o := call[domain.Order](s, "POST", "/orders", domain.PlaceOrder{Customer: "ana", Items: items, PaymentToken: token})
	if code != http.StatusCreated || o.Status != domain.StatusPending {
		s.t.Fatalf("place: %d %+v", code, o)
	}
	return o
}

// until polls the order, as a client would, until ok is satisfied.
func (s *stack) until(id, what string, ok func(domain.Order) bool) domain.Order {
	s.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, o := call[domain.Order](s, "GET", "/orders/"+id, nil)
		if ok(o) {
			return o
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("order never became %s: %+v", what, o)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func is(st domain.Status) func(domain.Order) bool {
	return func(o domain.Order) bool { return o.Status == st }
}

func (s *stack) eventually(what string, ok func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			s.t.Fatalf("never happened: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The request returns at once with a pending order; payment and shipping
// follow on their own.
func TestLiveOrderIsPaidAndShippedInTheBackground(t *testing.T) {
	s := start(t, time.Hour)
	s.product("tea", 500, 10)
	s.product("cup", 1200, 4)

	o := s.place("ok", domain.Item{SKU: "tea", Qty: 3}, domain.Item{SKU: "cup", Qty: 1})
	if o.Total != 2700 {
		t.Fatalf("total = %d", o.Total)
	}
	if s.stock("tea") != 7 || s.stock("cup") != 3 {
		t.Fatalf("stock was not reserved before the request returned")
	}

	done := s.until(o.ID, "shipped", is(domain.StatusShipped))
	if done.Tracking == "" {
		t.Fatalf("no tracking: %+v", done)
	}
	var trail []domain.Status
	for _, st := range done.Steps {
		trail = append(trail, st.Status)
	}
	if !slices.Equal(trail, []domain.Status{domain.StatusPending, domain.StatusPaid, domain.StatusShipped}) {
		t.Fatalf("timeline = %v", trail)
	}
	s.eventually("two notifications", func() bool { return len(s.inbox.notes(o.ID)) == 2 })
	if charges, refunds, _ := s.pay.snapshot(); charges != 1 || len(refunds) != 0 {
		t.Fatalf("charges=%d refunds=%v", charges, refunds)
	}
}

// A declined card is found out asynchronously; the handler compensates by
// returning the stock.
func TestLiveDeclinedPaymentCancelsAndRestocks(t *testing.T) {
	s := start(t, time.Hour)
	s.product("tea", 500, 5)
	o := s.place("declined", domain.Item{SKU: "tea", Qty: 5})
	if s.stock("tea") != 0 {
		t.Fatal("stock not reserved")
	}
	done := s.until(o.ID, "cancelled", is(domain.StatusCancelled))
	if done.Reason != "payment declined" || s.stock("tea") != 5 {
		t.Fatalf("reason=%q stock=%d", done.Reason, s.stock("tea"))
	}
}

func TestLiveOutOfStockIsRefusedSynchronously(t *testing.T) {
	s := start(t, time.Hour)
	s.product("tea", 500, 2)
	s.product("cup", 1200, 9)
	code, body := call[errBody](s, "POST", "/orders", domain.PlaceOrder{
		Customer: "ana", PaymentToken: "ok",
		Items: []domain.Item{{SKU: "cup", Qty: 1}, {SKU: "tea", Qty: 3}},
	})
	if code != http.StatusConflict || body.Error.Code != "out_of_stock" || body.Error.Fields["tea"] != "only 2 left" {
		t.Fatalf("%d %+v", code, body)
	}
	if s.stock("cup") != 9 {
		t.Fatal("a refused order kept stock")
	}
	if code, _ := call[errBody](s, "POST", "/orders", domain.PlaceOrder{Customer: "ana"}); code != http.StatusBadRequest {
		t.Fatalf("invalid order: %d", code)
	}
}

// The provider is down for a while. The message fails, the relay delivers it
// again, and the order is charged exactly once.
func TestLiveProviderOutageIsRetriedUntilPaid(t *testing.T) {
	s := start(t, time.Hour)
	s.product("tea", 500, 5)
	s.pay.down = 7 // more than the three in-handler tries: needs redelivery
	o := s.place("down", domain.Item{SKU: "tea", Qty: 1})
	s.until(o.ID, "shipped", is(domain.StatusShipped))
	if charges, _, calls := s.pay.snapshot(); charges != 1 || calls < 8 {
		t.Fatalf("charges=%d calls=%d", charges, calls)
	}
}

// The customer cancels while the provider is still working on the charge.
// The cancel wins; when the charge arrives it is refunded.
func TestLiveCancelDuringChargeIsRefunded(t *testing.T) {
	s := start(t, time.Hour)
	s.product("tea", 500, 5)
	o := s.place("hold", domain.Item{SKU: "tea", Qty: 2})
	<-s.pay.holding // the payment handler is now inside the provider call

	code, cancelled := call[domain.Order](s, "POST", "/orders/"+o.ID+"/cancel", nil)
	if code != http.StatusCreated || cancelled.Status != domain.StatusCancelled || s.stock("tea") != 5 {
		t.Fatalf("cancel: %d %+v stock=%d", code, cancelled, s.stock("tea"))
	}
	close(s.pay.release) // the provider now approves the charge

	done := s.until(o.ID, "refunded", func(o domain.Order) bool { return o.Refunded })
	if done.Status != domain.StatusCancelled {
		t.Fatalf("status = %s", done.Status)
	}
	if _, refunds, _ := s.pay.snapshot(); !slices.Equal(refunds, []string{"ch_" + o.ID}) {
		t.Fatalf("refunds = %v", refunds)
	}
	if s.stock("tea") != 5 {
		t.Fatalf("stock = %d", s.stock("tea"))
	}
	// A second cancel is a no-op, not an error.
	if code, _ := call[domain.Order](s, "POST", "/orders/"+o.ID+"/cancel", nil); code != http.StatusCreated {
		t.Fatalf("repeat cancel: %d", code)
	}
}

func TestLivePaidOrderCannotBeCancelled(t *testing.T) {
	s := start(t, time.Hour)
	s.product("tea", 500, 5)
	o := s.place("ok", domain.Item{SKU: "tea", Qty: 1})
	s.until(o.ID, "shipped", is(domain.StatusShipped))
	if code, body := call[errBody](s, "POST", "/orders/"+o.ID+"/cancel", nil); code != http.StatusConflict || body.Error.Code != "conflict" {
		t.Fatalf("%d %+v", code, body)
	}
	if code, _ := call[errBody](s, "GET", "/orders/nope", nil); code != http.StatusNotFound {
		t.Fatalf("unknown order: %d", code)
	}
}

// The provider never answers within the payment window. The scheduled job
// cancels the order and returns the stock.
func TestLiveUnpaidOrderExpires(t *testing.T) {
	s := start(t, 300*time.Millisecond)
	s.product("tea", 500, 5)
	s.pay.down = 1 << 30
	o := s.place("down", domain.Item{SKU: "tea", Qty: 5})
	done := s.until(o.ID, "cancelled", is(domain.StatusCancelled))
	if done.Reason != "payment timed out" || s.stock("tea") != 5 {
		t.Fatalf("reason=%q stock=%d", done.Reason, s.stock("tea"))
	}
}

func TestLiveRetriedRequestPlacesOneOrder(t *testing.T) {
	s := start(t, time.Hour)
	s.product("tea", 500, 5)
	req := domain.PlaceOrder{Customer: "ana", PaymentToken: "ok", Items: []domain.Item{{SKU: "tea", Qty: 2}}}
	_, first := call[domain.Order](s, "POST", "/orders", req, "Idempotency-Key", "cart-1")
	_, again := call[domain.Order](s, "POST", "/orders", req, "Idempotency-Key", "cart-1")
	if first.ID == "" || again.ID != first.ID || s.stock("tea") != 3 {
		t.Fatalf("first=%s again=%s stock=%d", first.ID, again.ID, s.stock("tea"))
	}
}

// Twenty customers want the last five. Exactly five orders are accepted and
// the stock never goes below zero.
func TestLiveConcurrentOrdersNeverOversell(t *testing.T) {
	s := start(t, time.Hour)
	s.product("tea", 500, 5)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var placed []string
	refused := 0
	for range 20 {
		wg.Go(func() {
			raw, _ := json.Marshal(domain.PlaceOrder{Customer: "ana", PaymentToken: "ok", Items: []domain.Item{{SKU: "tea", Qty: 1}}})
			w := httptest.NewRecorder()
			s.http.ServeHTTP(w, httptest.NewRequest("POST", "/orders", bytes.NewReader(raw)))
			mu.Lock()
			defer mu.Unlock()
			switch w.Code {
			case http.StatusCreated:
				var o domain.Order
				_ = json.Unmarshal(w.Body.Bytes(), &o)
				placed = append(placed, o.ID)
			case http.StatusConflict:
				refused++
			default:
				t.Errorf("unexpected %d %s", w.Code, w.Body)
			}
		})
	}
	wg.Wait()
	if len(placed) != 5 || refused != 15 || s.stock("tea") != 0 {
		t.Fatalf("placed=%d refused=%d stock=%d", len(placed), refused, s.stock("tea"))
	}
	for _, id := range placed {
		s.until(id, "shipped", is(domain.StatusShipped))
	}
	if charges, _, _ := s.pay.snapshot(); charges != 5 {
		t.Fatalf("charges = %d", charges)
	}
}
