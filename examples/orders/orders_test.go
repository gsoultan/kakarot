package orders_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/endpoint"
	"github.com/gsoultan/kakarot/examples/orders/service"
	"github.com/gsoultan/kakarot/examples/orders/transport"
	"github.com/gsoultan/kakarot/examples/orders/usecase"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/kakarot/transport/httpx"
)

// memRepo and noTx stand in for storm, showing that usecases test without a
// database because they depend only on ports.
type memRepo struct{ orders map[string]domain.Order }

func (m *memRepo) Create(_ context.Context, o *domain.Order) error {
	o.ID = "o-1"
	o.CreatedAt = time.Unix(0, 0).UTC()
	m.orders[o.ID] = *o
	return nil
}

func (m *memRepo) Get(_ context.Context, id string) (domain.Order, error) {
	o, ok := m.orders[id]
	if !ok {
		return domain.Order{}, kakarot.NotFound("order", id)
	}
	return o, nil
}

func (m *memRepo) ListByCustomer(_ context.Context, customerID, cursor string, limit int) (kakarot.Page[domain.Order], error) {
	var out []domain.Order
	for _, o := range m.orders {
		if o.CustomerID == customerID {
			out = append(out, o)
		}
	}
	return kakarot.Page[domain.Order]{Items: out}, nil
}

type memEvents struct{ placed []domain.Order }

func (e *memEvents) OrderPlaced(_ context.Context, o domain.Order) error {
	e.placed = append(e.placed, o)
	return nil
}

type noTx struct{ calls int }

func (t *noTx) Within(ctx context.Context, fn func(context.Context) error) error {
	t.calls++
	return fn(ctx)
}

func newServer(t *testing.T) (http.Handler, *noTx) {
	t.Helper()
	repo := &memRepo{orders: map[string]domain.Order{}}
	tx := &noTx{}
	svc := service.New(service.Usecases{
		Place:  &usecase.PlaceOrder{Orders: repo, Events: &memEvents{}, Tx: tx},
		Get:    &usecase.GetOrder{Orders: repo},
		List:   &usecase.ListOrders{Orders: repo},
		Export: (&usecase.ExportOrders{Orders: repo}).Stream,
	})
	r := httpx.NewRouter()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	transport.RegisterHTTP(r, endpoint.Make(svc), middleware.Default(log, time.Second))
	return r, tx
}

func TestPlaceAndGetOrder(t *testing.T) {
	h, tx := newServer(t)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/orders",
		strings.NewReader(`{"customer_id":"c-9","items":[{"sku":"pen","quantity":2}]}`)))
	if w.Code != http.StatusCreated {
		t.Fatalf("place: %d %s", w.Code, w.Body)
	}
	if tx.calls != 1 {
		t.Fatalf("tx calls = %d", tx.calls)
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/orders/o-1", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("get: %d %s", w.Code, w.Body)
	}
	var got domain.Order
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.CustomerID != "c-9" || got.Status != domain.StatusPlaced || len(got.Items) != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestPlaceOrderValidation(t *testing.T) {
	h, tx := newServer(t)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/orders", strings.NewReader(`{"items":[]}`)))
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "customer_id") {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if tx.calls != 0 {
		t.Fatal("usecase ran despite invalid input")
	}
}

func TestGetMissingOrder(t *testing.T) {
	h, _ := newServer(t)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/orders/nope", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestListAndExport(t *testing.T) {
	h, _ := newServer(t)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/orders",
		strings.NewReader(`{"customer_id":"c-9","items":[{"sku":"pen","quantity":2}]}`)))

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/orders", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("list without customer: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/orders?customer_id=c-9&limit=10", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"items":[{"id":"o-1"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/orders/export?customer_id=c-9", nil))
	if w.Code != 200 || strings.Count(w.Body.String(), "\n") != 1 {
		t.Fatalf("%d %q", w.Code, w.Body)
	}
}
