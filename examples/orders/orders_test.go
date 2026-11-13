package orders_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
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
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/transport/grpcx"
	"github.com/gsoultan/kakarot/transport/httpx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
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

type memNotifier struct{ orders []string }

func (n *memNotifier) OrderPlaced(_ context.Context, o domain.Order) error {
	n.orders = append(n.orders, o.ID)
	return nil
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newEndpoints() (endpoint.OrderEndpoints, *noTx, *memNotifier) {
	repo := &memRepo{orders: map[string]domain.Order{}}
	tx := &noTx{}
	notifier := &memNotifier{}
	svc := service.New(service.Usecases{
		Place:  &usecase.PlaceOrder{Orders: repo, Events: &memEvents{}, Tx: tx},
		Get:    &usecase.GetOrder{Orders: repo},
		List:   &usecase.ListOrders{Orders: repo},
		Export: (&usecase.ExportOrders{Orders: repo}).Stream,
		Notify: &usecase.NotifyOrderPlaced{Notifier: notifier},
	})
	return endpoint.MakeOrderEndpoints(svc), tx, notifier
}

func newServer(t *testing.T) (http.Handler, *noTx) {
	t.Helper()
	eps, tx, _ := newEndpoints()
	r := httpx.NewRouter()
	transport.RegisterOrderHTTP(r, eps, middleware.Default(quiet, time.Second))
	return r, tx
}

// The same endpoints over gRPC, called through the generated client: the
// client is an OrderEndpoints too, so callers cannot tell local from remote.
func TestGRPCRoundTrip(t *testing.T) {
	eps, _, _ := newEndpoints()
	gs := grpc.NewServer()
	transport.RegisterOrderGRPC(gs, eps, middleware.Default(quiet, time.Second))
	lis := bufconn.Listen(1 << 20)
	srv := &grpcx.Server{GRPC: gs, Listener: lis}
	go srv.Serve(t.Context())
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := transport.NewOrderGRPCClient(conn)

	placed, err := client.PlaceOrder(t.Context(), domain.PlaceOrder{CustomerID: "c-9", Items: []domain.LineItem{{SKU: "pen", Quantity: 2}}})
	if err != nil || placed.ID != "o-1" || placed.Status != domain.StatusPlaced {
		t.Fatalf("place: %+v %v", placed, err)
	}
	got, err := client.GetOrder(t.Context(), domain.GetOrder{ID: placed.ID})
	if err != nil || got.CustomerID != "c-9" || len(got.Items) != 1 {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := client.GetOrder(t.Context(), domain.GetOrder{ID: "nope"}); kakarot.CodeOf(err) != kakarot.CodeNotFound {
		t.Fatalf("missing: %v", err)
	}
	_, err = client.PlaceOrder(t.Context(), domain.PlaceOrder{})
	if e := kakarot.AsError(err); e.Code != kakarot.CodeInvalid {
		t.Fatalf("validation must run for gRPC too: %v", err)
	}
	page, err := client.ListOrders(t.Context(), domain.ListOrders{CustomerID: "c-9"})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("list: %+v %v", page, err)
	}
	all, err := kakarot.Collect(client.ExportOrders(t.Context(), domain.ExportOrders{CustomerID: "c-9"}))
	if err != nil || len(all) != 1 || all[0].ID != "o-1" {
		t.Fatalf("export: %+v %v", all, err)
	}
}

// An order.placed message reaches the usecase through the generated queue
// binding.
func TestQueueConsumer(t *testing.T) {
	eps, _, notifier := newEndpoints()
	broker := &queue.Memory{}
	c := &queue.Consumer{Service: "order", Source: broker, Logger: quiet}
	transport.RegisterOrderQueue(c, eps, middleware.Default(quiet, time.Second))
	done := make(chan error, 1)
	go func() { done <- c.Serve(context.Background()) }()
	for broker.Subscribers("order.placed") == 0 {
		time.Sleep(time.Millisecond)
	}
	m, err := transport.NotifyOrderPlacedTopic.Encode("o-7", domain.Order{ID: "o-7", CustomerID: "c-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if len(notifier.orders) != 1 || notifier.orders[0] != "o-7" {
		t.Fatalf("notified = %v", notifier.orders)
	}
	if err := c.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
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
