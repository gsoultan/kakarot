package orders_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/endpoint"
	"github.com/gsoultan/kakarot/examples/orders/service"
	"github.com/gsoultan/kakarot/examples/orders/transport"
	"github.com/gsoultan/kakarot/examples/orders/usecase"
	"github.com/gsoultan/kakarot/kakarottest"
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

type memNotifier struct{ orders []string }

func (n *memNotifier) OrderPlaced(_ context.Context, o domain.Order) error {
	n.orders = append(n.orders, o.ID)
	return nil
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newEndpoints() (endpoint.OrderEndpoints, *kakarottest.Tx, *memNotifier) {
	repo := &memRepo{orders: map[string]domain.Order{}}
	tx := &kakarottest.Tx{}
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

func newServer(t *testing.T) (http.Handler, *kakarottest.Tx) {
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
	kakarottest.Serve(t, srv)
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
	_, err = client.GetOrder(t.Context(), domain.GetOrder{ID: "nope"})
	kakarottest.WantCode(t, err, kakarot.CodeNotFound)
	_, err = client.PlaceOrder(t.Context(), domain.PlaceOrder{})
	kakarottest.WantCode(t, err, kakarot.CodeInvalid) // validation runs for gRPC too
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
	kakarottest.Serve(t, c)

	err := kakarottest.Publish(t, broker, transport.NotifyOrderPlacedTopic, "o-7", domain.Order{ID: "o-7", CustomerID: "c-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(notifier.orders) != 1 || notifier.orders[0] != "o-7" {
		t.Fatalf("notified = %v", notifier.orders)
	}
}

const pens = `{"customer_id":"c-9","items":[{"sku":"pen","quantity":2}]}`

func TestPlaceAndGetOrder(t *testing.T) {
	h, tx := newServer(t)

	placed := kakarottest.Do[domain.Order](t, h, "POST", "/orders", pens)
	if placed.Status != http.StatusCreated {
		t.Fatalf("place: %d %s", placed.Status, placed.Raw)
	}
	if tx.Commits() != 1 {
		t.Fatalf("commits = %d", tx.Commits())
	}

	got := kakarottest.Do[domain.Order](t, h, "GET", "/orders/o-1", nil)
	if got.Status != http.StatusOK {
		t.Fatalf("get: %d %s", got.Status, got.Raw)
	}
	if o := got.Body; o.CustomerID != "c-9" || o.Status != domain.StatusPlaced || len(o.Items) != 1 {
		t.Fatalf("got %+v", o)
	}
}

func TestPlaceOrderValidation(t *testing.T) {
	h, tx := newServer(t)
	res := kakarottest.Do[domain.Order](t, h, "POST", "/orders", `{"items":[]}`)
	if res.Status != http.StatusBadRequest || res.Error.Fields["customer_id"] == "" {
		t.Fatalf("%d %s", res.Status, res.Raw)
	}
	if tx.Commits()+tx.Rollbacks() != 0 {
		t.Fatal("usecase ran despite invalid input")
	}
}

func TestGetMissingOrder(t *testing.T) {
	h, _ := newServer(t)
	res := kakarottest.Do[domain.Order](t, h, "GET", "/orders/nope", nil)
	if res.Status != http.StatusNotFound || res.Error.Code != "not_found" {
		t.Fatalf("%d %s", res.Status, res.Raw)
	}
}

func TestListAndExport(t *testing.T) {
	h, _ := newServer(t)
	kakarottest.Do[domain.Order](t, h, "POST", "/orders", pens)

	if res := kakarottest.Do[kakarottest.None](t, h, "GET", "/orders", nil); res.Status != http.StatusBadRequest {
		t.Fatalf("list without customer: %d", res.Status)
	}
	page := kakarottest.Do[kakarot.Page[domain.Order]](t, h, "GET", "/orders?customer_id=c-9&limit=10", nil)
	if page.Status != http.StatusOK || len(page.Body.Items) != 1 || page.Body.Items[0].ID != "o-1" {
		t.Fatalf("%d %s", page.Status, page.Raw)
	}
	// The export is a stream: one JSON object per line.
	export := kakarottest.Do[kakarottest.None](t, h, "GET", "/orders/export?customer_id=c-9", nil)
	if export.Status != http.StatusOK || strings.Count(string(export.Raw), "\n") != 1 {
		t.Fatalf("%d %q", export.Status, export.Raw)
	}
}
