// Command server runs the orders example against PostgreSQL.
//
//	export STORM_DSN=postgres://user:pass@localhost:5432/orders
//	go run ./cmd/server   # HTTP on :8080, gRPC on :9090
//
// Schema: `storm diff create_orders` writes the migration to review and apply;
// AUTO_MIGRATE=1 applies it at startup instead (local development only).
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gsoultan/kakarot"
	kdata "github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/kakarot/data/idempotency"
	idemmodel "github.com/gsoultan/kakarot/data/idempotency/model"
	"github.com/gsoultan/kakarot/data/outbox"
	outboxmodel "github.com/gsoultan/kakarot/data/outbox/model"
	"github.com/gsoultan/kakarot/examples/orders/data"
	"github.com/gsoultan/kakarot/examples/orders/data/model"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/endpoint"
	"github.com/gsoultan/kakarot/examples/orders/service"
	"github.com/gsoultan/kakarot/examples/orders/transport"
	"github.com/gsoultan/kakarot/examples/orders/usecase"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/kakarot/otelx"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/transport/grpcx"
	"github.com/gsoultan/kakarot/transport/httpx"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/migrate"
	"github.com/gsoultan/storm/runtime/pgxdrv"
	"google.golang.org/grpc"
)

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	pool, err := pgxdrv.NewPool(ctx, os.Getenv("STORM_DSN"))
	if err != nil {
		return err
	}
	db := pgxdrv.Pool{P: pool}

	if os.Getenv("AUTO_MIGRATE") == "1" {
		s, err := storm.Build(&model.Order{}, &model.OrderItem{}, &outboxmodel.OutboxEvent{}, &idemmodel.IdempotencyRecord{})
		if err != nil {
			return err
		}
		if _, err := migrate.AutoPool(ctx, pool, s, migrate.AutoOptions{}); err != nil {
			return err
		}
	}

	// Wiring stays explicit Go; kakarot gen writes the endpoint and transport layers.
	repo := &data.OrderRepo{DB: db}
	svc := service.New(service.Usecases{
		Place:  &usecase.PlaceOrder{Orders: repo, Events: &data.Events{DB: db}, Tx: kdata.NewTxManager(db)},
		Get:    &usecase.GetOrder{Orders: repo},
		List:   &usecase.ListOrders{Orders: repo},
		Export: (&usecase.ExportOrders{Orders: repo}).Stream,
		Notify: &usecase.NotifyOrderPlaced{Notifier: logNotifier{log}},
	})

	// One set of typed endpoints serves HTTP, gRPC and the queue consumer.
	eps := endpoint.MakeOrderEndpoints(svc)
	// A repeated Idempotency-Key replays the stored order, on any instance.
	eps.PlaceOrder = kakarot.Decorate(kakarot.Op{Service: "order", Method: "PlaceOrder"}, eps.PlaceOrder,
		middleware.Idempotent[domain.PlaceOrder](&idempotency.Store[domain.Order]{DB: db}))

	// Shed load first, then trace, recover, log and bound every call.
	shed := middleware.LoadShed(512)
	mws := kakarot.Chain(shed, otelx.Middleware(), middleware.Default(log, 5*time.Second))

	r := httpx.NewRouter()
	r.Use(otelx.HTTP())
	r.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	transport.RegisterOrderHTTP(r, eps, mws)
	r.ServeOpenAPI("GET /openapi.json", httpx.Info{Title: "orders", Version: "0.1.0"})

	gs := grpc.NewServer()
	transport.RegisterOrderGRPC(gs, eps, mws)

	// Events: the outbox relay publishes committed events to the broker, and
	// the consumer runs them through the same endpoints. queue.Memory keeps
	// it in one process; swap in your broker's client for more.
	broker := &queue.Memory{}
	relay := &outbox.Relay{DB: db, Logger: log, Publisher: outbox.ToQueue(broker)}
	consumer := &queue.Consumer{Service: "order", Source: broker, Logger: log}
	transport.RegisterOrderQueue(consumer, eps, kakarot.Chain(otelx.Middleware(), middleware.Default(log, 30*time.Second)))

	app := &kakarot.App{
		Name:   "orders",
		Logger: log,
		Servers: []kakarot.Server{
			httpx.NewServer(":8080", r),
			&grpcx.Server{GRPC: gs, Addr: ":9090"},
			consumer,
			relay,
		},
		OnStop: []func(context.Context) error{func(context.Context) error { pool.Close(); return nil }},
	}
	return app.Run(ctx)
}

// logNotifier stands in for an email or push service.
type logNotifier struct{ log *slog.Logger }

func (n logNotifier) OrderPlaced(ctx context.Context, o domain.Order) error {
	n.log.InfoContext(ctx, "notify customer", "order", o.ID, "customer", o.CustomerID)
	return nil
}
