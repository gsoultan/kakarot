// Command server runs the orders example against PostgreSQL.
//
//	export STORM_DSN=postgres://user:pass@localhost:5432/orders
//	go run ./examples/orders/cmd/server
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
	"github.com/gsoultan/kakarot/data/outbox"
	outboxmodel "github.com/gsoultan/kakarot/data/outbox/model"
	"github.com/gsoultan/kakarot/examples/orders/data"
	"github.com/gsoultan/kakarot/examples/orders/data/model"
	"github.com/gsoultan/kakarot/examples/orders/endpoint"
	"github.com/gsoultan/kakarot/examples/orders/service"
	"github.com/gsoultan/kakarot/examples/orders/transport"
	"github.com/gsoultan/kakarot/examples/orders/usecase"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/kakarot/transport/httpx"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/migrate"
	"github.com/gsoultan/storm/runtime/pgxdrv"
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
		s, err := storm.Build(&model.Order{}, &model.OrderItem{}, &outboxmodel.OutboxEvent{})
		if err != nil {
			return err
		}
		if _, err := migrate.AutoPool(ctx, pool, s, migrate.AutoOptions{}); err != nil {
			return err
		}
	}

	// Wiring by hand; `kakarot gen` will generate this in milestone 2.
	repo := &data.OrderRepo{DB: db}
	svc := service.New(service.Usecases{
		Place:  &usecase.PlaceOrder{Orders: repo, Events: &data.Events{DB: db}, Tx: kdata.NewTxManager(db)},
		Get:    &usecase.GetOrder{Orders: repo},
		List:   &usecase.ListOrders{Orders: repo},
		Export: (&usecase.ExportOrders{Orders: repo}).Stream,
	})

	// Publishes outbox events; replace the logger with your broker client.
	relay := &outbox.Relay{DB: db, Logger: log, Publisher: outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
		log.InfoContext(ctx, "event", "topic", m.Topic, "id", m.ID, "key", m.Key)
		return nil
	})}

	r := httpx.NewRouter()
	r.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	transport.RegisterHTTP(r, endpoint.Make(svc), middleware.Default(log, 5*time.Second))

	app := &kakarot.App{
		Name:    "orders",
		Logger:  log,
		Servers: []kakarot.Server{httpx.NewServer(":8080", r), relay},
		OnStop:  []func(context.Context) error{func(context.Context) error { pool.Close(); return nil }},
	}
	return app.Run(ctx)
}
