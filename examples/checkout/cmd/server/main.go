// Command server runs the checkout sample on :8080.
//
//	export STORM_DSN=postgres://user:pass@localhost:5432/checkout
//	AUTO_MIGRATE=1 go run ./cmd/server
//
// AUTO_MIGRATE applies the schema at startup (local development only); use
// `storm diff` to write reviewed migrations otherwise.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/checkout/app"
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
	if os.Getenv("AUTO_MIGRATE") == "1" {
		s, err := storm.Build(app.Models()...)
		if err != nil {
			return err
		}
		if _, err := migrate.AutoPool(ctx, pool, s, migrate.AutoOptions{}); err != nil {
			return err
		}
	}

	// PAYMENT_WINDOW shortens the expiry for a demo, for example 20s.
	window, _ := time.ParseDuration(os.Getenv("PAYMENT_WINDOW"))
	c := app.Build(app.Config{
		DB:            pgxdrv.Pool{P: pool},
		Logger:        log,
		Payments:      &app.DemoPayments{Log: log},
		Warehouse:     app.DemoWarehouse{},
		Notifier:      app.LogNotifier{Log: log},
		PaymentWindow: window,
	})
	return (&kakarot.App{
		Name:    "checkout",
		Logger:  log,
		Servers: c.Servers(":8080"),
		OnStop:  []func(context.Context) error{func(context.Context) error { pool.Close(); return nil }},
	}).Run(ctx)
}
