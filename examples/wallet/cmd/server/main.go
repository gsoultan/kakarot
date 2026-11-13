// Command server runs the wallet sample: HTTP on :8080, gRPC on :9090.
//
//	export STORM_DSN=postgres://user:pass@localhost:5432/wallet
//	export WALLET_TOKENS=alice-secret=alice,bob-secret=bob
//	AUTO_MIGRATE=1 go run ./cmd/server
//
// AUTO_MIGRATE applies the schema at startup (local development only); use
// `storm diff` to write reviewed migrations otherwise.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/wallet/app"
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
	tokens := app.ParseTokens(os.Getenv("WALLET_TOKENS"))
	if len(tokens) == 0 {
		return errors.New("WALLET_TOKENS is empty: nobody could sign in (format token=user,token=user)")
	}
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

	w := app.Build(app.Config{DB: pgxdrv.Pool{P: pool}, Logger: log, Tokens: tokens, Audit: app.LogAudit{Log: log}})
	return (&kakarot.App{
		Name:    "wallet",
		Logger:  log,
		Servers: w.Servers(":8080", ":9090"),
		OnStop:  []func(context.Context) error{func(context.Context) error { pool.Close(); return nil }},
	}).Run(ctx)
}
