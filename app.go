package kakarot

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Server is anything App can run: an HTTP server, a gRPC server, a consumer.
// Serve blocks until the server stops; Shutdown stops it gracefully.
type Server interface {
	Serve(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// App runs servers together and shuts them all down on SIGINT, SIGTERM, a
// cancelled context, or the first server to fail.
type App struct {
	Name            string
	Servers         []Server
	Logger          *slog.Logger
	ShutdownTimeout time.Duration // default 15s
	OnStop          []func(ctx context.Context) error
}

// Run blocks until the app stops and returns the first error that stopped it.
func (a *App) Run(ctx context.Context) error {
	log := a.Logger
	if log == nil {
		log = slog.Default()
	}
	timeout := a.ShutdownTimeout
	if timeout == 0 {
		timeout = 15 * time.Second
	}

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancelCause(sigCtx)
	defer cancel(nil)

	var wg sync.WaitGroup
	for _, s := range a.Servers {
		wg.Go(func() {
			if err := s.Serve(ctx); err != nil && !errors.Is(err, context.Canceled) {
				cancel(err)
			}
		})
	}
	log.Info("app started", "app", a.Name, "servers", len(a.Servers))

	<-ctx.Done()
	cause := context.Cause(ctx)
	log.Info("app stopping", "app", a.Name, "cause", cause)

	sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer scancel()
	var errs []error
	for _, s := range a.Servers {
		errs = append(errs, s.Shutdown(sctx))
	}
	wg.Wait()
	for _, f := range a.OnStop {
		errs = append(errs, f(sctx))
	}

	// A signal or a cancelled parent is a clean stop; a failed server is not.
	if sigCtx.Err() == nil && cause != nil && !errors.Is(cause, context.Canceled) {
		errs = append([]error{cause}, errs...)
	}
	return errors.Join(errs...)
}
