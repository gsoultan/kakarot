// Package middleware holds endpoint middleware. Shared middleware
// (kakarot.Middleware) works for every endpoint and transport without seeing
// the request; Validate is a typed decorator checked at compile time.
package middleware

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/gsoultan/kakarot"
)

// Recover turns a panic in an endpoint into a CodeInternal error and logs the
// stack.
func Recover(log *slog.Logger) kakarot.Middleware {
	return func(ctx context.Context, op kakarot.Op, next kakarot.Next) (err error) {
		defer func() {
			if p := recover(); p != nil {
				log.ErrorContext(ctx, "panic", "op", op.String(), "panic", p, "stack", string(debug.Stack()))
				err = kakarot.E(kakarot.CodeInternal, "internal error", fmt.Errorf("panic: %v", p))
			}
		}()
		return next(ctx)
	}
}

// Logging logs each call with its duration and error code.
func Logging(log *slog.Logger) kakarot.Middleware {
	return func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		start := time.Now()
		err := next(ctx)
		attrs := []any{"op", op.String(), "transport", op.Transport, "took", time.Since(start)}
		if err != nil {
			attrs = append(attrs, "code", kakarot.CodeOf(err).String(), "err", err)
			log.WarnContext(ctx, "call failed", attrs...)
		} else {
			log.InfoContext(ctx, "call", attrs...)
		}
		return err
	}
}

// Timeout bounds each call. A call that runs past d returns CodeTimeout.
func Timeout(d time.Duration) kakarot.Middleware {
	return func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		err := next(ctx)
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			return kakarot.E(kakarot.CodeTimeout, op.String()+" timed out", err)
		}
		return err
	}
}

// Default is the recommended shared stack: recover, logging, timeout.
func Default(log *slog.Logger, timeout time.Duration) kakarot.Middleware {
	return kakarot.Chain(Recover(log), Logging(log), Timeout(timeout))
}

// Validatable is implemented by request types that check themselves.
// Return a *kakarot.Error with CodeInvalid and Fields for per-field problems;
// any other error is wrapped as CodeInvalid.
type Validatable interface {
	Validate() error
}

// Validate is typed middleware: it only compiles for endpoints whose request
// implements Validatable.
//
//	ep = kakarot.Decorate(op, ep, middleware.Validate[domain.PlaceOrder, domain.Order]())
//
// or, with inference, middleware.Validated(ep).
func Validate[Req Validatable, Resp any]() kakarot.TypedMiddleware[Req, Resp] {
	return func(_ kakarot.Op, next kakarot.Endpoint[Req, Resp]) kakarot.Endpoint[Req, Resp] {
		return Validated(next)
	}
}

// Validated wraps an endpoint so its request is validated first. Req and
// Resp are inferred from the endpoint.
func Validated[Req Validatable, Resp any](next kakarot.Endpoint[Req, Resp]) kakarot.Endpoint[Req, Resp] {
	return func(ctx context.Context, req Req) (Resp, error) {
		if err := req.Validate(); err != nil {
			var zero Resp
			if _, ok := errors.AsType[*kakarot.Error](err); ok {
				return zero, err
			}
			return zero, kakarot.E(kakarot.CodeInvalid, err.Error(), err)
		}
		return next(ctx, req)
	}
}
