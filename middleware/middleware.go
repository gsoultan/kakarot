// Package middleware holds endpoint middleware. Each works for every
// transport, because it wraps kakarot endpoints rather than HTTP handlers.
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
	return func(op kakarot.Op, next kakarot.Call) kakarot.Call {
		return func(ctx context.Context, req any) (resp any, err error) {
			defer func() {
				if p := recover(); p != nil {
					log.ErrorContext(ctx, "panic", "op", op.String(), "panic", p, "stack", string(debug.Stack()))
					resp, err = nil, kakarot.E(kakarot.CodeInternal, "internal error", fmt.Errorf("panic: %v", p))
				}
			}()
			return next(ctx, req)
		}
	}
}

// Logging logs each call with its duration and error code.
func Logging(log *slog.Logger) kakarot.Middleware {
	return func(op kakarot.Op, next kakarot.Call) kakarot.Call {
		return func(ctx context.Context, req any) (any, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			attrs := []any{"op", op.String(), "transport", op.Transport, "took", time.Since(start)}
			if err != nil {
				attrs = append(attrs, "code", kakarot.CodeOf(err).String(), "err", err)
				log.WarnContext(ctx, "call failed", attrs...)
			} else {
				log.InfoContext(ctx, "call", attrs...)
			}
			return resp, err
		}
	}
}

// Timeout bounds each call. A call that runs past d returns CodeTimeout.
func Timeout(d time.Duration) kakarot.Middleware {
	return func(op kakarot.Op, next kakarot.Call) kakarot.Call {
		return func(ctx context.Context, req any) (any, error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			resp, err := next(ctx, req)
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				return resp, kakarot.E(kakarot.CodeTimeout, op.String()+" timed out", err)
			}
			return resp, err
		}
	}
}

// Validatable is implemented by request types that check themselves.
// Return a *kakarot.Error with CodeInvalid and Fields for per-field problems;
// any other error is wrapped as CodeInvalid.
type Validatable interface {
	Validate() error
}

// Validate calls req.Validate() when the request implements Validatable.
func Validate() kakarot.Middleware {
	return func(op kakarot.Op, next kakarot.Call) kakarot.Call {
		return func(ctx context.Context, req any) (any, error) {
			if v, ok := req.(Validatable); ok {
				if err := v.Validate(); err != nil {
					if _, ok := errors.AsType[*kakarot.Error](err); ok {
						return nil, err
					}
					return nil, kakarot.E(kakarot.CodeInvalid, err.Error(), err)
				}
			}
			return next(ctx, req)
		}
	}
}

// Default is the recommended stack: recover, logging, timeout, validate.
func Default(log *slog.Logger, timeout time.Duration) kakarot.Middleware {
	return kakarot.Chain(Recover(log), Logging(log), Timeout(timeout), Validate())
}
