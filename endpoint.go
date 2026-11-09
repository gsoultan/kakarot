// Package kakarot is a layered Go framework: transport, middleware, endpoint,
// service, usecase and repository, each with a typed contract.
//
// The endpoint is the core primitive. Every operation, whatever transport
// carries it, becomes an Endpoint[Req, Resp], so one middleware works for
// HTTP, gRPC and message consumers alike, and user code never casts.
package kakarot

import (
	"context"
	"fmt"
)

// Endpoint is one RPC-shaped operation, independent of any transport.
// Req and Resp are domain types (the DTOs of the application).
type Endpoint[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)

// Op describes the operation an endpoint serves. Middleware reads it to name
// logs, spans and metrics.
type Op struct {
	Service   string // bounded context, e.g. "order"
	Method    string // operation, e.g. "PlaceOrder"
	Transport string // "http", "grpc", "queue"; empty when not yet bound
}

// String returns "Service.Method".
func (o Op) String() string { return o.Service + "." + o.Method }

// Call is the type-erased form of an endpoint that middleware sees.
//
// Go cannot keep generic functions in a slice, so middleware works on Call
// while users only ever hold typed endpoints. Most middleware (logging,
// tracing, timeout, retry) never looks at req or resp at all.
type Call func(ctx context.Context, req any) (any, error)

// Middleware decorates an endpoint. It must return a Call that preserves the
// dynamic types of req and resp.
type Middleware func(op Op, next Call) Call

// Wrap applies middleware to a typed endpoint and returns a typed endpoint.
// The first middleware is the outermost.
func Wrap[Req, Resp any](op Op, e Endpoint[Req, Resp], mws ...Middleware) Endpoint[Req, Resp] {
	if len(mws) == 0 {
		return e
	}
	call := Call(func(ctx context.Context, req any) (any, error) {
		r, ok := req.(Req)
		if !ok {
			return nil, fmt.Errorf("kakarot: %s: middleware changed request type to %T", op, req)
		}
		return e(ctx, r)
	})
	for _, mw := range reverse(mws) {
		call = mw(op, call)
	}
	return func(ctx context.Context, req Req) (Resp, error) {
		out, err := call(ctx, req)
		resp, ok := out.(Resp)
		if !ok && out != nil {
			var zero Resp
			return zero, fmt.Errorf("kakarot: %s: middleware changed response type to %T", op, out)
		}
		return resp, err
	}
}

// Chain composes middleware into one, outermost first.
func Chain(mws ...Middleware) Middleware {
	return func(op Op, next Call) Call {
		for _, mw := range reverse(mws) {
			next = mw(op, next)
		}
		return next
	}
}

func reverse[T any](s []T) []T {
	out := make([]T, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}

// Usecase is one business operation. Implementations are plain structs whose
// fields are the repository ports they need.
type Usecase[In, Out any] interface {
	Execute(ctx context.Context, in In) (Out, error)
}

// TxManager runs fn inside one transaction. The transaction travels in ctx,
// so a usecase can open one without importing the database library;
// repositories pick it up from ctx. See package data for the storm-backed
// implementation.
type TxManager interface {
	Within(ctx context.Context, fn func(ctx context.Context) error) error
}
