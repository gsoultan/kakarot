// Package kakarot is a layered Go framework: transport, middleware, endpoint,
// service, usecase and repository, each with a typed contract.
//
// The endpoint is the core primitive. Every operation, whatever transport
// carries it, becomes an Endpoint[Req, Resp], so one middleware works for
// HTTP, gRPC and message consumers alike. The public API has no `any`:
// shared middleware never sees the request, and typed middleware is checked
// by the compiler.
package kakarot

import "context"

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

// Next continues the call chain. It is a closure over the typed request and
// response, so shared middleware can run code around a call, change ctx,
// retry, or short-circuit with an error, without ever seeing an untyped value.
type Next func(ctx context.Context) error

// Middleware is shared, type-agnostic endpoint middleware (logging, tracing,
// timeout, recover, retry, rate limiting). It works for every endpoint
// because it never touches Req or Resp; middleware that needs them is a
// TypedMiddleware instead.
type Middleware func(ctx context.Context, op Op, next Next) error

// TypedMiddleware decorates one endpoint type and may read or replace the
// request and response, checked at compile time.
type TypedMiddleware[Req, Resp any] func(op Op, next Endpoint[Req, Resp]) Endpoint[Req, Resp]

// Wrap applies shared middleware to a typed endpoint and returns a typed
// endpoint. The first middleware is the outermost.
func Wrap[Req, Resp any](op Op, e Endpoint[Req, Resp], mws ...Middleware) Endpoint[Req, Resp] {
	if len(mws) == 0 {
		return e
	}
	return func(ctx context.Context, req Req) (Resp, error) {
		var resp Resp
		next := Next(func(ctx context.Context) error {
			var err error
			resp, err = e(ctx, req)
			return err
		})
		for i := len(mws) - 1; i >= 0; i-- {
			mw, inner := mws[i], next
			next = func(ctx context.Context) error { return mw(ctx, op, inner) }
		}
		err := next(ctx)
		return resp, err
	}
}

// Decorate applies typed middleware to an endpoint, outermost first.
func Decorate[Req, Resp any](op Op, e Endpoint[Req, Resp], mws ...TypedMiddleware[Req, Resp]) Endpoint[Req, Resp] {
	for i := len(mws) - 1; i >= 0; i-- {
		e = mws[i](op, e)
	}
	return e
}

// Chain composes shared middleware into one, outermost first.
func Chain(mws ...Middleware) Middleware {
	return func(ctx context.Context, op Op, next Next) error {
		for i := len(mws) - 1; i >= 0; i-- {
			mw, inner := mws[i], next
			next = func(ctx context.Context) error { return mw(ctx, op, inner) }
		}
		return next(ctx)
	}
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
