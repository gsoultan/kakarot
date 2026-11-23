package connectx

import (
	"context"
	"net/http"
	"strings"

	"connectrpc.com/connect"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/auth"
)

// Bearer returns a handler interceptor that verifies the "Authorization:
// Bearer <token>" header and puts the principal in the context, exactly as
// auth.Bearer does for HTTP and grpcx.Bearer for gRPC: the same usecases
// and auth.Require then work on every transport. Calls without a token pass
// through unauthenticated.
//
//	mux.Handle(svc.Handler(connect.WithInterceptors(connectx.Bearer(verify))))
func Bearer[P any](verify auth.Verifier[P]) connect.Interceptor {
	authenticate := func(ctx context.Context, h http.Header) (context.Context, error) {
		token, _ := strings.CutPrefix(h.Get("Authorization"), "Bearer ")
		if token == "" {
			return ctx, nil
		}
		p, err := verify(ctx, token)
		if err != nil {
			if kakarot.CodeOf(err) == kakarot.CodeInternal {
				err = kakarot.E(kakarot.CodeUnauthorized, "invalid credentials", err)
			}
			return ctx, Error(err)
		}
		return auth.With(ctx, p), nil
	}
	return interceptor{
		unary: func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				if req.Spec().IsClient {
					return next(ctx, req)
				}
				ctx, err := authenticate(ctx, req.Header())
				if err != nil {
					return nil, err
				}
				return next(ctx, req)
			}
		},
		handler: func(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
			return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
				ctx, err := authenticate(ctx, conn.RequestHeader())
				if err != nil {
					return err
				}
				return next(ctx, conn)
			}
		},
	}
}

// Carrier moves context values, such as a trace, across a Connect call as
// HTTP headers. Inject reads them from the caller's context; Extract puts
// them in the handler's context. With OpenTelemetry:
//
//	trace := connectx.Carrier{Inject: otelx.Headers, Extract: otelx.WithHeaders}
//	mux.Handle(svc.Handler(connect.WithInterceptors(trace.Interceptor())))
//	get := connectx.Invoke[GetOrder, Order](hc, url, svc, "GetOrder", connect.WithInterceptors(trace.Interceptor()))
type Carrier struct {
	Inject  func(ctx context.Context) map[string]string
	Extract func(ctx context.Context, headers map[string]string) context.Context
}

func (c Carrier) outgoing(ctx context.Context, h http.Header) {
	if c.Inject == nil {
		return
	}
	for k, v := range c.Inject(ctx) {
		h.Set(k, v)
	}
}

func (c Carrier) incoming(ctx context.Context, h http.Header) context.Context {
	if c.Extract == nil {
		return ctx
	}
	headers := make(map[string]string, len(h))
	for k, vals := range h {
		if len(vals) > 0 {
			headers[strings.ToLower(k)] = vals[0]
		}
	}
	return c.Extract(ctx, headers)
}

// Interceptor returns one interceptor for both sides: on a client it sends
// the carried values with each call, on a handler it restores them before
// the endpoint and its middleware run.
func (c Carrier) Interceptor() connect.Interceptor {
	return interceptor{
		unary: func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				if req.Spec().IsClient {
					c.outgoing(ctx, req.Header())
					return next(ctx, req)
				}
				return next(c.incoming(ctx, req.Header()), req)
			}
		},
		client: func(next connect.StreamingClientFunc) connect.StreamingClientFunc {
			return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
				conn := next(ctx, spec)
				c.outgoing(ctx, conn.RequestHeader())
				return conn
			}
		},
		handler: func(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
			return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
				return next(c.incoming(ctx, conn.RequestHeader()), conn)
			}
		},
	}
}

// interceptor implements connect.Interceptor from optional parts.
type interceptor struct {
	unary   func(connect.UnaryFunc) connect.UnaryFunc
	client  func(connect.StreamingClientFunc) connect.StreamingClientFunc
	handler func(connect.StreamingHandlerFunc) connect.StreamingHandlerFunc
}

func (i interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	if i.unary == nil {
		return next
	}
	return i.unary(next)
}

func (i interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	if i.client == nil {
		return next
	}
	return i.client(next)
}

func (i interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	if i.handler == nil {
		return next
	}
	return i.handler(next)
}
