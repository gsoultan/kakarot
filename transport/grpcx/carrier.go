package grpcx

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Carrier moves context values, such as a trace, across a gRPC call as
// metadata. Inject reads them from the caller's context; Extract puts them
// in the handler's context. With OpenTelemetry:
//
//	trace := grpcx.Carrier{Inject: otelx.Headers, Extract: otelx.WithHeaders}
//
//	unary, stream := trace.Server()
//	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
//
//	cu, cs := trace.Client()
//	conn, err := grpc.NewClient(addr, grpc.WithChainUnaryInterceptor(cu), grpc.WithChainStreamInterceptor(cs))
type Carrier struct {
	Inject  func(ctx context.Context) map[string]string
	Extract func(ctx context.Context, headers map[string]string) context.Context
}

func (c Carrier) outgoing(ctx context.Context) context.Context {
	if c.Inject == nil {
		return ctx
	}
	for k, v := range c.Inject(ctx) {
		ctx = metadata.AppendToOutgoingContext(ctx, k, v)
	}
	return ctx
}

func (c Carrier) incoming(ctx context.Context) context.Context {
	if c.Extract == nil {
		return ctx
	}
	md, _ := metadata.FromIncomingContext(ctx)
	headers := make(map[string]string, len(md))
	for k, vals := range md {
		if len(vals) > 0 {
			headers[k] = vals[0]
		}
	}
	return c.Extract(ctx, headers)
}

// Client returns interceptors that send the carried values with each call.
func (c Carrier) Client() (grpc.UnaryClientInterceptor, grpc.StreamClientInterceptor) {
	unary := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(c.outgoing(ctx), method, req, reply, cc, opts...)
	}
	stream := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(c.outgoing(ctx), desc, cc, method, opts...)
	}
	return unary, stream
}

// Server returns interceptors that restore the carried values before the
// handler and its middleware run.
func (c Carrier) Server() (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return h(c.incoming(ctx), req)
	}
	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		return h(srv, &authStream{ServerStream: ss, ctx: c.incoming(ss.Context())})
	}
	return unary, stream
}
