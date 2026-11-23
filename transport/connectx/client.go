package connectx

import (
	"context"
	"errors"
	"io"
	"iter"
	"net/http"
	"strings"

	"connectrpc.com/connect"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/transport/grpcx"
)

// Clients speak the Connect protocol with JSON by default. Pass Protobuf()
// to send protobuf, and connect.WithGRPC() or connect.WithGRPCWeb() to
// speak gRPC or gRPC-Web: a client with connect.WithGRPC() also calls an
// interface-first grpcx server, since both use the same JSON and protobuf.
//
//	get := connectx.Invoke[GetOrder, Order](http.DefaultClient, "https://orders.internal", "order.v1.OrderService", "GetOrder")
//	o, err := get(ctx, GetOrder{ID: "o-1"})

// Protobuf makes a client send and expect protobuf, in the layout
// Service.Proto writes, instead of JSON.
func Protobuf() connect.ClientOption { return connect.WithCodec(grpcx.ProtoCodec()) }

func newClient[Req, Resp any](hc connect.HTTPClient, baseURL, service, method string, opts []connect.ClientOption) *connect.Client[Req, Resp] {
	opts = append([]connect.ClientOption{connect.WithCodec(jsonCodec{"json"})}, opts...)
	return connect.NewClient[Req, Resp](hc, strings.TrimRight(baseURL, "/")+procedure(service, method), opts...)
}

// Invoke returns a client endpoint for a unary procedure: calling it
// performs the RPC. It has the same type as the server's endpoint, so
// client-side middleware (Retry, CircuitBreaker, Timeout) wraps it with
// kakarot.Wrap.
func Invoke[Req, Resp any](hc connect.HTTPClient, baseURL, service, method string, opts ...connect.ClientOption) kakarot.Endpoint[Req, Resp] {
	c := newClient[Req, Resp](hc, baseURL, service, method, opts)
	return func(ctx context.Context, req Req) (Resp, error) {
		r := connect.NewRequest(&req)
		outgoing(ctx, r.Header())
		resp, err := c.CallUnary(ctx, r)
		if err != nil {
			var zero Resp
			return zero, FromError(err)
		}
		return *resp.Msg, nil
	}
}

// Receive returns a client for a server-streaming procedure.
func Receive[Req, Msg any](hc connect.HTTPClient, baseURL, service, method string, opts ...connect.ClientOption) kakarot.ServerStream[Req, Msg] {
	c := newClient[Req, Msg](hc, baseURL, service, method, opts)
	return func(ctx context.Context, req Req) iter.Seq2[Msg, error] {
		return func(yield func(Msg, error) bool) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel() // releases the stream when the consumer stops early
			r := connect.NewRequest(&req)
			outgoing(ctx, r.Header())
			stream, err := c.CallServerStream(ctx, r)
			if err != nil {
				var zero Msg
				yield(zero, FromError(err))
				return
			}
			defer stream.Close()
			recv := func() (*Msg, error) {
				if stream.Receive() {
					return stream.Msg(), nil
				}
				if err := stream.Err(); err != nil {
					return nil, err
				}
				return nil, io.EOF
			}
			for m, err := range receive(recv, false) {
				if !yield(m, err) {
					return
				}
			}
		}
	}
}

// Send returns a client for a client-streaming procedure.
func Send[Msg, Resp any](hc connect.HTTPClient, baseURL, service, method string, opts ...connect.ClientOption) kakarot.ClientStream[Msg, Resp] {
	c := newClient[Msg, Resp](hc, baseURL, service, method, opts)
	return func(ctx context.Context, in iter.Seq2[Msg, error]) (Resp, error) {
		var zero Resp
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream := c.CallClientStream(ctx)
		outgoing(ctx, stream.RequestHeader())
		for m, err := range in {
			if err != nil {
				return zero, err // cancel aborts the RPC
			}
			if err := stream.Send(&m); err != nil {
				if errors.Is(err, io.EOF) {
					break // the server ended the call; CloseAndReceive has its error
				}
				return zero, FromError(err)
			}
		}
		resp, err := stream.CloseAndReceive()
		if err != nil {
			return zero, FromError(err)
		}
		return *resp.Msg, nil
	}
}

// Duplex returns a client for a bidirectional procedure. in is sent on its
// own goroutine while replies are passed to send; it returns when the
// server ends the stream. It needs an HTTP/2 client.
func Duplex[In, Out any](hc connect.HTTPClient, baseURL, service, method string, opts ...connect.ClientOption) kakarot.BidiStream[In, Out] {
	c := newClient[In, Out](hc, baseURL, service, method, opts)
	return func(ctx context.Context, in iter.Seq2[In, error], send func(Out) error) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream := c.CallBidiStream(ctx)
		outgoing(ctx, stream.RequestHeader())
		// Send the headers now, so the server starts even when in is slow
		// to yield its first message.
		if err := stream.Send(nil); err != nil && !errors.Is(err, io.EOF) {
			return FromError(err)
		}
		sent := make(chan error, 1)
		go func() {
			for m, err := range in {
				if err != nil {
					sent <- err
					cancel()
					return
				}
				if err := stream.Send(&m); err != nil {
					sent <- nil // the receive side reports the real error
					return
				}
			}
			sent <- FromError(stream.CloseRequest())
		}()
		var recvErr error
		for m, err := range receive(stream.Receive, false) {
			if err == nil {
				err = send(m)
			}
			if err != nil {
				recvErr = err
				break
			}
		}
		cancel()
		_ = stream.CloseResponse()
		// Not waited for: the caller's iterator may block for ever once the
		// server has ended the stream.
		select {
		case sendErr := <-sent:
			if sendErr != nil {
				return sendErr // the caller's own input error, not its echo
			}
		default:
		}
		return recvErr
	}
}

type bearerKey struct{}

// WithBearer returns a context whose outgoing connectx calls carry
// "Authorization: Bearer <token>".
func WithBearer(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, bearerKey{}, token)
}

func outgoing(ctx context.Context, h http.Header) {
	if token, ok := ctx.Value(bearerKey{}).(string); ok && token != "" {
		h.Set("Authorization", "Bearer "+token)
	}
}
