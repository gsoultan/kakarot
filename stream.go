package kakarot

import (
	"context"
	"errors"
	"iter"
)

// ServerStream answers one request with a sequence of messages (gRPC
// server streaming, HTTP Server-Sent Events or NDJSON). The sequence ends at
// the first error.
type ServerStream[Req, Msg any] func(ctx context.Context, req Req) iter.Seq2[Msg, error]

// ClientStream reads a sequence of messages and answers once (gRPC client
// streaming, HTTP NDJSON upload).
type ClientStream[Msg, Resp any] func(ctx context.Context, in iter.Seq2[Msg, error]) (Resp, error)

// BidiStream reads and writes independently until either side ends (gRPC
// bidirectional streaming, WebSocket).
type BidiStream[In, Out any] func(ctx context.Context, in iter.Seq2[In, error], send func(Out) error) error

// errStopped marks a consumer that stopped iterating early; it is not an
// error of the stream.
var errStopped = errors.New("kakarot: consumer stopped")

// WrapServerStream applies shared middleware around the whole stream: the
// middleware runs once, from the first message to the last, and sees the
// stream's error.
func WrapServerStream[Req, Msg any](op Op, s ServerStream[Req, Msg], mws ...Middleware) ServerStream[Req, Msg] {
	if len(mws) == 0 {
		return s
	}
	chain := Chain(mws...)
	return func(ctx context.Context, req Req) iter.Seq2[Msg, error] {
		return func(yield func(Msg, error) bool) {
			err := chain(ctx, op, func(ctx context.Context) error {
				for m, err := range s(ctx, req) {
					if err != nil {
						return err
					}
					if !yield(m, nil) {
						return errStopped
					}
				}
				return nil
			})
			if err != nil && !errors.Is(err, errStopped) {
				var zero Msg
				yield(zero, err)
			}
		}
	}
}

// WrapClientStream applies shared middleware around a client stream.
func WrapClientStream[Msg, Resp any](op Op, s ClientStream[Msg, Resp], mws ...Middleware) ClientStream[Msg, Resp] {
	if len(mws) == 0 {
		return s
	}
	chain := Chain(mws...)
	return func(ctx context.Context, in iter.Seq2[Msg, error]) (Resp, error) {
		var resp Resp
		err := chain(ctx, op, func(ctx context.Context) error {
			var err error
			resp, err = s(ctx, in)
			return err
		})
		return resp, err
	}
}

// WrapBidiStream applies shared middleware around a bidirectional stream.
func WrapBidiStream[In, Out any](op Op, s BidiStream[In, Out], mws ...Middleware) BidiStream[In, Out] {
	if len(mws) == 0 {
		return s
	}
	chain := Chain(mws...)
	return func(ctx context.Context, in iter.Seq2[In, error], send func(Out) error) error {
		return chain(ctx, op, func(ctx context.Context) error { return s(ctx, in, send) })
	}
}

// SliceStream adapts a slice to a stream; handy in tests and simple handlers.
func SliceStream[T any](items []T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, it := range items {
			if !yield(it, nil) {
				return
			}
		}
	}
}

// Collect drains a stream into a slice, stopping at the first error.
func Collect[T any](s iter.Seq2[T, error]) ([]T, error) {
	var out []T
	for v, err := range s {
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}
