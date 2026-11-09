// Package grpcx serves kakarot endpoints and streams over gRPC.
//
// protoc-gen-go-grpc generates a server interface; each method body is one
// line that hands the call to a kakarot endpoint:
//
//	type orderServer struct {
//	    pb.UnimplementedOrdersServer
//	    get   func(context.Context, *pb.GetOrderRequest) (*pb.Order, error)
//	    watch func(*pb.WatchRequest, grpc.ServerStreamingServer[pb.OrderEvent]) error
//	}
//	func (s *orderServer) GetOrder(ctx context.Context, r *pb.GetOrderRequest) (*pb.Order, error) {
//	    return s.get(ctx, r)
//	}
//
//	srv := &orderServer{
//	    get:   grpcx.Unary(op, eps.GetOrder, getOrderFromPB, orderToPB, mws...),
//	    watch: grpcx.ServerStream(op, eps.Watch, watchFromPB, eventToPB, mws...),
//	}
//
// The mapping functions convert between protobuf messages and domain types,
// so the domain never imports generated protobuf code. When the domain uses
// the protobuf types directly, pass grpcx.Same.
package grpcx

import (
	"context"
	"errors"
	"io"
	"iter"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gsoultan/kakarot"
)

// Same is the identity mapping, for domain types that are protobuf messages.
func Same[T any](v T) (T, error) { return v, nil }

// Unary adapts an endpoint to a gRPC unary method.
func Unary[PReq, PResp, Req, Resp any](
	op kakarot.Op,
	e kakarot.Endpoint[Req, Resp],
	in func(PReq) (Req, error),
	out func(Resp) (PResp, error),
	mws ...kakarot.Middleware,
) func(context.Context, PReq) (PResp, error) {
	op.Transport = "grpc"
	e = kakarot.Wrap(op, e, mws...)
	return func(ctx context.Context, p PReq) (PResp, error) {
		var zero PResp
		req, err := in(p)
		if err != nil {
			return zero, Status(kakarot.E(kakarot.CodeInvalid, "invalid request", err))
		}
		resp, err := e(ctx, req)
		if err != nil {
			return zero, Status(err)
		}
		pr, err := out(resp)
		if err != nil {
			return zero, Status(err)
		}
		return pr, nil
	}
}

// ServerStream adapts a kakarot server stream to a gRPC server-streaming
// method.
func ServerStream[PReq, PMsg, Req, Msg any](
	op kakarot.Op,
	s kakarot.ServerStream[Req, Msg],
	in func(PReq) (Req, error),
	out func(Msg) (*PMsg, error),
	mws ...kakarot.Middleware,
) func(PReq, grpc.ServerStreamingServer[PMsg]) error {
	op.Transport = "grpc"
	s = kakarot.WrapServerStream(op, s, mws...)
	return func(p PReq, stream grpc.ServerStreamingServer[PMsg]) error {
		req, err := in(p)
		if err != nil {
			return Status(kakarot.E(kakarot.CodeInvalid, "invalid request", err))
		}
		for msg, err := range s(stream.Context(), req) {
			if err != nil {
				return Status(err)
			}
			pm, err := out(msg)
			if err != nil {
				return Status(err)
			}
			if err := stream.Send(pm); err != nil {
				return err
			}
		}
		return nil
	}
}

// ClientStream adapts a kakarot client stream to a gRPC client-streaming
// method.
func ClientStream[PMsg, PResp, Msg, Resp any](
	op kakarot.Op,
	s kakarot.ClientStream[Msg, Resp],
	in func(*PMsg) (Msg, error),
	out func(Resp) (*PResp, error),
	mws ...kakarot.Middleware,
) func(grpc.ClientStreamingServer[PMsg, PResp]) error {
	op.Transport = "grpc"
	s = kakarot.WrapClientStream(op, s, mws...)
	return func(stream grpc.ClientStreamingServer[PMsg, PResp]) error {
		resp, err := s(stream.Context(), recvAll(stream.Recv, in))
		if err != nil {
			return Status(err)
		}
		pr, err := out(resp)
		if err != nil {
			return Status(err)
		}
		return stream.SendAndClose(pr)
	}
}

// Bidi adapts a kakarot bidirectional stream to a gRPC bidi method.
func Bidi[PIn, POut, In, Out any](
	op kakarot.Op,
	s kakarot.BidiStream[In, Out],
	in func(*PIn) (In, error),
	out func(Out) (*POut, error),
	mws ...kakarot.Middleware,
) func(grpc.BidiStreamingServer[PIn, POut]) error {
	op.Transport = "grpc"
	s = kakarot.WrapBidiStream(op, s, mws...)
	return func(stream grpc.BidiStreamingServer[PIn, POut]) error {
		send := func(m Out) error {
			pm, err := out(m)
			if err != nil {
				return err
			}
			return stream.Send(pm)
		}
		if err := s(stream.Context(), recvAll(stream.Recv, in), send); err != nil {
			return Status(err)
		}
		return nil
	}
}

func recvAll[P, M any](recv func() (*P, error), in func(*P) (M, error)) iter.Seq2[M, error] {
	return func(yield func(M, error) bool) {
		for {
			p, err := recv()
			if errors.Is(err, io.EOF) {
				return
			}
			var m M
			if err == nil {
				m, err = in(p)
				if err != nil {
					err = kakarot.E(kakarot.CodeInvalid, "invalid message", err)
				}
			}
			if !yield(m, err) || err != nil {
				return
			}
		}
	}
}

// Status converts an error to a gRPC status error, mapping kakarot codes.
// Internal errors keep their cause out of the message.
func Status(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		if _, isK := errors.AsType[*kakarot.Error](err); !isK {
			return err // already a gRPC status
		}
	}
	e := kakarot.AsError(err)
	return status.Error(CodeOf(e.Code), e.Message)
}

// CodeOf maps a kakarot code to a gRPC code.
func CodeOf(c kakarot.Code) codes.Code {
	switch c {
	case kakarot.CodeInvalid:
		return codes.InvalidArgument
	case kakarot.CodeNotFound:
		return codes.NotFound
	case kakarot.CodeConflict:
		return codes.AlreadyExists
	case kakarot.CodeUnauthorized:
		return codes.Unauthenticated
	case kakarot.CodeForbidden:
		return codes.PermissionDenied
	case kakarot.CodeUnavailable:
		return codes.Unavailable
	case kakarot.CodeTimeout:
		return codes.DeadlineExceeded
	}
	if c.String() == "rate_limited" {
		return codes.ResourceExhausted
	}
	return codes.Internal
}

// Server adapts a grpc.Server to kakarot.Server.
type Server struct {
	GRPC     *grpc.Server
	Addr     string
	Listener net.Listener // optional; overrides Addr (tests use bufconn)
}

// Serve listens until Shutdown.
func (s *Server) Serve(context.Context) error {
	lis := s.Listener
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", s.Addr); err != nil {
			return err
		}
	}
	err := s.GRPC.Serve(lis)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Shutdown stops gracefully, or hard when ctx expires first.
func (s *Server) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() { s.GRPC.GracefulStop(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.GRPC.Stop()
		return ctx.Err()
	}
}
