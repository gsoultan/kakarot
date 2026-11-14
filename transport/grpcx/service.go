package grpcx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"reflect"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"

	"github.com/gsoultan/kakarot"
)

// Interface-first gRPC: a Service is built straight from typed endpoints,
// with no .proto file and no protoc. Messages are the domain types encoded
// as JSON (gRPC content-subtype "json"), carried over ordinary HTTP/2 gRPC
// framing, so deadlines, metadata, status codes, streaming and interceptors
// all work as usual.
//
//	svc := grpcx.NewService("order.OrderService", mws...)
//	grpcx.AddUnary(svc, "GetOrder", eps.GetOrder)
//	grpcx.AddServerStream(svc, "ExportOrders", eps.ExportOrders)
//	svc.Register(grpcServer)
//
// Callers use the typed helpers Invoke, Receive, Send and Duplex, or any
// gRPC client with a JSON codec. `kakarot gen` writes both sides from a
// service interface marked //kakarot:grpc. For clients in other languages,
// Service.Proto writes a .proto file and EnableProtobuf serves protobuf.

// JSONSubtype is the gRPC content-subtype of interface-first services.
const JSONSubtype = "json"

type jsonCodec struct{}

func (jsonCodec) Name() string                  { return JSONSubtype }
func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal hands a server the raw bytes when it asks for them, so the
// handler decodes them itself and reports a bad message as CodeInvalid;
// gRPC reports a codec failure as Internal.
func (jsonCodec) Unmarshal(b []byte, v any) error {
	if raw, ok := v.(*rawMessage); ok {
		raw.bytes, raw.proto = append(raw.bytes[:0], b...), false
		return nil
	}
	return json.Unmarshal(b, v)
}

// decode reads one message on the server side.
func decode[M any](recv func(any) error) (M, error) {
	var (
		m   M
		raw rawMessage
	)
	if err := recv(&raw); err != nil {
		return m, err // io.EOF or a transport status
	}
	err := error(nil)
	if raw.proto {
		err = unmarshalProto(raw.bytes, &m)
	} else {
		err = json.Unmarshal(raw.bytes, &m)
	}
	if err != nil {
		return m, kakarot.E(kakarot.CodeInvalid, "malformed message", err)
	}
	return m, nil
}
func init()                                    { encoding.RegisterCodec(jsonCodec{}) }
func callJSON() grpc.CallOption                { return grpc.CallContentSubtype(JSONSubtype) }
func fullMethod(service, method string) string { return "/" + service + "/" + method }

// Service collects the methods of one interface-first gRPC service.
type Service struct {
	name    string
	mws     []kakarot.Middleware
	methods []grpc.MethodDesc
	streams []grpc.StreamDesc
	rpcs    []rpc
}

// NewService starts a service named name ("order.OrderService"). mws are
// shared endpoint middleware for every method.
func NewService(name string, mws ...kakarot.Middleware) *Service {
	return &Service{name: name, mws: mws}
}

// Name returns the gRPC service name.
func (s *Service) Name() string { return s.name }

// Register adds the service to a gRPC server. Call it after every Add.
func (s *Service) Register(r grpc.ServiceRegistrar) {
	r.RegisterService(&grpc.ServiceDesc{
		ServiceName: s.name,
		HandlerType: (*any)(nil),
		Methods:     s.methods,
		Streams:     s.streams,
	}, s)
}

func (s *Service) op(method string) kakarot.Op {
	return kakarot.Op{Service: s.name, Method: method, Transport: "grpc"}
}

// AddUnary serves an endpoint as a unary method.
func AddUnary[Req, Resp any](s *Service, method string, e kakarot.Endpoint[Req, Resp]) {
	s.rpcs = append(s.rpcs, rpc{name: method, in: reflect.TypeFor[Req](), out: reflect.TypeFor[Resp](), clientStream: false, serverStream: false})
	e = kakarot.Wrap(s.op(method), e, s.mws...)
	info := &grpc.UnaryServerInfo{Server: s, FullMethod: fullMethod(s.name, method)}
	s.methods = append(s.methods, grpc.MethodDesc{
		MethodName: method,
		Handler: func(_ any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
			req, err := decode[Req](dec)
			if err != nil {
				return nil, Status(err)
			}
			if ic == nil {
				resp, err := e(ctx, req)
				if err != nil {
					return nil, Status(err)
				}
				return resp, nil
			}
			// Interceptors are untyped by gRPC's design; the request they
			// are handed is the typed value, restored before the endpoint.
			return ic(ctx, req, info, func(ctx context.Context, r any) (any, error) {
				typed, ok := r.(Req)
				if !ok {
					return nil, status.Error(codes.Internal, "interceptor replaced the request with another type")
				}
				resp, err := e(ctx, typed)
				if err != nil {
					return nil, Status(err)
				}
				return resp, nil
			})
		},
	})
}

// AddServerStream serves a server stream.
func AddServerStream[Req, Msg any](s *Service, method string, st kakarot.ServerStream[Req, Msg]) {
	s.rpcs = append(s.rpcs, rpc{name: method, in: reflect.TypeFor[Req](), out: reflect.TypeFor[Msg](), clientStream: false, serverStream: true})
	st = kakarot.WrapServerStream(s.op(method), st, s.mws...)
	s.streams = append(s.streams, grpc.StreamDesc{
		StreamName:    method,
		ServerStreams: true,
		Handler: func(_ any, stream grpc.ServerStream) error {
			req, err := decode[Req](stream.RecvMsg)
			if err != nil {
				return Status(err)
			}
			for msg, err := range st(stream.Context(), req) {
				if err != nil {
					return Status(err)
				}
				if err := stream.SendMsg(msg); err != nil {
					return err
				}
			}
			return nil
		},
	})
}

// AddClientStream serves a client stream.
func AddClientStream[Msg, Resp any](s *Service, method string, st kakarot.ClientStream[Msg, Resp]) {
	s.rpcs = append(s.rpcs, rpc{name: method, in: reflect.TypeFor[Msg](), out: reflect.TypeFor[Resp](), clientStream: true, serverStream: false})
	st = kakarot.WrapClientStream(s.op(method), st, s.mws...)
	s.streams = append(s.streams, grpc.StreamDesc{
		StreamName:    method,
		ClientStreams: true,
		Handler: func(_ any, stream grpc.ServerStream) error {
			resp, err := st(stream.Context(), recvJSON[Msg](stream.RecvMsg, true))
			if err != nil {
				return Status(err)
			}
			return stream.SendMsg(resp)
		},
	})
}

// AddBidi serves a bidirectional stream.
func AddBidi[In, Out any](s *Service, method string, st kakarot.BidiStream[In, Out]) {
	s.rpcs = append(s.rpcs, rpc{name: method, in: reflect.TypeFor[In](), out: reflect.TypeFor[Out](), clientStream: true, serverStream: true})
	st = kakarot.WrapBidiStream(s.op(method), st, s.mws...)
	s.streams = append(s.streams, grpc.StreamDesc{
		StreamName:    method,
		ServerStreams: true,
		ClientStreams: true,
		Handler: func(_ any, stream grpc.ServerStream) error {
			send := func(m Out) error { return stream.SendMsg(m) }
			if err := st(stream.Context(), recvJSON[In](stream.RecvMsg, true), send); err != nil {
				return Status(err)
			}
			return nil
		},
	})
}

// recvJSON reads typed messages until EOF. A server decodes each message
// itself; a client maps the peer's status back to a kakarot error.
func recvJSON[M any](recv func(any) error, server bool) iter.Seq2[M, error] {
	return func(yield func(M, error) bool) {
		for {
			var (
				m   M
				err error
			)
			if server {
				m, err = decode[M](recv)
			} else {
				err = recv(&m)
			}
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil && !server {
				err = FromStatus(err)
			}
			if !yield(m, err) || err != nil {
				return
			}
		}
	}
}

// FromStatus converts a gRPC error received by a client back to a
// *kakarot.Error with the matching code, so callers handle remote and local
// failures the same way.
func FromStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*kakarot.Error](err); ok {
		return err
	}
	st, ok := status.FromError(err)
	if !ok {
		return kakarot.E(kakarot.CodeUnavailable, "rpc failed", err)
	}
	out := &kakarot.Error{Code: fromCode(st.Code()), Message: st.Message(), Cause: err}
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != errorDomain {
			continue
		}
		// The server named its exact code; an unknown name (a code this
		// process never registered) keeps the code derived from the status.
		if c, ok := kakarot.LookupCode(info.GetReason()); ok && c != kakarot.CodeInternal {
			out.Code = c
		}
		if len(info.GetMetadata()) > 0 {
			out.Fields = info.GetMetadata()
		}
	}
	return out
}

func fromCode(c codes.Code) kakarot.Code {
	switch c {
	case codes.InvalidArgument, codes.OutOfRange, codes.FailedPrecondition:
		return kakarot.CodeInvalid
	case codes.NotFound:
		return kakarot.CodeNotFound
	case codes.AlreadyExists, codes.Aborted:
		return kakarot.CodeConflict
	case codes.Unauthenticated:
		return kakarot.CodeUnauthorized
	case codes.PermissionDenied:
		return kakarot.CodeForbidden
	case codes.Unavailable, codes.ResourceExhausted:
		return kakarot.CodeUnavailable
	case codes.DeadlineExceeded, codes.Canceled:
		return kakarot.CodeTimeout
	}
	return kakarot.CodeInternal
}

// Invoke returns a client endpoint for a unary method: calling it performs
// the RPC. It has the same type as the server's endpoint, so client-side
// middleware (Retry, CircuitBreaker, Timeout) wraps it with kakarot.Wrap.
func Invoke[Req, Resp any](cc grpc.ClientConnInterface, service, method string) kakarot.Endpoint[Req, Resp] {
	full := fullMethod(service, method)
	return func(ctx context.Context, req Req) (Resp, error) {
		var resp Resp
		if err := cc.Invoke(ctx, full, req, &resp, callJSON()); err != nil {
			var zero Resp
			return zero, FromStatus(err)
		}
		return resp, nil
	}
}

// Receive returns a client for a server-streaming method.
func Receive[Req, Msg any](cc grpc.ClientConnInterface, service, method string) kakarot.ServerStream[Req, Msg] {
	full := fullMethod(service, method)
	desc := &grpc.StreamDesc{StreamName: method, ServerStreams: true}
	return func(ctx context.Context, req Req) iter.Seq2[Msg, error] {
		return func(yield func(Msg, error) bool) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel() // releases the stream when the consumer stops early
			var zero Msg
			cs, err := cc.NewStream(ctx, desc, full, callJSON())
			if err == nil {
				err = cs.SendMsg(req)
			}
			if err == nil {
				err = cs.CloseSend()
			}
			if err != nil {
				yield(zero, FromStatus(err))
				return
			}
			for m, err := range recvJSON[Msg](cs.RecvMsg, false) {
				if !yield(m, err) {
					return
				}
			}
		}
	}
}

// Send returns a client for a client-streaming method.
func Send[Msg, Resp any](cc grpc.ClientConnInterface, service, method string) kakarot.ClientStream[Msg, Resp] {
	full := fullMethod(service, method)
	desc := &grpc.StreamDesc{StreamName: method, ClientStreams: true}
	return func(ctx context.Context, in iter.Seq2[Msg, error]) (Resp, error) {
		var resp, zero Resp
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		cs, err := cc.NewStream(ctx, desc, full, callJSON())
		if err != nil {
			return zero, FromStatus(err)
		}
		for m, err := range in {
			if err != nil {
				return zero, err // cancel aborts the RPC
			}
			if err := cs.SendMsg(m); err != nil {
				if errors.Is(err, io.EOF) {
					break // the server ended the call; RecvMsg has its status
				}
				return zero, FromStatus(err)
			}
		}
		if err := cs.CloseSend(); err != nil {
			return zero, FromStatus(err)
		}
		if err := cs.RecvMsg(&resp); err != nil {
			return zero, FromStatus(err)
		}
		return resp, nil
	}
}

// Duplex returns a client for a bidirectional method. in is sent on its own
// goroutine while replies are passed to send; it returns when the server
// ends the stream.
func Duplex[In, Out any](cc grpc.ClientConnInterface, service, method string) kakarot.BidiStream[In, Out] {
	full := fullMethod(service, method)
	desc := &grpc.StreamDesc{StreamName: method, ServerStreams: true, ClientStreams: true}
	return func(ctx context.Context, in iter.Seq2[In, error], send func(Out) error) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		cs, err := cc.NewStream(ctx, desc, full, callJSON())
		if err != nil {
			return FromStatus(err)
		}
		sent := make(chan error, 1)
		go func() {
			for m, err := range in {
				if err != nil {
					sent <- err
					cancel()
					return
				}
				if err := cs.SendMsg(m); err != nil {
					sent <- nil // the receive side reports the real status
					return
				}
			}
			sent <- FromStatus(cs.CloseSend())
		}()
		var recvErr error
		for m, err := range recvJSON[Out](cs.RecvMsg, false) {
			if err == nil {
				err = send(m)
			}
			if err != nil {
				recvErr = err
				break
			}
		}
		cancel()
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
