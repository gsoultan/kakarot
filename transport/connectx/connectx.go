// Package connectx serves kakarot endpoints and streams over Connect
// (connectrpc.com/connect).
//
// A Service is built straight from typed endpoints, with no .proto file and
// no code generation, exactly like an interface-first grpcx service:
//
//	svc := connectx.NewService("order.v1.OrderService", mws...)
//	connectx.AddUnary(svc, "GetOrder", eps.GetOrder)
//	connectx.AddServerStream(svc, "ExportOrders", eps.ExportOrders)
//	mux.Handle(svc.Handler())
//
// One handler answers the three protocols connect-go speaks: Connect, gRPC
// and gRPC-Web, so browsers, curl, connect-es, connect-go and plain gRPC
// clients all reach the same endpoints. Messages are the domain types,
// encoded as JSON (content type application/json, application/connect+json
// or application/grpc+json) or as protobuf in the layout Service.Proto
// writes, the same layout grpcx uses.
//
// Callers in Go use the typed helpers Invoke, Receive, Send and Duplex.
// Errors travel as Connect errors with the kakarot code name and the
// per-field problems in a google.rpc.ErrorInfo detail, so FromError on the
// client restores the exact *kakarot.Error, as grpcx.FromStatus does.
//
// Streams that send in both directions at once (AddBidi) need HTTP/2; use
// NewServer, which also accepts HTTP/2 without TLS.
package connectx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/transport/grpcx"
)

// Transport is the Op.Transport of calls served by connectx.
const Transport = "connect"

// jsonCodec encodes the domain types with encoding/json. connect-go's own
// JSON codec only handles protobuf messages.
type jsonCodec struct{ name string }

func (c jsonCodec) Name() string                  { return c.name }
func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// codecs replace connect-go's protobuf-only defaults on both sides.
func handlerCodecs() connect.HandlerOption {
	return connect.WithHandlerOptions(
		connect.WithCodec(grpcx.ProtoCodec()),
		connect.WithCodec(jsonCodec{"json"}),
		connect.WithCodec(jsonCodec{"json; charset=utf-8"}),
	)
}

// Service collects the procedures of one Connect service.
type Service struct {
	name     string
	mws      []kakarot.Middleware
	mux      *http.ServeMux
	pending  []func(opts []connect.HandlerOption)
	schema   *grpcx.Service
	finished bool
}

// NewService starts a service named name ("order.v1.OrderService"). mws are
// shared endpoint middleware for every procedure.
func NewService(name string, mws ...kakarot.Middleware) *Service {
	return &Service{name: name, mws: mws, schema: grpcx.NewService(name)}
}

// Name returns the fully qualified service name.
func (s *Service) Name() string { return s.name }

// Handler returns the path prefix ("/order.v1.OrderService/") and the
// handler that serves every procedure under it, for http.ServeMux.Handle.
// opts are connect-go handler options, such as connect.WithInterceptors or
// connect.WithReadMaxBytes. Call it once, after every Add.
func (s *Service) Handler(opts ...connect.HandlerOption) (string, http.Handler) {
	if s.finished {
		panic("connectx: Handler called twice on " + s.name)
	}
	s.finished = true
	opts = append([]connect.HandlerOption{handlerCodecs()}, opts...)
	s.mux = http.NewServeMux()
	for _, add := range s.pending {
		add(opts)
	}
	return "/" + s.name + "/", s.mux
}

// Proto writes the service as a proto3 file, for clients generated with
// buf or protoc (connect-es, connect-swift, connect-kotlin, gRPC). It is the
// file grpcx.Service.Proto writes for the same methods.
func (s *Service) Proto() (string, error) { return s.schema.Proto() }

// Descriptor describes the service as a protobuf file descriptor.
func (s *Service) Descriptor() (*descriptorpb.FileDescriptorProto, error) {
	return s.schema.Descriptor()
}

func (s *Service) op(method string) kakarot.Op {
	return kakarot.Op{Service: s.name, Method: method, Transport: Transport}
}

func (s *Service) add(method string, h func(procedure string, opts []connect.HandlerOption) http.Handler) {
	if s.finished {
		panic("connectx: " + s.name + "." + method + " added after Handler")
	}
	procedure := procedure(s.name, method)
	s.pending = append(s.pending, func(opts []connect.HandlerOption) {
		s.mux.Handle(procedure, h(procedure, opts))
	})
}

func procedure(service, method string) string { return "/" + service + "/" + method }

// AddUnary serves an endpoint as a unary procedure.
func AddUnary[Req, Resp any](s *Service, method string, e kakarot.Endpoint[Req, Resp]) {
	grpcx.AddUnary(s.schema, method, e)
	e = kakarot.Wrap(s.op(method), e, s.mws...)
	s.add(method, func(p string, opts []connect.HandlerOption) http.Handler {
		return connect.NewUnaryHandlerSimple(p, func(ctx context.Context, req *Req) (*Resp, error) {
			resp, err := e(ctx, *req)
			if err != nil {
				return nil, Error(err)
			}
			return &resp, nil
		}, opts...)
	})
}

// AddServerStream serves a server stream.
func AddServerStream[Req, Msg any](s *Service, method string, st kakarot.ServerStream[Req, Msg]) {
	grpcx.AddServerStream(s.schema, method, st)
	st = kakarot.WrapServerStream(s.op(method), st, s.mws...)
	s.add(method, func(p string, opts []connect.HandlerOption) http.Handler {
		return connect.NewServerStreamHandlerSimple(p, func(ctx context.Context, req *Req, out *connect.ServerStream[Msg]) error {
			for msg, err := range st(ctx, *req) {
				if err != nil {
					return Error(err)
				}
				if err := out.Send(&msg); err != nil {
					return err
				}
			}
			return nil
		}, opts...)
	})
}

// AddClientStream serves a client stream.
func AddClientStream[Msg, Resp any](s *Service, method string, st kakarot.ClientStream[Msg, Resp]) {
	grpcx.AddClientStream(s.schema, method, st)
	st = kakarot.WrapClientStream(s.op(method), st, s.mws...)
	s.add(method, func(p string, opts []connect.HandlerOption) http.Handler {
		return connect.NewClientStreamHandlerSimple(p, func(ctx context.Context, in *connect.ClientStream[Msg]) (*Resp, error) {
			resp, err := st(ctx, receive(func() (*Msg, error) {
				if in.Receive() {
					return in.Msg(), nil
				}
				if err := in.Err(); err != nil {
					return nil, err
				}
				return nil, io.EOF
			}, true))
			if err != nil {
				return nil, Error(err)
			}
			return &resp, nil
		}, opts...)
	})
}

// AddBidi serves a bidirectional stream. It needs HTTP/2.
func AddBidi[In, Out any](s *Service, method string, st kakarot.BidiStream[In, Out]) {
	grpcx.AddBidi(s.schema, method, st)
	st = kakarot.WrapBidiStream(s.op(method), st, s.mws...)
	s.add(method, func(p string, opts []connect.HandlerOption) http.Handler {
		return connect.NewBidiStreamHandler(p, func(ctx context.Context, stream *connect.BidiStream[In, Out]) error {
			send := func(m Out) error { return stream.Send(&m) }
			if err := st(ctx, receive(stream.Receive, true), send); err != nil {
				return Error(err)
			}
			return nil
		}, opts...)
	})
}

// receive turns a Receive function into a typed iterator that ends at EOF.
// A server reports a message that failed to decode as CodeInvalid; a client
// maps the peer's error back to a kakarot error.
func receive[M any](recv func() (*M, error), server bool) iter.Seq2[M, error] {
	return func(yield func(M, error) bool) {
		for {
			var m M
			p, err := recv()
			if errors.Is(err, io.EOF) {
				return
			}
			switch {
			case err == nil:
				m = *p
			case server && connect.CodeOf(err) == connect.CodeInvalidArgument:
				err = kakarot.E(kakarot.CodeInvalid, "malformed message", err)
			default:
				err = FromError(err)
			}
			if !yield(m, err) || err != nil {
				return
			}
		}
	}
}

// Error converts an error to a *connect.Error, mapping kakarot codes the
// way grpcx.CodeOf does (including codes set with grpcx.RegisterCode).
// Internal errors keep their cause out of the message.
//
// The error also carries the kakarot code name and the per-field problems
// as a google.rpc.ErrorInfo detail, so FromError on the client restores
// the exact error, including application codes made with kakarot.NewCode.
func Error(err error) error {
	if err == nil {
		return nil
	}
	if _, isK := errors.AsType[*kakarot.Error](err); !isK {
		if _, ok := errors.AsType[*connect.Error](err); ok {
			return err // already a Connect error
		}
	}
	e := kakarot.AsError(err)
	ce := connect.NewError(connect.Code(grpcx.CodeOf(e.Code)), errors.New(e.Message))
	if e.Code != kakarot.CodeInternal {
		if d, derr := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Reason: e.Code.String(), Domain: errorDomain, Metadata: e.Fields,
		}); derr == nil {
			ce.AddDetail(d)
		}
	}
	return ce
}

const errorDomain = "kakarot"

// FromError converts an error returned by a Connect client back to a
// *kakarot.Error with the matching code, so callers handle remote and local
// failures the same way.
func FromError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*kakarot.Error](err); ok {
		return err
	}
	ce, ok := errors.AsType[*connect.Error](err)
	if !ok {
		return kakarot.E(kakarot.CodeUnavailable, "rpc failed", err)
	}
	// Connect codes are the gRPC codes, so grpcx does the mapping and reads
	// the ErrorInfo detail; one table serves both transports.
	st := status.New(codes.Code(ce.Code()), ce.Message())
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if info, ok := v.(*errdetails.ErrorInfo); derr == nil && ok {
			if withInfo, werr := st.WithDetails(info); werr == nil {
				st = withInfo
			}
		}
	}
	out := kakarot.AsError(grpcx.FromStatus(st.Err()))
	out.Cause = err
	return out
}
