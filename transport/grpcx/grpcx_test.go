package grpcx_test

import (
	"context"
	"errors"
	"io"
	"iter"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/transport/grpcx"
)

// Domain side: plain Go types, no protobuf.
type name string

func greet(_ context.Context, n name) (string, error) {
	if n == "" {
		return "", kakarot.Invalid("name required", nil)
	}
	if n == "ghost" {
		return "", kakarot.NotFound("user", "ghost")
	}
	return "hello " + string(n), nil
}

func countTo(_ context.Context, n int64) iter.Seq2[int64, error] {
	return func(yield func(int64, error) bool) {
		for i := int64(1); i <= n; i++ {
			if !yield(i, nil) {
				return
			}
		}
	}
}

func sum(_ context.Context, in iter.Seq2[int64, error]) (int64, error) {
	var t int64
	for v, err := range in {
		if err != nil {
			return 0, err
		}
		t += v
	}
	return t, nil
}

func shout(_ context.Context, in iter.Seq2[string, error], send func(string) error) error {
	for s, err := range in {
		if err != nil {
			return err
		}
		if err := send(strings.ToUpper(s)); err != nil {
			return err
		}
	}
	return nil
}

// Mapping between protobuf messages and domain types.
func nameFromPB(p *wrapperspb.StringValue) (name, error)  { return name(p.GetValue()), nil }
func strToPB(s string) (*wrapperspb.StringValue, error)   { return wrapperspb.String(s), nil }
func strFromPB(p *wrapperspb.StringValue) (string, error) { return p.GetValue(), nil }
func intFromPB(p *wrapperspb.Int64Value) (int64, error)   { return p.GetValue(), nil }
func intToPB(v int64) (*wrapperspb.Int64Value, error)     { return wrapperspb.Int64(v), nil }

// What protoc-gen-go-grpc would generate, written by hand for the test.
type demoServer struct {
	greet func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
	count func(*wrapperspb.Int64Value, grpc.ServerStreamingServer[wrapperspb.Int64Value]) error
	sum   func(grpc.ClientStreamingServer[wrapperspb.Int64Value, wrapperspb.Int64Value]) error
	shout func(grpc.BidiStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error
}

var desc = grpc.ServiceDesc{
	ServiceName: "demo.Demo",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Greet",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.(*demoServer).greet(ctx, in)
		},
	}},
	Streams: []grpc.StreamDesc{
		{StreamName: "Count", ServerStreams: true, Handler: func(srv any, st grpc.ServerStream) error {
			in := new(wrapperspb.Int64Value)
			if err := st.RecvMsg(in); err != nil {
				return err
			}
			return srv.(*demoServer).count(in, &grpc.GenericServerStream[wrapperspb.Int64Value, wrapperspb.Int64Value]{ServerStream: st})
		}},
		{StreamName: "Sum", ClientStreams: true, Handler: func(srv any, st grpc.ServerStream) error {
			return srv.(*demoServer).sum(&grpc.GenericServerStream[wrapperspb.Int64Value, wrapperspb.Int64Value]{ServerStream: st})
		}},
		{StreamName: "Shout", ServerStreams: true, ClientStreams: true, Handler: func(srv any, st grpc.ServerStream) error {
			return srv.(*demoServer).shout(&grpc.GenericServerStream[wrapperspb.StringValue, wrapperspb.StringValue]{ServerStream: st})
		}},
	},
}

func dial(t *testing.T) *grpc.ClientConn {
	t.Helper()
	op := func(m string) kakarot.Op { return kakarot.Op{Service: "demo", Method: m} }
	s := grpc.NewServer()
	s.RegisterService(&desc, &demoServer{
		greet: grpcx.Unary(op("Greet"), greet, nameFromPB, strToPB),
		count: grpcx.ServerStream(op("Count"), countTo, intFromPB, intToPB),
		sum:   grpcx.ClientStream(op("Sum"), sum, intFromPB, intToPB),
		shout: grpcx.Bidi(op("Shout"), shout, strFromPB, strToPB),
	})
	lis := bufconn.Listen(1 << 20)
	srv := &grpcx.Server{GRPC: s, Listener: lis}
	go srv.Serve(t.Context())
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestUnary(t *testing.T) {
	conn := dial(t)
	out := new(wrapperspb.StringValue)
	if err := conn.Invoke(t.Context(), "/demo.Demo/Greet", wrapperspb.String("ada"), out); err != nil || out.Value != "hello ada" {
		t.Fatalf("out=%v err=%v", out, err)
	}
	for in, want := range map[string]codes.Code{"": codes.InvalidArgument, "ghost": codes.NotFound} {
		err := conn.Invoke(t.Context(), "/demo.Demo/Greet", wrapperspb.String(in), out)
		if status.Code(err) != want {
			t.Errorf("%q: %v, want %v", in, err, want)
		}
	}
}

func TestServerStream(t *testing.T) {
	conn := dial(t)
	cs, err := conn.NewStream(t.Context(), &desc.Streams[0], "/demo.Demo/Count")
	if err != nil {
		t.Fatal(err)
	}
	st := &grpc.GenericClientStream[wrapperspb.Int64Value, wrapperspb.Int64Value]{ClientStream: cs}
	_ = st.SendMsg(wrapperspb.Int64(3))
	_ = st.CloseSend()
	var got []int64
	for {
		m, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, m.Value)
	}
	if len(got) != 3 || got[2] != 3 {
		t.Fatalf("got %v", got)
	}
}

func TestClientStream(t *testing.T) {
	conn := dial(t)
	cs, err := conn.NewStream(t.Context(), &desc.Streams[1], "/demo.Demo/Sum")
	if err != nil {
		t.Fatal(err)
	}
	st := &grpc.GenericClientStream[wrapperspb.Int64Value, wrapperspb.Int64Value]{ClientStream: cs}
	for _, v := range []int64{1, 2, 3, 4} {
		if err := st.Send(wrapperspb.Int64(v)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := st.CloseAndRecv()
	if err != nil || res.Value != 10 {
		t.Fatalf("res=%v err=%v", res, err)
	}
}

func TestBidi(t *testing.T) {
	conn := dial(t)
	cs, err := conn.NewStream(t.Context(), &desc.Streams[2], "/demo.Demo/Shout")
	if err != nil {
		t.Fatal(err)
	}
	st := &grpc.GenericClientStream[wrapperspb.StringValue, wrapperspb.StringValue]{ClientStream: cs}
	for _, w := range []string{"a", "bc"} {
		_ = st.Send(wrapperspb.String(w))
		m, err := st.Recv()
		if err != nil || m.Value != strings.ToUpper(w) {
			t.Fatalf("m=%v err=%v", m, err)
		}
	}
	_ = st.CloseSend()
	if _, err := st.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}
