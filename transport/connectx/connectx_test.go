package connectx_test

import (
	"context"
	"errors"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/auth"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/transport/connectx"
	"github.com/gsoultan/kakarot/transport/grpcx"
)

type getOrder struct {
	ID string `json:"id"`
}

type order struct {
	ID    string `json:"id"`
	Total int32  `json:"total"`
}

const svcName = "order.v1.OrderService"

var codeQuota = kakarot.NewCode("connect_quota_exceeded")

func init() { grpcx.RegisterCode(codeQuota, codes.ResourceExhausted) }

// orderService registers one procedure of every kind. ops records each
// call's op as the shared middleware sees it.
func orderService(ops *opLog) *connectx.Service {
	svc := connectx.NewService(svcName, ops.record)
	connectx.AddUnary(svc, "GetOrder", func(_ context.Context, in getOrder) (order, error) {
		switch in.ID {
		case "missing":
			return order{}, kakarot.NotFound("order", in.ID)
		case "bad":
			return order{}, kakarot.Invalid("invalid order", map[string]string{"id": "must not be bad"})
		case "quota":
			return order{}, kakarot.E(codeQuota, "monthly quota used up", nil)
		case "boom":
			return order{}, errors.New("db password is hunter2")
		}
		return order{ID: in.ID, Total: 42}, nil
	})
	connectx.AddServerStream(svc, "Export", func(_ context.Context, in getOrder) iter.Seq2[order, error] {
		return func(yield func(order, error) bool) {
			for i := range int32(3) {
				if !yield(order{ID: in.ID, Total: i}, nil) {
					return
				}
			}
			if in.ID == "fail" {
				yield(order{}, kakarot.E(kakarot.CodeUnavailable, "source went away", nil))
			}
		}
	})
	connectx.AddClientStream(svc, "Sum", func(_ context.Context, in iter.Seq2[order, error]) (order, error) {
		var sum order
		for o, err := range in {
			if err != nil {
				return order{}, err
			}
			sum.Total += o.Total
		}
		return sum, nil
	})
	connectx.AddBidi(svc, "Double", func(_ context.Context, in iter.Seq2[order, error], send func(order) error) error {
		for o, err := range in {
			if err != nil {
				return err
			}
			o.Total *= 2
			if err := send(o); err != nil {
				return err
			}
		}
		return nil
	})
	return svc
}

type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) record(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
	l.mu.Lock()
	l.ops = append(l.ops, op.String()+"@"+op.Transport)
	l.mu.Unlock()
	return next(ctx)
}

// serve starts the service on an HTTP/2 TLS test server.
func serve(t *testing.T, svc *connectx.Service, opts ...connect.HandlerOption) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(svc.Handler(opts...))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// protocols are the client configurations every test runs against.
var protocols = map[string][]connect.ClientOption{
	"connect+json":  nil,
	"connect+proto": {connectx.Protobuf()},
	"grpc+json":     {connect.WithGRPC()},
	"grpc+proto":    {connect.WithGRPC(), connectx.Protobuf()},
	"grpcweb+proto": {connect.WithGRPCWeb(), connectx.Protobuf()},
}

func TestUnaryOverEveryProtocol(t *testing.T) {
	var ops opLog
	srv := serve(t, orderService(&ops))
	for name, opts := range protocols {
		t.Run(name, func(t *testing.T) {
			get := connectx.Invoke[getOrder, order](srv.Client(), srv.URL, svcName, "GetOrder", opts...)

			got, err := get(t.Context(), getOrder{ID: "o-1"})
			if err != nil || got != (order{ID: "o-1", Total: 42}) {
				t.Fatalf("got %+v, %v", got, err)
			}

			_, err = get(t.Context(), getOrder{ID: "missing"})
			kakarottest.WantCode(t, err, kakarot.CodeNotFound)

			_, err = get(t.Context(), getOrder{ID: "bad"})
			kakarottest.WantCode(t, err, kakarot.CodeInvalid)
			kakarottest.WantField(t, err, "id")

			_, err = get(t.Context(), getOrder{ID: "quota"})
			kakarottest.WantCode(t, err, codeQuota)

			_, err = get(t.Context(), getOrder{ID: "boom"})
			kakarottest.WantCode(t, err, kakarot.CodeInternal)
			if strings.Contains(err.Error(), "hunter2") {
				t.Fatalf("internal cause leaked to the client: %v", err)
			}
		})
	}
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if len(ops.ops) == 0 || ops.ops[0] != svcName+".GetOrder@connect" {
		t.Fatalf("ops = %v", ops.ops)
	}
}

func TestStreamsOverEveryProtocol(t *testing.T) {
	srv := serve(t, orderService(&opLog{}))
	for name, opts := range protocols {
		t.Run(name, func(t *testing.T) {
			hc, url := srv.Client(), srv.URL
			export := connectx.Receive[getOrder, order](hc, url, svcName, "Export", opts...)
			got, err := kakarot.Collect(export(t.Context(), getOrder{ID: "o"}))
			if err != nil || len(got) != 3 || got[2].Total != 2 {
				t.Fatalf("server stream: %+v, %v", got, err)
			}
			got, err = kakarot.Collect(export(t.Context(), getOrder{ID: "fail"}))
			kakarottest.WantCode(t, err, kakarot.CodeUnavailable)
			if len(got) != 3 {
				t.Fatalf("messages before the error: %+v", got)
			}

			sum := connectx.Send[order, order](hc, url, svcName, "Sum", opts...)
			total, err := sum(t.Context(), kakarot.SliceStream([]order{{Total: 1}, {Total: 2}, {Total: 3}}))
			if err != nil || total.Total != 6 {
				t.Fatalf("client stream: %+v, %v", total, err)
			}

			double := connectx.Duplex[order, order](hc, url, svcName, "Double", opts...)
			var replies []int32
			err = double(t.Context(), kakarot.SliceStream([]order{{Total: 1}, {Total: 5}}), func(o order) error {
				replies = append(replies, o.Total)
				return nil
			})
			if err != nil || len(replies) != 2 || replies[0] != 2 || replies[1] != 10 {
				t.Fatalf("bidi: %v, %v", replies, err)
			}
		})
	}
}

func TestPlainHTTPClientsCallUnaryWithJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(orderService(&opLog{}).Handler())

	res := kakarottest.Do[order](t, mux, http.MethodPost, "/"+svcName+"/GetOrder", getOrder{ID: "o-7"})
	if res.Status != http.StatusOK || res.Body != (order{ID: "o-7", Total: 42}) {
		t.Fatalf("status %d, body %s", res.Status, res.Raw)
	}

	res = kakarottest.Do[order](t, mux, http.MethodPost, "/"+svcName+"/GetOrder", `{"id": 7}`)
	if res.Status != http.StatusBadRequest || !strings.Contains(string(res.Raw), `"invalid_argument"`) {
		t.Fatalf("malformed body: status %d, body %s", res.Status, res.Raw)
	}

	res = kakarottest.Do[order](t, mux, http.MethodPost, "/"+svcName+"/GetOrder", getOrder{ID: "missing"})
	if res.Status != http.StatusNotFound || !strings.Contains(string(res.Raw), `"not_found"`) {
		t.Fatalf("not found: status %d, body %s", res.Status, res.Raw)
	}
}

func TestMalformedStreamMessageIsInvalid(t *testing.T) {
	svc := connectx.NewService("bad.Bad")
	connectx.AddClientStream(svc, "Count", func(_ context.Context, in iter.Seq2[order, error]) (int, error) {
		n := 0
		for _, err := range in {
			if err != nil {
				return 0, err
			}
			n++
		}
		return n, nil
	})
	srv := serve(t, svc)
	// The client sends strings where the server expects orders.
	count := connectx.Send[string, int](srv.Client(), srv.URL, "bad.Bad", "Count")
	_, err := count(t.Context(), kakarot.SliceStream([]string{"not an order"}))
	kakarottest.WantCode(t, err, kakarot.CodeInvalid)
}

type user struct{ Name string }

func TestBearerAuthenticatesUnaryAndStreams(t *testing.T) {
	verify := func(_ context.Context, token string) (user, error) {
		if token != "good" {
			return user{}, errors.New("signature mismatch")
		}
		return user{Name: "ada"}, nil
	}
	svc := connectx.NewService("who.v1.Who", auth.Require[user](nil))
	connectx.AddUnary(svc, "Me", func(ctx context.Context, _ struct{}) (string, error) {
		u, _ := auth.From[user](ctx)
		return u.Name, nil
	})
	connectx.AddServerStream(svc, "Names", func(ctx context.Context, _ struct{}) iter.Seq2[string, error] {
		u, _ := auth.From[user](ctx)
		return kakarot.SliceStream([]string{u.Name})
	})
	srv := serve(t, svc, connect.WithInterceptors(connectx.Bearer(verify)))
	me := connectx.Invoke[struct{}, string](srv.Client(), srv.URL, "who.v1.Who", "Me")
	names := connectx.Receive[struct{}, string](srv.Client(), srv.URL, "who.v1.Who", "Names")

	if name, err := me(connectx.WithBearer(t.Context(), "good"), struct{}{}); err != nil || name != "ada" {
		t.Fatalf("name=%q err=%v", name, err)
	}
	if got, err := kakarot.Collect(names(connectx.WithBearer(t.Context(), "good"), struct{}{})); err != nil || len(got) != 1 || got[0] != "ada" {
		t.Fatalf("stream: %v %v", got, err)
	}
	_, err := me(t.Context(), struct{}{})
	kakarottest.WantCode(t, err, kakarot.CodeUnauthorized)

	_, err = me(connectx.WithBearer(t.Context(), "bad"), struct{}{})
	kakarottest.WantCode(t, err, kakarot.CodeUnauthorized)
	if strings.Contains(err.Error(), "signature") {
		t.Fatalf("verifier cause leaked: %v", err)
	}
	_, err = kakarot.Collect(names(connectx.WithBearer(t.Context(), "bad"), struct{}{}))
	kakarottest.WantCode(t, err, kakarot.CodeUnauthorized)
}

type traceKey struct{}

func TestCarrierPropagatesValuesBothWays(t *testing.T) {
	trace := connectx.Carrier{
		Inject: func(ctx context.Context) map[string]string {
			id, _ := ctx.Value(traceKey{}).(string)
			return map[string]string{"x-trace-id": id}
		},
		Extract: func(ctx context.Context, h map[string]string) context.Context {
			return context.WithValue(ctx, traceKey{}, h["x-trace-id"])
		},
	}
	svc := connectx.NewService("trace.v1.Trace")
	connectx.AddUnary(svc, "ID", func(ctx context.Context, _ struct{}) (string, error) {
		id, _ := ctx.Value(traceKey{}).(string)
		return id, nil
	})
	connectx.AddServerStream(svc, "IDs", func(ctx context.Context, _ struct{}) iter.Seq2[string, error] {
		id, _ := ctx.Value(traceKey{}).(string)
		return kakarot.SliceStream([]string{id})
	})
	srv := serve(t, svc, connect.WithInterceptors(trace.Interceptor()))
	ic := connect.WithInterceptors(trace.Interceptor())
	ctx := context.WithValue(t.Context(), traceKey{}, "t-42")

	id, err := connectx.Invoke[struct{}, string](srv.Client(), srv.URL, "trace.v1.Trace", "ID", ic)(ctx, struct{}{})
	if err != nil || id != "t-42" {
		t.Fatalf("unary: %q, %v", id, err)
	}
	ids, err := kakarot.Collect(connectx.Receive[struct{}, string](srv.Client(), srv.URL, "trace.v1.Trace", "IDs", ic)(ctx, struct{}{}))
	if err != nil || len(ids) != 1 || ids[0] != "t-42" {
		t.Fatalf("stream: %v, %v", ids, err)
	}
}

// A Connect client speaking gRPC reaches an interface-first grpcx server:
// both sides use the same JSON and protobuf.
func TestClientCallsGRPCXServer(t *testing.T) {
	gsvc := grpcx.NewService(svcName)
	grpcx.AddUnary(gsvc, "GetOrder", func(_ context.Context, in getOrder) (order, error) {
		if in.ID == "missing" {
			return order{}, kakarot.NotFound("order", in.ID)
		}
		return order{ID: in.ID, Total: 7}, nil
	})
	grpcx.EnableProtobuf()
	gs := grpc.NewServer()
	gsvc.Register(gs)
	srv := httptest.NewUnstartedServer(gs)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	for _, opts := range [][]connect.ClientOption{{connect.WithGRPC()}, {connect.WithGRPC(), connectx.Protobuf()}} {
		get := connectx.Invoke[getOrder, order](srv.Client(), srv.URL, svcName, "GetOrder", opts...)
		if got, err := get(t.Context(), getOrder{ID: "o-1"}); err != nil || got.Total != 7 {
			t.Fatalf("got %+v, %v", got, err)
		}
		_, err := get(t.Context(), getOrder{ID: "missing"})
		kakarottest.WantCode(t, err, kakarot.CodeNotFound)
	}
}

func TestProtoMatchesGRPCX(t *testing.T) {
	got, err := orderService(&opLog{}).Proto()
	if err != nil {
		t.Fatal(err)
	}
	gsvc := grpcx.NewService(svcName)
	grpcx.AddUnary(gsvc, "GetOrder", func(context.Context, getOrder) (order, error) { return order{}, nil })
	grpcx.AddServerStream(gsvc, "Export", func(context.Context, getOrder) iter.Seq2[order, error] { return nil })
	grpcx.AddClientStream(gsvc, "Sum", func(context.Context, iter.Seq2[order, error]) (order, error) { return order{}, nil })
	grpcx.AddBidi(gsvc, "Double", func(context.Context, iter.Seq2[order, error], func(order) error) error { return nil })
	want, err := gsvc.Proto()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("connectx proto differs from grpcx:\n%s\nwant:\n%s", got, want)
	}
	if !strings.Contains(got, "rpc Double(stream Order) returns (stream Order);") {
		t.Fatalf("missing bidi rpc:\n%s", got)
	}
}

func TestServerSpeaksH2C(t *testing.T) {
	s := connectx.NewServer(":0", http.NotFoundHandler())
	if p := s.HTTP.Protocols; p == nil || !p.HTTP1() || !p.HTTP2() || !p.UnencryptedHTTP2() {
		t.Fatalf("protocols = %v", s.HTTP.Protocols)
	}
}

func TestHandlerTwicePanics(t *testing.T) {
	svc := connectx.NewService("x.X")
	svc.Handler()
	defer func() {
		if recover() == nil {
			t.Fatal("second Handler call did not panic")
		}
	}()
	svc.Handler()
}
