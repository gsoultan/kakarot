package grpcx_test

import (
	"context"
	"errors"
	"iter"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/auth"
	"github.com/gsoultan/kakarot/transport/grpcx"
)

type getOrder struct {
	ID string `json:"id"`
}

type order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

const svcName = "order.OrderService"

func serveJSON(t *testing.T, opts ...grpc.ServerOption) (*grpc.ClientConn, *[]string) {
	t.Helper()
	var ops []string
	spy := func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		ops = append(ops, op.String()+"@"+op.Transport)
		return next(ctx)
	}
	svc := grpcx.NewService(svcName, spy)
	grpcx.AddUnary(svc, "GetOrder", func(_ context.Context, in getOrder) (order, error) {
		if in.ID == "missing" {
			return order{}, kakarot.NotFound("order", in.ID)
		}
		if in.ID == "boom" {
			return order{}, errors.New("db password is hunter2")
		}
		return order{ID: in.ID, Total: 42}, nil
	})
	grpcx.AddServerStream(svc, "Export", func(_ context.Context, in getOrder) iter.Seq2[order, error] {
		return func(yield func(order, error) bool) {
			for i := range 3 {
				if !yield(order{ID: in.ID, Total: i}, nil) {
					return
				}
			}
			if in.ID == "fail" {
				yield(order{}, kakarot.E(kakarot.CodeUnavailable, "source went away", nil))
			}
		}
	})
	grpcx.AddClientStream(svc, "Import", func(_ context.Context, in iter.Seq2[order, error]) (order, error) {
		sum := order{ID: "sum"}
		for o, err := range in {
			if err != nil {
				return order{}, err
			}
			sum.Total += o.Total
		}
		return sum, nil
	})
	grpcx.AddBidi(svc, "Echo", func(_ context.Context, in iter.Seq2[order, error], send func(order) error) error {
		for o, err := range in {
			if err != nil {
				return err
			}
			if o.ID == "stop" {
				return kakarot.Invalid("stop requested", nil)
			}
			o.Total *= 2
			if err := send(o); err != nil {
				return err
			}
		}
		return nil
	})

	s := grpc.NewServer(opts...)
	svc.Register(s)
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
	return conn, &ops
}

func TestJSONUnaryAndErrors(t *testing.T) {
	conn, ops := serveJSON(t)
	get := grpcx.Invoke[getOrder, order](conn, svcName, "GetOrder")

	o, err := get(t.Context(), getOrder{ID: "7"})
	if err != nil || o != (order{ID: "7", Total: 42}) {
		t.Fatalf("o=%+v err=%v", o, err)
	}
	if (*ops)[0] != "order.OrderService.GetOrder@grpc" {
		t.Fatalf("ops = %v", *ops)
	}
	_, err = get(t.Context(), getOrder{ID: "missing"})
	if kakarot.CodeOf(err) != kakarot.CodeNotFound || kakarot.AsError(err).Message != "order missing not found" {
		t.Fatalf("domain error did not survive the wire: %v", err)
	}
	_, err = get(t.Context(), getOrder{ID: "boom"})
	if kakarot.CodeOf(err) != kakarot.CodeInternal || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("internal cause leaked or code wrong: %v", err)
	}
}

func TestJSONUnaryThroughInterceptor(t *testing.T) {
	seen := ""
	conn, _ := serveJSON(t, grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		seen = info.FullMethod
		return h(ctx, req)
	}))
	o, err := grpcx.Invoke[getOrder, order](conn, svcName, "GetOrder")(t.Context(), getOrder{ID: "1"})
	if err != nil || o.Total != 42 || seen != "/order.OrderService/GetOrder" {
		t.Fatalf("o=%+v err=%v seen=%q", o, err, seen)
	}
}

func TestJSONServerStream(t *testing.T) {
	conn, _ := serveJSON(t)
	export := grpcx.Receive[getOrder, order](conn, svcName, "Export")

	got, err := kakarot.Collect(export(t.Context(), getOrder{ID: "a"}))
	if err != nil || len(got) != 3 || got[2].Total != 2 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	got, err = kakarot.Collect(export(t.Context(), getOrder{ID: "fail"}))
	if kakarot.CodeOf(err) != kakarot.CodeUnavailable || len(got) != 3 {
		t.Fatalf("got=%d err=%v", len(got), err)
	}
	for o := range export(t.Context(), getOrder{ID: "early"}) { // stopping early must not hang or leak
		_ = o
		break
	}
}

func TestJSONClientStream(t *testing.T) {
	conn, _ := serveJSON(t)
	imp := grpcx.Send[order, order](conn, svcName, "Import")
	sum, err := imp(t.Context(), kakarot.SliceStream([]order{{Total: 1}, {Total: 2}, {Total: 3}}))
	if err != nil || sum != (order{ID: "sum", Total: 6}) {
		t.Fatalf("sum=%+v err=%v", sum, err)
	}
}

func TestJSONBidi(t *testing.T) {
	conn, _ := serveJSON(t)
	echo := grpcx.Duplex[order, order](conn, svcName, "Echo")

	var got []int
	err := echo(t.Context(), kakarot.SliceStream([]order{{Total: 1}, {Total: 5}}), func(o order) error {
		got = append(got, o.Total)
		return nil
	})
	if err != nil || len(got) != 2 || got[0] != 2 || got[1] != 10 {
		t.Fatalf("got=%v err=%v", got, err)
	}
	err = echo(t.Context(), kakarot.SliceStream([]order{{ID: "stop"}}), func(order) error { return nil })
	if kakarot.CodeOf(err) != kakarot.CodeInvalid {
		t.Fatalf("err = %v", err)
	}
}

func TestJSONMalformedRequest(t *testing.T) {
	conn, _ := serveJSON(t)
	// A string where the request object is expected.
	_, err := grpcx.Invoke[string, order](conn, svcName, "GetOrder")(t.Context(), "not an object")
	if kakarot.CodeOf(err) != kakarot.CodeInvalid {
		t.Fatalf("err = %v", err)
	}
}

type user struct{ Name string }

func TestBearerAuthenticatesUnaryAndStreams(t *testing.T) {
	verify := func(_ context.Context, token string) (user, error) {
		if token != "good" {
			return user{}, errors.New("signature mismatch")
		}
		return user{Name: "ada"}, nil
	}
	unary, stream := grpcx.Bearer(verify)
	svc := grpcx.NewService("who.Who", auth.Require[user](nil))
	grpcx.AddUnary(svc, "Me", func(ctx context.Context, _ struct{}) (string, error) {
		u, _ := auth.From[user](ctx)
		return u.Name, nil
	})
	grpcx.AddServerStream(svc, "Names", func(ctx context.Context, _ struct{}) iter.Seq2[string, error] {
		u, _ := auth.From[user](ctx)
		return kakarot.SliceStream([]string{u.Name})
	})
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
	svc.Register(s)
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
	me := grpcx.Invoke[struct{}, string](conn, "who.Who", "Me")
	names := grpcx.Receive[struct{}, string](conn, "who.Who", "Names")

	if name, err := me(grpcx.WithBearer(t.Context(), "good"), struct{}{}); err != nil || name != "ada" {
		t.Fatalf("name=%q err=%v", name, err)
	}
	if got, err := kakarot.Collect(names(grpcx.WithBearer(t.Context(), "good"), struct{}{})); err != nil || len(got) != 1 || got[0] != "ada" {
		t.Fatalf("stream: %v %v", got, err)
	}
	if _, err := me(t.Context(), struct{}{}); kakarot.CodeOf(err) != kakarot.CodeUnauthorized {
		t.Fatalf("no token: %v", err)
	}
	_, err = me(grpcx.WithBearer(t.Context(), "bad"), struct{}{})
	if kakarot.CodeOf(err) != kakarot.CodeUnauthorized || strings.Contains(err.Error(), "signature") {
		t.Fatalf("bad token: %v", err)
	}
	if _, err := kakarot.Collect(names(grpcx.WithBearer(t.Context(), "bad"), struct{}{})); kakarot.CodeOf(err) != kakarot.CodeUnauthorized {
		t.Fatalf("bad token on stream: %v", err)
	}
}

var codeQuota = kakarot.NewCode("grpc_quota_exceeded")

func TestErrorsKeepCodeAndFieldsOverTheWire(t *testing.T) {
	grpcx.RegisterCode(codeQuota, codes.ResourceExhausted)
	svc := grpcx.NewService("errs.Errs")
	grpcx.AddUnary(svc, "Do", func(_ context.Context, kind string) (string, error) {
		if kind == "quota" {
			return "", kakarot.E(codeQuota, "monthly quota used up", nil)
		}
		return "", kakarot.Invalid("invalid order", map[string]string{"amount": "must be positive"})
	})
	s := grpc.NewServer()
	svc.Register(s)
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
	do := grpcx.Invoke[string, string](conn, "errs.Errs", "Do")

	_, err = do(t.Context(), "quota")
	if e := kakarot.AsError(err); e.Code != codeQuota || e.Message != "monthly quota used up" || status.Code(e.Cause) != codes.ResourceExhausted {
		t.Fatalf("application code lost: %v", err)
	}
	_, err = do(t.Context(), "fields")
	if e := kakarot.AsError(err); e.Code != kakarot.CodeInvalid || e.Fields["amount"] != "must be positive" {
		t.Fatalf("fields lost: %+v", e)
	}
}
