package kakarot_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gsoultan/kakarot"
)

type greetReq struct{ Name string }
type greetResp struct{ Text string }

func greet(_ context.Context, r greetReq) (greetResp, error) {
	return greetResp{Text: "hi " + r.Name}, nil
}

func trace(log *[]string, name string) kakarot.Middleware {
	return func(op kakarot.Op, next kakarot.Call) kakarot.Call {
		return func(ctx context.Context, req any) (any, error) {
			*log = append(*log, name+">"+op.Method)
			resp, err := next(ctx, req)
			*log = append(*log, name+"<")
			return resp, err
		}
	}
}

func TestWrapOrderAndTypes(t *testing.T) {
	var log []string
	op := kakarot.Op{Service: "greeter", Method: "Greet"}
	e := kakarot.Wrap(op, greet, trace(&log, "a"), trace(&log, "b"))

	resp, err := e(t.Context(), greetReq{Name: "ada"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "hi ada" {
		t.Fatalf("resp = %q", resp.Text)
	}
	want := "[a>Greet b>Greet b< a<]"
	if got := fmt.Sprint(log); got != want {
		t.Fatalf("order = %s, want %s", got, want)
	}
}

func TestWrapNoMiddlewareReturnsSameEndpoint(t *testing.T) {
	e := kakarot.Wrap(kakarot.Op{}, greet)
	if r, _ := e(t.Context(), greetReq{Name: "x"}); r.Text != "hi x" {
		t.Fatal(r)
	}
}

func TestWrapShortCircuitError(t *testing.T) {
	deny := func(op kakarot.Op, next kakarot.Call) kakarot.Call {
		return func(ctx context.Context, req any) (any, error) {
			return nil, kakarot.E(kakarot.CodeForbidden, "no", nil)
		}
	}
	e := kakarot.Wrap(kakarot.Op{}, greet, deny)
	resp, err := e(t.Context(), greetReq{})
	if kakarot.CodeOf(err) != kakarot.CodeForbidden {
		t.Fatalf("err = %v", err)
	}
	if resp != (greetResp{}) {
		t.Fatalf("resp = %+v, want zero", resp)
	}
}

func TestWrapRejectsChangedRequestType(t *testing.T) {
	swap := func(op kakarot.Op, next kakarot.Call) kakarot.Call {
		return func(ctx context.Context, req any) (any, error) { return next(ctx, 42) }
	}
	_, err := kakarot.Wrap(kakarot.Op{Method: "Greet"}, greet, swap)(t.Context(), greetReq{})
	if err == nil {
		t.Fatal("want error")
	}
}

func TestAsError(t *testing.T) {
	nf := kakarot.NotFound("order", "1")
	wrapped := fmt.Errorf("repo: %w", nf)
	if kakarot.CodeOf(wrapped) != kakarot.CodeNotFound {
		t.Fatal("wrapped code lost")
	}
	plain := errors.New("db down")
	e := kakarot.AsError(plain)
	if e.Code != kakarot.CodeInternal || e.Message != "internal error" || !errors.Is(e, plain) {
		t.Fatalf("AsError(plain) = %+v", e)
	}
	if kakarot.AsError(nil) != nil {
		t.Fatal("AsError(nil) != nil")
	}
}
