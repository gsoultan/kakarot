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
	return func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		*log = append(*log, name+">"+op.Method)
		err := next(ctx)
		*log = append(*log, name+"<")
		return err
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
	deny := func(context.Context, kakarot.Op, kakarot.Next) error {
		return kakarot.E(kakarot.CodeForbidden, "no", nil)
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

func TestRetryMiddlewareCallsNextTwice(t *testing.T) {
	calls := 0
	flaky := func(_ context.Context, r greetReq) (greetResp, error) {
		calls++
		if calls == 1 {
			return greetResp{}, errors.New("transient")
		}
		return greetResp{Text: "ok " + r.Name}, nil
	}
	retry := func(ctx context.Context, _ kakarot.Op, next kakarot.Next) error {
		if err := next(ctx); err != nil {
			return next(ctx)
		}
		return nil
	}
	resp, err := kakarot.Wrap(kakarot.Op{}, flaky, retry)(t.Context(), greetReq{Name: "x"})
	if err != nil || resp.Text != "ok x" || calls != 2 {
		t.Fatalf("resp=%+v err=%v calls=%d", resp, err, calls)
	}
}

func TestDecorateTyped(t *testing.T) {
	upper := func(_ kakarot.Op, next kakarot.Endpoint[greetReq, greetResp]) kakarot.Endpoint[greetReq, greetResp] {
		return func(ctx context.Context, r greetReq) (greetResp, error) {
			r.Name = "[" + r.Name + "]" // typed access, no casts
			return next(ctx, r)
		}
	}
	resp, _ := kakarot.Decorate(kakarot.Op{}, greet, upper)(t.Context(), greetReq{Name: "ada"})
	if resp.Text != "hi [ada]" {
		t.Fatal(resp.Text)
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

func TestLookupCode(t *testing.T) {
	custom := kakarot.NewCode("lookup_test_code")
	if c, ok := kakarot.LookupCode("lookup_test_code"); !ok || c != custom {
		t.Fatal(c, ok)
	}
	if c, ok := kakarot.LookupCode("not_found"); !ok || c != kakarot.CodeNotFound {
		t.Fatal(c, ok)
	}
	if _, ok := kakarot.LookupCode("nope"); ok {
		t.Fatal("unknown name found")
	}
}

func TestIsAndCodeOfAcceptNil(t *testing.T) {
	if kakarot.Is(nil, kakarot.CodeInternal) {
		t.Fatal("nil is not an internal error")
	}
	if kakarot.CodeOf(nil) != kakarot.CodeInternal { // must not panic
		t.Fatal("CodeOf(nil)")
	}
	wrapped := fmt.Errorf("lookup: %w", kakarot.NotFound("order", "1"))
	if !kakarot.Is(wrapped, kakarot.CodeNotFound) || kakarot.Is(wrapped, kakarot.CodeConflict) {
		t.Fatal("Is does not see the wrapped code")
	}
	if !kakarot.Is(errors.New("boom"), kakarot.CodeInternal) {
		t.Fatal("a plain error is internal")
	}
}
