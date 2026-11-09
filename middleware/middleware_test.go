package middleware_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/middleware"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRecover(t *testing.T) {
	e := kakarot.Wrap(kakarot.Op{Method: "Boom"}, func(context.Context, int) (int, error) { panic("boom") },
		middleware.Recover(quiet))
	_, err := e(t.Context(), 1)
	if kakarot.CodeOf(err) != kakarot.CodeInternal {
		t.Fatalf("err = %v", err)
	}
}

func TestTimeout(t *testing.T) {
	slow := func(ctx context.Context, _ int) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	e := kakarot.Wrap(kakarot.Op{Method: "Slow"}, slow, middleware.Timeout(10*time.Millisecond))
	_, err := e(t.Context(), 1)
	if kakarot.CodeOf(err) != kakarot.CodeTimeout {
		t.Fatalf("err = %v", err)
	}
}

type v struct{ ok bool }

func (x v) Validate() error {
	if !x.ok {
		return kakarot.Invalid("bad", nil)
	}
	return nil
}

func TestValidated(t *testing.T) {
	e := middleware.Validated(func(context.Context, v) (string, error) { return "ok", nil })
	if _, err := e(t.Context(), v{ok: false}); kakarot.CodeOf(err) != kakarot.CodeInvalid {
		t.Fatalf("err = %v", err)
	}
	if out, err := e(t.Context(), v{ok: true}); err != nil || out != "ok" {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestDefaultStack(t *testing.T) {
	e := kakarot.Wrap(kakarot.Op{Method: "V"}, func(context.Context, v) (string, error) { panic("x") },
		middleware.Default(quiet, time.Second))
	if _, err := e(t.Context(), v{}); kakarot.CodeOf(err) != kakarot.CodeInternal {
		t.Fatalf("err = %v", err)
	}
}
