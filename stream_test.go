package kakarot_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/gsoultan/kakarot"
)

func count(_ context.Context, n int) iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		for i := range n {
			if !yield(i, nil) {
				return
			}
		}
	}
}

func TestWrapServerStream(t *testing.T) {
	var log []string
	s := kakarot.WrapServerStream(kakarot.Op{Method: "Count"}, count, trace(&log, "m"))
	got, err := kakarot.Collect(s(t.Context(), 3))
	if err != nil || len(got) != 3 {
		t.Fatalf("got=%v err=%v", got, err)
	}
	if len(log) != 2 {
		t.Fatalf("middleware ran %v", log)
	}

	// early stop by the consumer is not an error
	for v := range s(t.Context(), 10) {
		if v == 1 {
			break
		}
	}
}

func TestWrapServerStreamError(t *testing.T) {
	boom := errors.New("boom")
	bad := func(context.Context, int) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			if yield(1, nil) {
				yield(0, boom)
			}
		}
	}
	var seen error
	spy := func(ctx context.Context, _ kakarot.Op, next kakarot.Next) error { seen = next(ctx); return seen }
	got, err := kakarot.Collect(kakarot.WrapServerStream(kakarot.Op{}, bad, spy)(t.Context(), 0))
	if !errors.Is(err, boom) || !errors.Is(seen, boom) || len(got) != 1 {
		t.Fatalf("got=%v err=%v seen=%v", got, err, seen)
	}
}

func TestClientAndBidi(t *testing.T) {
	sum := func(_ context.Context, in iter.Seq2[int, error]) (int, error) {
		total := 0
		for v, err := range in {
			if err != nil {
				return 0, err
			}
			total += v
		}
		return total, nil
	}
	if v, err := kakarot.WrapClientStream(kakarot.Op{}, sum)(t.Context(), kakarot.SliceStream([]int{1, 2, 3})); err != nil || v != 6 {
		t.Fatalf("v=%d err=%v", v, err)
	}
	echo := func(_ context.Context, in iter.Seq2[string, error], send func(string) error) error {
		for v, err := range in {
			if err != nil {
				return err
			}
			if err := send(v + "!"); err != nil {
				return err
			}
		}
		return nil
	}
	var out []string
	err := kakarot.WrapBidiStream(kakarot.Op{}, echo)(t.Context(), kakarot.SliceStream([]string{"a", "b"}),
		func(s string) error { out = append(out, s); return nil })
	if err != nil || len(out) != 2 || out[1] != "b!" {
		t.Fatalf("out=%v err=%v", out, err)
	}
}
