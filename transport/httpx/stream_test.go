package httpx_test

import (
	"context"
	"iter"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/transport/httpx"
)

type feedReq struct {
	N    int `query:"n"`
	Fail int `query:"fail"`
}
type tick struct {
	I int `json:"i"`
}

func feed(_ context.Context, r feedReq) iter.Seq2[tick, error] {
	return func(yield func(tick, error) bool) {
		for i := range r.N {
			if r.Fail > 0 && i == r.Fail {
				yield(tick{}, kakarot.E(kakarot.CodeUnavailable, "upstream gone", nil))
				return
			}
			if !yield(tick{I: i}, nil) {
				return
			}
		}
	}
}

func streamRouter() *httpx.Router {
	r := httpx.NewRouter()
	r.Stream("GET /feed", feed)
	r.Upload("POST /sum", func(_ context.Context, in iter.Seq2[tick, error]) (tick, error) {
		total := 0
		for t, err := range in {
			if err != nil {
				return tick{}, err
			}
			total += t.I
		}
		return tick{I: total}, nil
	})
	return r
}

func TestNDJSONStream(t *testing.T) {
	w := do(t, streamRouter(), "GET", "/feed?n=3", "", nil)
	if w.Header().Get("Content-Type") != "application/x-ndjson" || w.Body.String() != "{\"i\":0}\n{\"i\":1}\n{\"i\":2}\n" {
		t.Fatalf("%q %q", w.Header().Get("Content-Type"), w.Body)
	}
}

func TestSSEStreamWithMidStreamError(t *testing.T) {
	w := do(t, streamRouter(), "GET", "/feed?n=5&fail=2", "", map[string]string{"Accept": "text/event-stream"})
	body := w.Body.String()
	if w.Code != 200 || !strings.HasPrefix(body, "data: {\"i\":0}\n\ndata: {\"i\":1}\n\nevent: error\n") ||
		!strings.Contains(body, "unavailable") {
		t.Fatalf("%d %q", w.Code, body)
	}
}

func TestStreamErrorBeforeFirstMessage(t *testing.T) {
	w := do(t, streamRouter(), "GET", "/feed?n=x", "", nil)
	if w.Code != 400 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestUploadNDJSON(t *testing.T) {
	w := do(t, streamRouter(), "POST", "/sum", "{\"i\":1}\n{\"i\":2}\n\n{\"i\":4}\n", nil)
	if w.Code != 201 || strings.TrimSpace(w.Body.String()) != `{"i":7}` {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := do(t, streamRouter(), "POST", "/sum", "{\"i\":1}\nnot json\n", nil); w.Code != 400 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

var _ = httptest.NewRecorder
