package httpx_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/kakarot/transport/httpx"
)

type getItem struct {
	ID     string   `path:"id"`
	Limit  int      `query:"limit"`
	Tags   []string `query:"tag"`
	Tenant string   `header:"X-Tenant"`
}

type item struct {
	ID     string   `json:"id"`
	Limit  int      `json:"limit"`
	Tags   []string `json:"tags"`
	Tenant string   `json:"tenant"`
	Name   string   `json:"name,omitempty"`
}

type createItem struct {
	Name string `json:"name"`
}

func (c createItem) Validate() error {
	if c.Name == "" {
		return kakarot.Invalid("name is required", map[string]string{"name": "required"})
	}
	return nil
}

func newRouter() *httpx.Router {
	r := httpx.NewRouter()
	g := r.Group("items")
	httpx.Handle(g, "GET /items/{id}", func(_ context.Context, in getItem) (item, error) {
		if in.ID == "missing" {
			return item{}, kakarot.NotFound("item", in.ID)
		}
		return item{ID: in.ID, Limit: in.Limit, Tags: in.Tags, Tenant: in.Tenant}, nil
	})
	httpx.Handle(g, "POST /items", middleware.Validated(func(_ context.Context, in createItem) (item, error) {
		return item{ID: "1", Name: in.Name}, nil
	}))
	return r
}

func do(t *testing.T, h http.Handler, method, target, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, target, nil)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestDecodePathQueryHeader(t *testing.T) {
	w := do(t, newRouter(), "GET", "/items/42?limit=5&tag=a&tag=b", "", map[string]string{"X-Tenant": "acme"})
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var got item
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.ID != "42" || got.Limit != 5 || len(got.Tags) != 2 || got.Tenant != "acme" {
		t.Fatalf("got %+v", got)
	}
}

func TestBadQueryIsInvalid(t *testing.T) {
	w := do(t, newRouter(), "GET", "/items/42?limit=lots", "", nil)
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"limit"`) {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
}

func TestNotFoundMapsTo404(t *testing.T) {
	w := do(t, newRouter(), "GET", "/items/missing", "", nil)
	if w.Code != 404 || !strings.Contains(w.Body.String(), `"not_found"`) {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
}

func TestPostCreatedAndValidation(t *testing.T) {
	h := newRouter()
	if w := do(t, h, "POST", "/items", `{"name":"pen"}`, nil); w.Code != 201 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if w := do(t, h, "POST", "/items", `{"name":""}`, nil); w.Code != 400 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if w := do(t, h, "POST", "/items", `{"nope":1}`, nil); w.Code != 400 {
		t.Fatalf("unknown field: status %d", w.Code)
	}
}

func TestTransportMiddleware(t *testing.T) {
	r := newRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("X-Seen", "1")
			next.ServeHTTP(w, req)
		})
	})
	if w := do(t, r, "GET", "/items/1", "", nil); w.Header().Get("X-Seen") != "1" {
		t.Fatal("transport middleware not applied")
	}
}

func BenchmarkHandle(b *testing.B) {
	h := newRouter()
	req := httptest.NewRequest("GET", "/items/42?limit=5", nil)
	for b.Loop() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}
}

func TestOptions(t *testing.T) {
	r := httpx.NewRouter()
	var seen string
	spy := func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		seen = op.String()
		return next(ctx)
	}
	custom := func(*http.Request) (createItem, error) { return createItem{Name: "from-decoder"}, nil }
	httpx.HandleCodec(r.Group("items"), "PUT /items", func(_ context.Context, in createItem) (item, error) {
		return item{Name: in.Name}, nil
	}, custom, nil, httpx.Name("Replace"), httpx.Status(http.StatusAccepted), httpx.With(spy))

	w := do(t, r, "PUT", "/items", "", nil)
	if w.Code != http.StatusAccepted || !strings.Contains(w.Body.String(), "from-decoder") {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if seen != "items.Replace" {
		t.Fatalf("op = %q", seen)
	}
}
