package httpx_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gsoultan/kakarot/transport/httpx"
)

func TestOpenAPI(t *testing.T) {
	r := newRouter()
	r.Stream("GET /feed", feed)
	r.ServeOpenAPI("GET /openapi.json", httpx.Info{Title: "items", Version: "1.0.0"})
	w := do(t, r, "GET", "/openapi.json", "", nil)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	var doc map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	body := w.Body.String()
	for _, want := range []string{
		`"openapi": "3.1.0"`,
		`"/items/{id}"`,
		`"name": "id"`, `"in": "path"`,
		`"name": "tag"`, `"in": "query"`,
		`"name": "X-Tenant"`, `"in": "header"`,
		`"201"`, `"#/components/schemas/createItem"`,
		`"application/x-ndjson"`, `"text/event-stream"`,
		`"errorBody"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	paths := doc["paths"].(map[string]any)
	get := paths["/items/{id}"].(map[string]any)["get"].(map[string]any)
	if get["requestBody"] != nil {
		t.Error("GET has a request body")
	}
}
