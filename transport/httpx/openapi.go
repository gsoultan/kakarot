package httpx

import (
	"encoding/json"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type routeKind uint8

const (
	kindUnary routeKind = iota
	kindStream
	kindUpload
)

type routeInfo struct {
	pattern, service, name string
	status                 int
	req, resp              reflect.Type
	kind                   routeKind
}

type routeTable struct {
	mu     sync.Mutex
	routes []routeInfo
}

func (t *routeTable) add(ri routeInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.routes = append(t.routes, ri)
}

// Info is the OpenAPI info object.
type Info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

// Schema is a JSON Schema (OpenAPI 3.1 dialect) subset.
type Schema struct {
	Ref                  string             `json:"$ref,omitempty"`
	Type                 any                `json:"type,omitempty"` // string or [type, "null"]
	Format               string             `json:"format,omitempty"`
	Properties           map[string]*Schema `json:"properties,omitempty"`
	Required             []string           `json:"required,omitempty"`
	Items                *Schema            `json:"items,omitempty"`
	AdditionalProperties *Schema            `json:"additionalProperties,omitempty"`
}

// Document is an OpenAPI 3.1 document.
type Document struct {
	OpenAPI    string                           `json:"openapi"`
	Info       Info                             `json:"info"`
	Paths      map[string]map[string]*operation `json:"paths"`
	Components struct {
		Schemas map[string]*Schema `json:"schemas"`
	} `json:"components"`
}

type operation struct {
	OperationID string               `json:"operationId,omitempty"`
	Tags        []string             `json:"tags,omitempty"`
	Parameters  []parameter          `json:"parameters,omitempty"`
	RequestBody *body                `json:"requestBody,omitempty"`
	Responses   map[string]*response `json:"responses"`
}

type parameter struct {
	Name     string  `json:"name"`
	In       string  `json:"in"`
	Required bool    `json:"required,omitempty"`
	Schema   *Schema `json:"schema"`
}

type body struct {
	Required bool                 `json:"required,omitempty"`
	Content  map[string]mediaType `json:"content"`
}

type response struct {
	Description string               `json:"description"`
	Content     map[string]mediaType `json:"content,omitempty"`
}

type mediaType struct {
	Schema *Schema `json:"schema"`
}

// OpenAPI builds an OpenAPI 3.1 document from every route registered on the
// router and its groups. It is derived from the endpoint types, so it cannot
// drift from the code.
func (r *Router) OpenAPI(info Info) *Document {
	g := &schemaGen{defs: map[string]*Schema{}, names: map[reflect.Type]string{}}
	doc := &Document{OpenAPI: "3.1.0", Info: info, Paths: map[string]map[string]*operation{}}
	errRef := g.schema(reflect.TypeFor[errorBody]())

	r.routes.mu.Lock()
	routes := slices.Clone(r.routes.routes)
	r.routes.mu.Unlock()

	for _, rt := range routes {
		method, path, _ := strings.Cut(rt.pattern, " ")
		if path == "" {
			method, path = "", method
		}
		path = strings.ReplaceAll(path, "...}", "}")
		path = strings.ReplaceAll(path, "{$}", "")
		op := &operation{OperationID: opID(rt), Responses: map[string]*response{}}
		if rt.service != "" {
			op.Tags = []string{rt.service}
		}
		bodyType := rt.req
		if rt.kind != kindUpload {
			params, rest := g.params(rt.req)
			op.Parameters = params
			bodyType = rest
		}
		if bodyType != nil && method != http.MethodGet && method != http.MethodDelete && method != http.MethodHead {
			ct := "application/json"
			s := g.schema(bodyType)
			if rt.kind == kindUpload {
				ct = "application/x-ndjson"
			}
			op.RequestBody = &body{Required: true, Content: map[string]mediaType{ct: {Schema: s}}}
		}
		ok := &response{Description: http.StatusText(rt.status)}
		if rt.status != http.StatusNoContent {
			s := g.schema(rt.resp)
			if rt.kind == kindStream {
				ok.Content = map[string]mediaType{"application/x-ndjson": {Schema: s}, "text/event-stream": {Schema: s}}
			} else {
				ok.Content = map[string]mediaType{"application/json": {Schema: s}}
			}
		}
		op.Responses[strconv.Itoa(rt.status)] = ok
		op.Responses["default"] = &response{Description: "Error", Content: map[string]mediaType{"application/json": {Schema: errRef}}}

		m := strings.ToLower(method)
		if m == "" {
			m = "get"
		}
		if doc.Paths[path] == nil {
			doc.Paths[path] = map[string]*operation{}
		}
		doc.Paths[path][m] = op
	}
	doc.Components.Schemas = g.defs
	return doc
}

// ServeOpenAPI serves the document as JSON at pattern ("GET /openapi.json").
// The document is built once, on first request, after all routes exist.
func (r *Router) ServeOpenAPI(pattern string, info Info) {
	var once sync.Once
	var b []byte
	r.mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { b, _ = json.MarshalIndent(r.OpenAPI(info), "", "  ") })
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	})
}

var nonWord = regexp.MustCompile(`[^A-Za-z0-9]+`)

func opID(rt routeInfo) string {
	if rt.name != rt.pattern {
		if rt.service != "" {
			return rt.service + "." + rt.name
		}
		return rt.name
	}
	return strings.Trim(nonWord.ReplaceAllString(rt.pattern, "_"), "_")
}

type schemaGen struct {
	defs  map[string]*Schema
	names map[reflect.Type]string
}

var timeType = reflect.TypeFor[time.Time]()

// params splits a request struct into path/query/header parameters and
// reports whether any JSON body fields remain (returned as the body type).
func (g *schemaGen) params(t reflect.Type) ([]parameter, reflect.Type) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, t
	}
	var out []parameter
	bodyFields := 0
	for _, f := range reflect.VisibleFields(t) {
		if !f.IsExported() || f.Anonymous {
			continue
		}
		found := false
		for _, in := range []string{"path", "query", "header"} {
			if name, ok := f.Tag.Lookup(in); ok && name != "-" {
				out = append(out, parameter{Name: name, In: in, Required: in == "path", Schema: g.schema(f.Type)})
				found = true
			}
		}
		if !found && f.Tag.Get("json") != "-" {
			bodyFields++
		}
	}
	if bodyFields == 0 {
		return out, nil
	}
	return out, t
}

func (g *schemaGen) schema(t reflect.Type) *Schema {
	nullable := false
	for t.Kind() == reflect.Pointer {
		t, nullable = t.Elem(), true
	}
	s := g.base(t)
	if nullable && s.Ref == "" {
		s.Type = []any{s.Type, "null"}
	}
	return s
}

func (g *schemaGen) base(t reflect.Type) *Schema {
	if t == timeType {
		return &Schema{Type: "string", Format: "date-time"}
	}
	switch t.Kind() {
	case reflect.String:
		return &Schema{Type: "string"}
	case reflect.Bool:
		return &Schema{Type: "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return &Schema{Type: "integer", Format: "int32"}
	case reflect.Int64, reflect.Uint64:
		return &Schema{Type: "integer", Format: "int64"}
	case reflect.Float32, reflect.Float64:
		return &Schema{Type: "number"}
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return &Schema{Type: "string", Format: "byte"}
		}
		return &Schema{Type: "array", Items: g.schema(t.Elem())}
	case reflect.Map:
		return &Schema{Type: "object", AdditionalProperties: g.schema(t.Elem())}
	case reflect.Struct:
		return g.object(t)
	}
	return &Schema{}
}

func (g *schemaGen) object(t reflect.Type) *Schema {
	name := t.Name()
	if name != "" {
		if n, ok := g.names[t]; ok {
			return &Schema{Ref: "#/components/schemas/" + n}
		}
		n := nonWord.ReplaceAllString(name, "_")
		for i := 2; g.defs[n] != nil; i++ {
			n = nonWord.ReplaceAllString(name, "_") + strconv.Itoa(i)
		}
		g.names[t] = n
		g.defs[n] = &Schema{} // placeholder for recursion
		g.defs[n] = g.fields(t)
		return &Schema{Ref: "#/components/schemas/" + n}
	}
	return g.fields(t)
}

func (g *schemaGen) fields(t reflect.Type) *Schema {
	s := &Schema{Type: "object", Properties: map[string]*Schema{}}
	for _, f := range reflect.VisibleFields(t) {
		if !f.IsExported() || f.Anonymous {
			continue
		}
		if hasAny(f.Tag, "path", "query", "header") {
			continue
		}
		name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = f.Name
		}
		s.Properties[name] = g.schema(f.Type)
		if !strings.Contains(opts, "omitempty") && !strings.Contains(opts, "omitzero") && f.Type.Kind() != reflect.Pointer {
			s.Required = append(s.Required, name)
		}
	}
	return s
}

func hasAny(tag reflect.StructTag, keys ...string) bool {
	for _, k := range keys {
		if _, ok := tag.Lookup(k); ok {
			return true
		}
	}
	return false
}
