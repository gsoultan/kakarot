package main

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// project writes a small stdlib-only kakarot project and returns its dir.
func project(t *testing.T, files map[string]string) string {
	t.Helper()
	_, self, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(self), "..", "..")
	dir := t.TempDir()
	files["go.mod"] = "module example.com/shop\n\ngo 1.27.4\n\nrequire github.com/gsoultan/kakarot v0.1.0\n\nreplace github.com/gsoultan/kakarot => " + root + "\n"
	for p, s := range files {
		p = filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// stderr captures what fn prints to os.Stderr.
func stderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	fn()
	w.Close()
	os.Stderr = old
	out, _ := io.ReadAll(r)
	return string(out)
}

const shopDomain = "package domain\n\ntype Item struct{ ID string `path:\"id\"` }\n"

const shopService = `package service

import (
	"context"

	"example.com/shop/domain"
)

//kakarot:service shop
type ShopService interface {
	//kakarot:http GET /items/{id}
	Get(ctx context.Context, in domain.Item) (domain.Item, error)
}
`

func TestCheckPassesThenFindsStaleCode(t *testing.T) {
	dir := project(t, map[string]string{"domain/domain.go": shopDomain, "service/service.go": shopService})
	if err := generate(filepath.Join(dir, "service")); err != nil {
		t.Fatal(err)
	}
	if err := check(dir); err != nil {
		t.Fatalf("a fresh project must pass: %v", err)
	}

	// The interface gains a route but nobody runs go generate.
	edited := strings.Replace(shopService, "\tGet(", "\tGet(ctx context.Context, in domain.Item) (domain.Item, error)\n\t//kakarot:http DELETE /items/{id}\n\tRemove(", 1)
	if err := os.WriteFile(filepath.Join(dir, "service", "service.go"), []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	var err error
	out := stderr(t, func() { err = check(dir) })
	if err == nil || !strings.Contains(out, "http_gen.go: out of date") || !strings.Contains(out, "go generate ./...") {
		t.Fatalf("err=%v\n%s", err, out)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "transport", "http_gen.go"))
	if strings.Contains(string(before), "Remove") {
		t.Fatal("check must not write files")
	}
}

func TestCheckReportsLayerViolations(t *testing.T) {
	dir := project(t, map[string]string{
		"domain/domain.go":   shopDomain,
		"service/service.go": shopService,
		"data/repo.go":       "package data\n\ntype Repo struct{}\n",
		"usecase/get.go": `package usecase

import (
	"net/http"

	"example.com/shop/data"
)

var _ = http.StatusOK
var _ data.Repo
`,
	})
	if err := generate(filepath.Join(dir, "service")); err != nil {
		t.Fatal(err)
	}
	var err error
	out := stderr(t, func() { err = check(dir) })
	if err == nil || !strings.Contains(err.Error(), "2 problem(s)") {
		t.Fatalf("err=%v\n%s", err, out)
	}
	for _, want := range []string{
		"get.go:4: usecase must not import net/http",
		"get.go:6: usecase must not import data/",
		"only through ports",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestDirectiveMistakesAreExplained(t *testing.T) {
	for doc, want := range map[string]string{
		"kakarot:htp GET /x":     "unknown directive //kakarot:htp (did you mean //kakarot:http?)",
		"kakarot:rest GET /x":    "known directives are",
		"kakarot:http /orders":   "for example //kakarot:http GET /orders/{id}",
		"kakarot:http FETCH /x":  "want a method and a path",
		"kakarot:queue":          "want one topic name",
		"kakarot:grpc streaming": "takes no argument",
	} {
		err := checkDirectives("Get reads one.\n" + doc)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want %q", doc, err, want)
		}
	}
	if err := checkDirectives("kakarot:http GET /orders/{id}\nkakarot:grpc\nkakarot:queue order.placed"); err != nil {
		t.Fatal(err)
	}
}

func TestBadShapeErrorShowsTheShapesAndPosition(t *testing.T) {
	dir := project(t, map[string]string{"service/s.go": "package service\n\n//kakarot:service bad\ntype BadService interface {\n\tDo(a, b int) int\n}\n"})
	err := generate(filepath.Join(dir, "service"))
	if err == nil || !strings.Contains(err.Error(), "s.go:5") || !strings.Contains(err.Error(), "(ctx context.Context, in Req) (Resp, error)") {
		t.Fatalf("err = %v", err)
	}
}

func TestUnknownCommandShowsHelp(t *testing.T) {
	err := run([]string{"generate"})
	if err == nil || !strings.Contains(err.Error(), `unknown command "generate"`) || !strings.Contains(err.Error(), "kakarot check") {
		t.Fatalf("err = %v", err)
	}
}
