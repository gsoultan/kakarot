package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const svcSrc = `package service

import (
	"context"
	"iter"

	"example.com/demo/domain"
)

// Chat is a demo service with every method shape.
//
//kakarot:service chat
type ChatService interface {
	//kakarot:http POST /messages
	Send(ctx context.Context, in domain.Msg) (domain.Ack, error)
	//kakarot:http GET /messages
	Tail(ctx context.Context, in domain.Room) iter.Seq2[domain.Msg, error]
	//kakarot:http POST /messages/bulk
	Bulk(ctx context.Context, in iter.Seq2[domain.Msg, error]) (domain.Ack, error)
	//kakarot:http GET /ws
	Live(ctx context.Context, in iter.Seq2[domain.Msg, error], send func(domain.Msg) error) error
	Internal(ctx context.Context, in domain.Room) (domain.Ack, error)
}
`

const domainSrc = `package domain

import "errors"

type Msg struct{ Text string ` + "`json:\"text\"`" + ` }

func (m Msg) Validate() error {
	if m.Text == "" {
		return errors.New("empty")
	}
	return nil
}

type Room struct{ Name string ` + "`query:\"room\"`" + ` }
type Ack struct{ OK bool }
`

func TestGenerateCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a module")
	}
	_, self, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(self), "..", "..")
	dir := t.TempDir()
	write := func(p, s string) {
		t.Helper()
		p = filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	gomod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	// Reuse kakarot's requirements so the build needs no network.
	reqs := gomod[strings.Index(string(gomod), "require"):]
	write("go.mod", "module example.com/demo\n\ngo 1.27.3\n\nrequire github.com/gsoultan/kakarot v0.0.0\n\n"+string(reqs)+
		"\nreplace github.com/gsoultan/kakarot => "+root+"\n")
	sum, _ := os.ReadFile(filepath.Join(root, "go.sum"))
	write("go.sum", string(sum))
	write("domain/domain.go", domainSrc)
	write("service/service.go", svcSrc)

	if err := generate(filepath.Join(dir, "service")); err != nil {
		t.Fatal(err)
	}
	ep, _ := os.ReadFile(filepath.Join(dir, "endpoint", "endpoints_gen.go"))
	tr, _ := os.ReadFile(filepath.Join(dir, "transport", "http_gen.go"))
	for _, want := range []string{
		"Send     kakarot.Endpoint[domain.Msg, domain.Ack]",
		"Tail     kakarot.ServerStream[domain.Room, domain.Msg]",
		"Bulk     kakarot.ClientStream[domain.Msg, domain.Ack]",
		"Live     kakarot.BidiStream[domain.Msg, domain.Msg]",
		"Send:     middleware.Validated(svc.Send)",
		"func MakeChatEndpoints(svc service.ChatService) ChatEndpoints",
	} {
		if !strings.Contains(string(ep), want) {
			t.Errorf("endpoints_gen.go lacks %q:\n%s", want, ep)
		}
	}
	for _, want := range []string{
		`g := r.Group("chat", mws...)`,
		`g.Handle("POST /messages", eps.Send, httpx.Name("Send"))`,
		`g.Stream("GET /messages", eps.Tail, httpx.Name("Tail"))`,
		`g.Upload("POST /messages/bulk", eps.Bulk, httpx.Name("Bulk"))`,
		`wsx.Handle(g, "GET /ws", eps.Live, wsx.Options{Name: "Live"})`,
	} {
		if !strings.Contains(string(tr), want) {
			t.Errorf("http_gen.go lacks %q:\n%s", want, tr)
		}
	}
	if strings.Contains(string(tr), "Internal") {
		t.Error("method without //kakarot:http was routed")
	}

	cmd := exec.Command("go", "build", "-mod=mod", "./...")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated code does not build: %v\n%s", err, out)
	}
}

func TestGenerateRejectsBadShape(t *testing.T) {
	_, self, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(self), "..", "..")
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/bad\n\ngo 1.27.3\n\nrequire github.com/gsoultan/kakarot v0.0.0\n\nreplace github.com/gsoultan/kakarot => "+root+"\n"), 0o644)
	os.MkdirAll(filepath.Join(dir, "service"), 0o755)
	os.WriteFile(filepath.Join(dir, "service", "s.go"), []byte(`package service

//kakarot:service bad
type BadService interface {
	Do(a, b int) int
}
`), 0o644)
	err := generate(filepath.Join(dir, "service"))
	if err == nil || !strings.Contains(err.Error(), "Do") {
		t.Fatalf("err = %v", err)
	}
}

func TestScaffoldRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := scaffold("example.com/acme/notes", dir); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"go.mod", "service/service.go", "data/model/note.go", "cmd/server/main.go"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Error(err)
		}
	}
	mod, _ := os.ReadFile(filepath.Join(dir, "go.mod"))
	if !strings.Contains(string(mod), "module example.com/acme/notes") {
		t.Errorf("go.mod = %s", mod)
	}
	if err := scaffold("example.com/acme/notes", dir); err == nil {
		t.Fatal("second scaffold overwrote files")
	}
}
