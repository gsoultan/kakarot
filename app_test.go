package kakarot_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/gsoultan/kakarot"
)

type fakeServer struct {
	fail     error
	stop     chan struct{}
	shutdown bool
}

func (s *fakeServer) Serve(ctx context.Context) error {
	if s.fail != nil {
		return s.fail
	}
	<-s.stop
	return nil
}

func (s *fakeServer) Shutdown(context.Context) error {
	s.shutdown = true
	close(s.stop)
	return nil
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestAppCancelIsCleanStop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	s := &fakeServer{stop: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- (&kakarot.App{Servers: []kakarot.Server{s}, Logger: quiet}).Run(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("err = %v", err)
	}
	if !s.shutdown {
		t.Fatal("server not shut down")
	}
}

func TestAppServerFailureStopsOthers(t *testing.T) {
	boom := errors.New("bind failed")
	ok := &fakeServer{stop: make(chan struct{})}
	bad := &fakeServer{fail: boom, stop: make(chan struct{})}
	err := (&kakarot.App{Servers: []kakarot.Server{ok, bad}, Logger: quiet}).Run(t.Context())
	if !errors.Is(err, boom) || !ok.shutdown {
		t.Fatalf("err = %v, shutdown = %v", err, ok.shutdown)
	}
}
