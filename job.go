package kakarot

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Job runs a function on a fixed interval. It is a Server: add it to
// App.Servers and it starts and stops with the application.
//
//	expire := &kakarot.Job{Name: "expire-orders", Every: time.Minute, Run: expireOrders.Run}
//
// Runs never overlap: the next tick waits for the current run. A failed run
// is logged and the job carries on.
type Job struct {
	Name   string
	Every  time.Duration
	Run    func(ctx context.Context) error
	Logger *slog.Logger // default slog.Default()

	mu      sync.Mutex
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	started bool
}

// channels returns the stop and done channels. serving marks that Serve has
// started, so Shutdown knows whether there is anything to wait for.
func (j *Job) channels(serving bool) (stop, done chan struct{}, started bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.stop == nil {
		j.stop, j.done = make(chan struct{}), make(chan struct{})
	}
	j.started = j.started || serving
	return j.stop, j.done, j.started
}

// Serve runs the job until Shutdown or until ctx ends.
func (j *Job) Serve(ctx context.Context) error {
	if j.Every <= 0 || j.Run == nil {
		return E(CodeInternal, "kakarot: Job "+j.Name+" needs Every and Run", nil)
	}
	stop, done, _ := j.channels(true)
	defer close(done)
	log := j.Logger
	if log == nil {
		log = slog.Default()
	}
	t := time.NewTicker(j.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stop:
			return nil
		case <-t.C:
			if err := j.Run(ctx); err != nil {
				log.ErrorContext(ctx, "job failed", "job", j.Name, "err", err)
			}
		}
	}
}

// Shutdown stops the job after its current run, or when ctx ends.
func (j *Job) Shutdown(ctx context.Context) error {
	stop, done, started := j.channels(false)
	j.once.Do(func() { close(stop) })
	if !started {
		return nil // Serve never ran; if it starts later it stops at once
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
