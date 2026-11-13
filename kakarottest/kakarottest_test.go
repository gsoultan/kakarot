package kakarottest_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/transport/httpx"
)

type greet struct {
	Name string `json:"name"`
	Lang string `header:"Accept-Language" json:"-"`
}

func (g greet) Validate() error {
	if g.Name == "" {
		return kakarot.Invalid("invalid greeting", map[string]string{"name": "required"})
	}
	return nil
}

type greeting struct {
	Text string `json:"text"`
}

func router() *httpx.Router {
	r := httpx.NewRouter()
	r.Handle("POST /greet", middleware.Validated(func(_ context.Context, in greet) (greeting, error) {
		if in.Lang == "id" {
			return greeting{Text: "halo " + in.Name}, nil
		}
		return greeting{Text: "hello " + in.Name}, nil
	}))
	return r
}

func TestDoDecodesBodyAndError(t *testing.T) {
	res := kakarottest.Do[greeting](t, router(), "POST", "/greet", greet{Name: "ana"}, "Accept-Language", "id")
	if res.Status != http.StatusCreated || res.Body.Text != "halo ana" {
		t.Fatalf("%d %+v", res.Status, res.Body)
	}
	bad := kakarottest.Do[greeting](t, router(), "POST", "/greet", `{"name":""}`)
	if bad.Status != http.StatusBadRequest || bad.Error.Code != "invalid" || bad.Error.Fields["name"] != "required" {
		t.Fatalf("%d %+v", bad.Status, bad.Error)
	}
	if res := kakarottest.Do[kakarottest.None](t, router(), "GET", "/nope", nil); res.Status != http.StatusNotFound {
		t.Fatalf("status = %d", res.Status)
	}
}

func TestTxCountsAndRollsBack(t *testing.T) {
	balance, saved := 100, 0
	tx := &kakarottest.Tx{
		OnBegin:    func() { saved = balance },
		OnRollback: func() { balance = saved },
	}
	boom := errors.New("boom")
	err := tx.Within(t.Context(), func(ctx context.Context) error {
		balance -= 30
		return tx.Within(ctx, func(context.Context) error { return boom }) // joins the outer one
	})
	if !errors.Is(err, boom) || balance != 100 || tx.Rollbacks() != 1 || tx.Commits() != 0 {
		t.Fatalf("err=%v balance=%d rollbacks=%d commits=%d", err, balance, tx.Rollbacks(), tx.Commits())
	}
	if err := tx.Within(t.Context(), func(context.Context) error { balance -= 30; return nil }); err != nil || balance != 70 || tx.Commits() != 1 {
		t.Fatalf("err=%v balance=%d commits=%d", err, balance, tx.Commits())
	}
	tx.FailCommit = kakarot.E(kakarot.CodeUnavailable, "connection lost at commit", nil)
	err = tx.Within(t.Context(), func(context.Context) error { balance = 0; return nil })
	kakarottest.WantCode(t, err, kakarot.CodeUnavailable)
	if balance != 70 {
		t.Fatalf("a failed commit must roll back, balance = %d", balance)
	}
}

func TestWantFieldAndClock(t *testing.T) {
	kakarottest.WantField(t, greet{}.Validate(), "name")
	c := kakarottest.NewClock(time.Unix(0, 0))
	c.Advance(90 * time.Second)
	if got := c.Now().Unix(); got != 90 {
		t.Fatalf("clock = %d", got)
	}
}

func TestServePublishAndEventually(t *testing.T) {
	const topic queue.Topic[greet] = "greeted"
	broker := &queue.Memory{}
	consumer := &queue.Consumer{Service: "demo", Source: broker}
	var seen atomic.Int32
	queue.Handle(consumer, topic, "Count", func(_ context.Context, g greet) (greeting, error) {
		if g.Name == "" {
			return greeting{}, kakarot.E(kakarot.CodeUnavailable, "try again", nil)
		}
		seen.Add(1)
		return greeting{}, nil
	})

	var ticks atomic.Int32
	job := &kakarot.Job{Name: "tick", Every: 5 * time.Millisecond, Run: func(context.Context) error {
		ticks.Add(1)
		return errors.New("a failing run does not stop the job")
	}}
	kakarottest.Serve(t, consumer, job)

	if err := kakarottest.Publish(t, broker, topic, "k", greet{Name: "ana"}); err != nil {
		t.Fatal(err)
	}
	// The handler's error comes back: a broker would deliver the message again.
	kakarottest.WantCode(t, kakarottest.Publish(t, broker, topic, "k", greet{}), kakarot.CodeUnavailable)
	if seen.Load() != 1 {
		t.Fatalf("seen = %d", seen.Load())
	}
	kakarottest.Eventually(t, 5*time.Second, "three ticks", func() bool { return ticks.Load() >= 3 })
}

func TestJobShutdownBeforeServeAndTwice(t *testing.T) {
	j := &kakarot.Job{Name: "idle", Every: time.Hour, Run: func(context.Context) error { return nil }}
	if err := j.Shutdown(t.Context()); err != nil { // never served: nothing to wait for
		t.Fatal(err)
	}
	if err := j.Serve(t.Context()); err != nil { // stopped already: returns at once
		t.Fatal(err)
	}
	if err := j.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := (&kakarot.Job{Name: "empty"}).Serve(t.Context()); err == nil {
		t.Fatal("a job with no interval must not start")
	}
}
