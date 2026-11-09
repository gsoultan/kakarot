package wsx_test

import (
	"context"
	"iter"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/transport/httpx"
	"github.com/gsoultan/kakarot/transport/wsx"
)

type chatIn struct {
	Text string `json:"text"`
}
type chatOut struct {
	Echo string `json:"echo"`
}

func chat(_ context.Context, in iter.Seq2[chatIn, error], send func(chatOut) error) error {
	for m, err := range in {
		if err != nil {
			return err
		}
		if m.Text == "bad" {
			return kakarot.Invalid("bad word", nil)
		}
		if err := send(chatOut{Echo: strings.ToUpper(m.Text)}); err != nil {
			return err
		}
	}
	return nil
}

func TestWebSocketBidi(t *testing.T) {
	r := httpx.NewRouter()
	wsx.Handle(r, "GET /chat", chat, wsx.Options{})
	srv := httptest.NewServer(r)
	defer srv.Close()

	ctx := t.Context()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	for _, w := range []string{"hi", "there"} {
		if err := wsjson.Write(ctx, conn, chatIn{Text: w}); err != nil {
			t.Fatal(err)
		}
		var out chatOut
		if err := wsjson.Read(ctx, conn, &out); err != nil {
			t.Fatal(err)
		}
		if out.Echo != strings.ToUpper(w) {
			t.Fatalf("echo %q", out.Echo)
		}
	}
	_ = wsjson.Write(ctx, conn, chatIn{Text: "bad"})
	var out chatOut
	err = wsjson.Read(ctx, conn, &out)
	if websocket.CloseStatus(err) != websocket.StatusInvalidFramePayloadData {
		t.Fatalf("close err = %v", err)
	}
}
