// Package wsx serves kakarot bidirectional streams over WebSocket, one JSON
// message per frame.
package wsx

import (
	"context"
	"errors"
	"iter"
	"net/http"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/transport/httpx"
)

// Options configures the WebSocket upgrade.
type Options struct {
	Name           string   // operation name for middleware; default the pattern
	OriginPatterns []string // allowed cross-origin hosts; same-origin only by default
	Middleware     []kakarot.Middleware
}

// Handle registers a bidirectional stream on r at pattern ("GET /chat").
// In is decoded from each client frame, Out encoded into each server frame.
// A domain error ends the connection with a close reason carrying its code.
func Handle[In, Out any](r *httpx.Router, pattern string, s kakarot.BidiStream[In, Out], o Options) {
	name := o.Name
	if name == "" {
		name = pattern
	}
	op := kakarot.Op{Service: r.Service(), Method: name, Transport: "websocket"}
	s = kakarot.WrapBidiStream(op, s, append(r.EndpointMiddleware(), o.Middleware...)...)

	r.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{OriginPatterns: o.OriginPatterns})
		if err != nil {
			return // Accept already wrote the HTTP error
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		in := func(yield func(In, error) bool) {
			for {
				var m In
				err := wsjson.Read(ctx, conn, &m)
				if err != nil {
					if websocket.CloseStatus(err) == websocket.StatusNormalClosure || errors.Is(err, context.Canceled) {
						return
					}
					yield(m, kakarot.E(kakarot.CodeInvalid, "reading message", err))
					return
				}
				if !yield(m, nil) {
					return
				}
			}
		}
		send := func(m Out) error { return wsjson.Write(ctx, conn, m) }

		if err := s(ctx, iter.Seq2[In, error](in), send); err != nil {
			e := kakarot.AsError(err)
			reason := e.Code.String() + ": " + e.Message
			if len(reason) > 120 {
				reason = reason[:120]
			}
			_ = conn.Close(statusFor(e.Code), reason)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
}

func statusFor(c kakarot.Code) websocket.StatusCode {
	switch c {
	case kakarot.CodeInvalid:
		return websocket.StatusInvalidFramePayloadData
	case kakarot.CodeUnauthorized, kakarot.CodeForbidden:
		return websocket.StatusPolicyViolation
	case kakarot.CodeUnavailable:
		return websocket.StatusTryAgainLater
	default:
		return websocket.StatusInternalError
	}
}
