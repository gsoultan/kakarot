package httpx

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"strings"

	"github.com/gsoultan/kakarot"
)

// Stream binds a server stream. The response format follows the Accept
// header: "text/event-stream" gets Server-Sent Events, anything else gets
// NDJSON (one JSON object per line). Each message is flushed as it is sent.
//
// An error before the first message is a normal error response; an error
// later is sent in-band: an SSE "error" event, or an NDJSON line
// {"error":{...}}.
func (r *Router) Stream[Req, Msg any](pattern string, s kakarot.ServerStream[Req, Msg], opts ...Option) {
	o := r.routeOptions(pattern, opts)
	op := kakarot.Op{Service: r.service, Method: o.name, Transport: "http"}
	s = kakarot.WrapServerStream(op, s, append(append([]kakarot.Middleware{}, r.epMws...), o.mws...)...)
	decode := DefaultDecoder[Req]()

	r.mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		in, err := decode(req)
		if err != nil {
			o.onErr(ctx, w, err)
			return
		}
		sse := strings.Contains(req.Header.Get("Accept"), "text/event-stream")
		rc := http.NewResponseController(w)
		started := false
		start := func() {
			if sse {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
			} else {
				w.Header().Set("Content-Type", "application/x-ndjson")
			}
			w.WriteHeader(http.StatusOK)
			started = true
		}
		for msg, err := range s(ctx, in) {
			if err != nil {
				if !started {
					o.onErr(ctx, w, err)
					return
				}
				writeStreamError(ctx, w, sse, err)
				_ = rc.Flush()
				return
			}
			if !started {
				start()
			}
			b, err := json.Marshal(msg)
			if err != nil {
				writeStreamError(ctx, w, sse, err)
				return
			}
			if sse {
				_, err = fmt.Fprintf(w, "data: %s\n\n", b)
			} else {
				_, err = fmt.Fprintf(w, "%s\n", b)
			}
			if err != nil || rc.Flush() != nil {
				return // client went away
			}
		}
		if !started {
			start()
		}
	})
}

func writeStreamError(ctx context.Context, w http.ResponseWriter, sse bool, err error) {
	e := kakarot.AsError(err)
	var body errorBody
	body.Error.Code, body.Error.Message, body.Error.Fields = e.Code.String(), e.Message, e.Fields
	b, _ := json.Marshal(body)
	if sse {
		fmt.Fprintf(w, "event: error\ndata: %s\n\n", b)
	} else {
		fmt.Fprintf(w, "%s\n", b)
	}
}

// Upload binds a client stream: the request body is NDJSON, one Msg per
// line, decoded lazily as the endpoint iterates.
func (r *Router) Upload[Msg, Resp any](pattern string, s kakarot.ClientStream[Msg, Resp], opts ...Option) {
	o := r.routeOptions(pattern, opts)
	op := kakarot.Op{Service: r.service, Method: o.name, Transport: "http"}
	s = kakarot.WrapClientStream(op, s, append(append([]kakarot.Middleware{}, r.epMws...), o.mws...)...)

	r.mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		resp, err := s(ctx, NDJSON[Msg](req))
		if err != nil {
			o.onErr(ctx, w, err)
			return
		}
		if err := JSONEncoder(ctx, w, o.status, resp); err != nil {
			o.onErr(ctx, w, err)
		}
	})
}

// NDJSON reads a request body of newline-delimited JSON as a typed stream.
func NDJSON[Msg any](req *http.Request) iter.Seq2[Msg, error] {
	return func(yield func(Msg, error) bool) {
		sc := bufio.NewScanner(req.Body)
		sc.Buffer(make([]byte, 64<<10), MaxBodyBytes)
		line := 0
		for sc.Scan() {
			line++
			b := sc.Bytes()
			if len(strings.TrimSpace(string(b))) == 0 {
				continue
			}
			var m Msg
			if err := json.Unmarshal(b, &m); err != nil {
				yield(m, kakarot.E(kakarot.CodeInvalid, fmt.Sprintf("line %d: malformed JSON", line), err))
				return
			}
			if !yield(m, nil) {
				return
			}
		}
		if err := sc.Err(); err != nil {
			var zero Msg
			yield(zero, kakarot.E(kakarot.CodeInvalid, "reading body", err))
		}
	}
}
