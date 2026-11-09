// Package httpx binds kakarot endpoints to HTTP on the standard library's
// http.ServeMux (method and path patterns such as "GET /orders/{id}").
package httpx

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gsoultan/kakarot"
)

// Router is an http.ServeMux plus transport-level middleware. Transport
// middleware is plain func(http.Handler) http.Handler, so any net/http or chi
// middleware works unchanged.
type Router struct {
	mux     *http.ServeMux
	mws     []func(http.Handler) http.Handler
	service string
	epMws   []kakarot.Middleware
}

// NewRouter returns an empty router.
func NewRouter() *Router { return &Router{mux: http.NewServeMux()} }

// Use adds transport middleware, applied to every route (outermost first).
func (r *Router) Use(mws ...func(http.Handler) http.Handler) { r.mws = append(r.mws, mws...) }

// Group returns a view of the router whose routes share a service name and
// endpoint middleware.
func (r *Router) Group(service string, mws ...kakarot.Middleware) *Router {
	return &Router{mux: r.mux, service: service, epMws: append(append([]kakarot.Middleware{}, r.epMws...), mws...)}
}

// HandleFunc registers a plain handler (health checks, static files).
func (r *Router) HandleFunc(pattern string, h http.HandlerFunc) { r.mux.Handle(pattern, h) }

// ServeHTTP applies transport middleware and dispatches.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var h http.Handler = r.mux
	for i := len(r.mws) - 1; i >= 0; i-- {
		h = r.mws[i](h)
	}
	h.ServeHTTP(w, req)
}

type options struct {
	name   string
	status int
	onErr  ErrorEncoder
	mws    []kakarot.Middleware
}

// Option configures one route. Options are not generic, so they never need
// type arguments at the call site.
type Option func(*options)

// Name sets the operation name middleware sees; default is the pattern.
func Name(method string) Option { return func(o *options) { o.name = method } }

// Status sets the success status; default 200 (201 for POST).
func Status(code int) Option { return func(o *options) { o.status = code } }

// Errors replaces the error encoder.
func Errors(e ErrorEncoder) Option { return func(o *options) { o.onErr = e } }

// With adds endpoint middleware for this route only, inside the group's.
func With(mws ...kakarot.Middleware) Option {
	return func(o *options) { o.mws = append(o.mws, mws...) }
}

// Handle binds an endpoint to a pattern such as "POST /orders", with the
// tag-based decoder and the JSON encoder.
//
//	httpx.Handle(r, "GET /orders/{id}", eps.GetOrder)
//
// Req and Resp are inferred from the endpoint, so the route is typed end to
// end: no casts, no framework context type in business code.
func Handle[Req, Resp any](r *Router, pattern string, e kakarot.Endpoint[Req, Resp], opts ...Option) {
	HandleCodec(r, pattern, e, nil, nil, opts...)
}

// HandleCodec is Handle with a custom decoder and/or encoder (nil keeps the
// default). Their types must match the endpoint's, checked by the compiler.
func HandleCodec[Req, Resp any](r *Router, pattern string, e kakarot.Endpoint[Req, Resp], decode DecodeFunc[Req], encode EncodeFunc[Resp], opts ...Option) {
	o := options{name: pattern, onErr: WriteError}
	if strings.HasPrefix(pattern, http.MethodPost+" ") {
		o.status = http.StatusCreated
	} else {
		o.status = http.StatusOK
	}
	for _, opt := range opts {
		opt(&o)
	}
	if decode == nil {
		decode = DefaultDecoder[Req]()
	}
	if encode == nil {
		encode = JSONEncoder[Resp]
	}

	op := kakarot.Op{Service: r.service, Method: o.name, Transport: "http"}
	e = kakarot.Wrap(op, e, append(append([]kakarot.Middleware{}, r.epMws...), o.mws...)...)

	r.mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		in, err := decode(req)
		if err != nil {
			o.onErr(ctx, w, err)
			return
		}
		out, err := e(ctx, in)
		if err != nil {
			o.onErr(ctx, w, err)
			return
		}
		if err := encode(ctx, w, o.status, out); err != nil {
			o.onErr(ctx, w, err)
		}
	})
}

// Server adapts an http.Server to kakarot.Server.
type Server struct {
	HTTP *http.Server
}

// NewServer returns a server for h on addr with safe timeouts.
func NewServer(addr string, h http.Handler) *Server {
	return &Server{HTTP: &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}}
}

// Serve listens until Shutdown is called.
func (s *Server) Serve(ctx context.Context) error {
	s.HTTP.BaseContext = func(net.Listener) context.Context { return context.WithoutCancel(ctx) }
	err := s.HTTP.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown drains in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error { return s.HTTP.Shutdown(ctx) }
