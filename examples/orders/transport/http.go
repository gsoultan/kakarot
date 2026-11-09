// Package transport binds the order endpoints to HTTP.
package transport

import (
	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/endpoint"
	"github.com/gsoultan/kakarot/transport/httpx"
)

// RegisterHTTP mounts the order routes. mws are endpoint middleware shared by
// every route in the group.
func RegisterHTTP(r *httpx.Router, eps endpoint.Endpoints, mws ...kakarot.Middleware) {
	g := r.Group("order", mws...)
	httpx.Handle(g, "POST /orders", eps.PlaceOrder, httpx.Name("PlaceOrder"))
	httpx.Handle(g, "GET /orders/{id}", eps.GetOrder, httpx.Name("GetOrder"))
}
