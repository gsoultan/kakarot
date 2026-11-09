// Package endpoint turns OrderService methods into typed endpoints.
package endpoint

import (
	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/service"
	"github.com/gsoultan/kakarot/middleware"
)

// Endpoints holds one typed endpoint or stream per service method.
type Endpoints struct {
	PlaceOrder   kakarot.Endpoint[domain.PlaceOrder, domain.Order]
	GetOrder     kakarot.Endpoint[domain.GetOrder, domain.Order]
	ListOrders   kakarot.Endpoint[domain.ListOrders, kakarot.Page[domain.Order]]
	ExportOrders kakarot.ServerStream[domain.ExportOrders, domain.Order]
}

// Make builds the endpoints. A method value already has the Endpoint shape.
// Validated only compiles for requests with a Validate method.
func Make(svc service.OrderService) Endpoints {
	return Endpoints{
		PlaceOrder:   middleware.Validated(svc.PlaceOrder),
		GetOrder:     svc.GetOrder,
		ListOrders:   middleware.Validated(svc.ListOrders),
		ExportOrders: svc.ExportOrders,
	}
}
