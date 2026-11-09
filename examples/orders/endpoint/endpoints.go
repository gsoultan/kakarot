// Package endpoint turns OrderService methods into typed endpoints.
//
// Written by hand in milestone 1; `kakarot gen` will generate this file from
// the OrderService interface in milestone 2.
package endpoint

import (
	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/service"
	"github.com/gsoultan/kakarot/middleware"
)

// Endpoints holds one typed endpoint per service method.
type Endpoints struct {
	PlaceOrder kakarot.Endpoint[domain.PlaceOrder, domain.Order]
	GetOrder   kakarot.Endpoint[domain.GetOrder, domain.Order]
}

// Make builds the endpoints. A method value already has the Endpoint shape.
// Validated only compiles because domain.PlaceOrder has a Validate method.
func Make(svc service.OrderService) Endpoints {
	return Endpoints{
		PlaceOrder: middleware.Validated(svc.PlaceOrder),
		GetOrder:   svc.GetOrder,
	}
}
