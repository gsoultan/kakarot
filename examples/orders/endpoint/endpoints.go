// Package endpoint turns OrderService methods into typed endpoints.
//
// Written by hand in milestone 1; `kakarot gen` will generate this file from
// the OrderService interface in milestone 2.
package endpoint

import (
	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/service"
)

// Endpoints holds one typed endpoint per service method.
type Endpoints struct {
	PlaceOrder kakarot.Endpoint[domain.PlaceOrder, domain.Order]
	GetOrder   kakarot.Endpoint[domain.GetOrder, domain.Order]
}

// Make builds the endpoints. A method value already has the Endpoint shape.
func Make(svc service.OrderService) Endpoints {
	return Endpoints{
		PlaceOrder: svc.PlaceOrder,
		GetOrder:   svc.GetOrder,
	}
}
