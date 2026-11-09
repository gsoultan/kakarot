// Package service is the API of the order bounded context. It composes
// usecases and is where authorization belongs; it knows nothing of HTTP or SQL.
package service

import (
	"context"
	"iter"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
)

//go:generate go tool kakarot gen

// OrderService is the contract endpoints and routes are generated from.
//
//kakarot:service order
type OrderService interface {
	//kakarot:http POST /orders
	PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error)
	//kakarot:http GET /orders/{id}
	GetOrder(ctx context.Context, in domain.GetOrder) (domain.Order, error)
	//kakarot:http GET /orders
	ListOrders(ctx context.Context, in domain.ListOrders) (kakarot.Page[domain.Order], error)
	//kakarot:http GET /orders/export
	ExportOrders(ctx context.Context, in domain.ExportOrders) iter.Seq2[domain.Order, error]
}

// Usecases groups what the service composes.
type Usecases struct {
	Place  kakarot.Usecase[domain.PlaceOrder, domain.Order]
	Get    kakarot.Usecase[domain.GetOrder, domain.Order]
	List   kakarot.Usecase[domain.ListOrders, kakarot.Page[domain.Order]]
	Export kakarot.ServerStream[domain.ExportOrders, domain.Order]
}

type orderService struct{ uc Usecases }

// New returns the order service.
func New(uc Usecases) OrderService { return &orderService{uc: uc} }

func (s *orderService) PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error) {
	return s.uc.Place.Execute(ctx, in)
}

func (s *orderService) GetOrder(ctx context.Context, in domain.GetOrder) (domain.Order, error) {
	return s.uc.Get.Execute(ctx, in)
}

func (s *orderService) ListOrders(ctx context.Context, in domain.ListOrders) (kakarot.Page[domain.Order], error) {
	return s.uc.List.Execute(ctx, in)
}

func (s *orderService) ExportOrders(ctx context.Context, in domain.ExportOrders) iter.Seq2[domain.Order, error] {
	return s.uc.Export(ctx, in)
}
