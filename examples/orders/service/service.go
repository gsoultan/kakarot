// Package service is the API of the order bounded context. It composes
// usecases and is where authorization belongs; it knows nothing of HTTP or SQL.
package service

import (
	"context"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
)

// OrderService is the contract endpoints are generated from.
type OrderService interface {
	PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error)
	GetOrder(ctx context.Context, in domain.GetOrder) (domain.Order, error)
}

type orderService struct {
	place kakarot.Usecase[domain.PlaceOrder, domain.Order]
	get   kakarot.Usecase[domain.GetOrder, domain.Order]
}

// New returns the order service.
func New(place kakarot.Usecase[domain.PlaceOrder, domain.Order], get kakarot.Usecase[domain.GetOrder, domain.Order]) OrderService {
	return &orderService{place: place, get: get}
}

func (s *orderService) PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error) {
	return s.place.Execute(ctx, in)
}

func (s *orderService) GetOrder(ctx context.Context, in domain.GetOrder) (domain.Order, error) {
	return s.get.Execute(ctx, in)
}
