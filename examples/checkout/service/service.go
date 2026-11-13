// Package service is the checkout's API. kakarot gen reads this interface and
// writes the endpoint layer and the HTTP and queue bindings.
package service

//go:generate go tool kakarot gen

import (
	"context"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/checkout/domain"
)

// CheckoutService is the contract every transport is generated from. The
// first five methods are synchronous requests; the last four are message
// handlers.
//
//kakarot:service checkout
type CheckoutService interface {
	//kakarot:http POST /products
	AddProduct(ctx context.Context, in domain.AddProduct) (domain.Product, error)
	//kakarot:http GET /products/{sku}
	GetProduct(ctx context.Context, in domain.GetProduct) (domain.Product, error)
	//kakarot:http POST /orders
	PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error)
	//kakarot:http GET /orders/{id}
	GetOrder(ctx context.Context, in domain.GetOrder) (domain.Order, error)
	//kakarot:http POST /orders/{id}/cancel
	CancelOrder(ctx context.Context, in domain.CancelOrder) (domain.Order, error)

	//kakarot:queue order.placed
	ChargePayment(ctx context.Context, in domain.OrderPlaced) (domain.Handled, error)
	//kakarot:queue order.paid
	ShipOrder(ctx context.Context, in domain.OrderPaid) (domain.Handled, error)
	//kakarot:queue order.refund_due
	RefundPayment(ctx context.Context, in domain.RefundDue) (domain.Handled, error)
	//kakarot:queue order.updated
	NotifyCustomer(ctx context.Context, in domain.OrderUpdated) (domain.Handled, error)
}

// Usecases groups what the service composes.
type Usecases struct {
	AddProduct  kakarot.Usecase[domain.AddProduct, domain.Product]
	GetProduct  kakarot.Usecase[domain.GetProduct, domain.Product]
	PlaceOrder  kakarot.Usecase[domain.PlaceOrder, domain.Order]
	GetOrder    kakarot.Usecase[domain.GetOrder, domain.Order]
	CancelOrder kakarot.Usecase[domain.CancelOrder, domain.Order]
	Charge      kakarot.Usecase[domain.OrderPlaced, domain.Handled]
	Ship        kakarot.Usecase[domain.OrderPaid, domain.Handled]
	Refund      kakarot.Usecase[domain.RefundDue, domain.Handled]
	Notify      kakarot.Usecase[domain.OrderUpdated, domain.Handled]
}

type checkout struct{ uc Usecases }

// New returns the service.
func New(uc Usecases) CheckoutService { return &checkout{uc: uc} }

func (s *checkout) AddProduct(ctx context.Context, in domain.AddProduct) (domain.Product, error) {
	return s.uc.AddProduct.Execute(ctx, in)
}

func (s *checkout) GetProduct(ctx context.Context, in domain.GetProduct) (domain.Product, error) {
	return s.uc.GetProduct.Execute(ctx, in)
}

func (s *checkout) PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error) {
	return s.uc.PlaceOrder.Execute(ctx, in)
}

func (s *checkout) GetOrder(ctx context.Context, in domain.GetOrder) (domain.Order, error) {
	return s.uc.GetOrder.Execute(ctx, in)
}

func (s *checkout) CancelOrder(ctx context.Context, in domain.CancelOrder) (domain.Order, error) {
	return s.uc.CancelOrder.Execute(ctx, in)
}

func (s *checkout) ChargePayment(ctx context.Context, in domain.OrderPlaced) (domain.Handled, error) {
	return s.uc.Charge.Execute(ctx, in)
}

func (s *checkout) ShipOrder(ctx context.Context, in domain.OrderPaid) (domain.Handled, error) {
	return s.uc.Ship.Execute(ctx, in)
}

func (s *checkout) RefundPayment(ctx context.Context, in domain.RefundDue) (domain.Handled, error) {
	return s.uc.Refund.Execute(ctx, in)
}

func (s *checkout) NotifyCustomer(ctx context.Context, in domain.OrderUpdated) (domain.Handled, error) {
	return s.uc.Notify.Execute(ctx, in)
}
