package usecase

import (
	"context"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
)

// PlaceOrder creates an order and its items in one transaction.
type PlaceOrder struct {
	Orders OrderRepository
	Tx     kakarot.TxManager
}

var _ kakarot.Usecase[domain.PlaceOrder, domain.Order] = (*PlaceOrder)(nil)

func (u *PlaceOrder) Execute(ctx context.Context, in domain.PlaceOrder) (domain.Order, error) {
	o := domain.Order{
		CustomerID: in.CustomerID,
		Status:     domain.StatusPlaced,
		Items:      in.Items,
	}
	err := u.Tx.Within(ctx, func(ctx context.Context) error {
		return u.Orders.Create(ctx, &o)
	})
	return o, err
}
