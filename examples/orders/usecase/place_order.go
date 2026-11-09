package usecase

import (
	"context"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
)

// PlaceOrder creates an order and its items in one transaction.
type PlaceOrder struct {
	Orders OrderRepository
	Events Events
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
		if err := u.Orders.Create(ctx, &o); err != nil {
			return err
		}
		return u.Events.OrderPlaced(ctx, o) // same transaction

	})
	return o, err
}
