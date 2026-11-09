package usecase

import (
	"context"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
)

// GetOrder reads one order.
type GetOrder struct {
	Orders OrderRepository
}

var _ kakarot.Usecase[domain.GetOrder, domain.Order] = (*GetOrder)(nil)

func (u *GetOrder) Execute(ctx context.Context, in domain.GetOrder) (domain.Order, error) {
	return u.Orders.Get(ctx, in.ID)
}
