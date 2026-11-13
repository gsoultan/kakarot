package usecase

import (
	"context"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
)

// Notifier is the port for telling the customer about their order (email,
// push, ...).
type Notifier interface {
	OrderPlaced(ctx context.Context, o domain.Order) error
}

// NotifyOrderPlaced reacts to the order.placed event. Events arrive at
// least once, so the notifier must tolerate a repeat.
type NotifyOrderPlaced struct{ Notifier Notifier }

var _ kakarot.Usecase[domain.Order, domain.Notified] = (*NotifyOrderPlaced)(nil)

func (u *NotifyOrderPlaced) Execute(ctx context.Context, o domain.Order) (domain.Notified, error) {
	if err := u.Notifier.OrderPlaced(ctx, o); err != nil {
		return domain.Notified{}, err
	}
	return domain.Notified{OrderID: o.ID}, nil
}
