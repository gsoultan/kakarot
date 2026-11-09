package usecase

import (
	"context"
	"iter"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/orders/domain"
)

// ListOrders returns one page of a customer's orders.
type ListOrders struct {
	Orders OrderRepository
}

var _ kakarot.Usecase[domain.ListOrders, kakarot.Page[domain.Order]] = (*ListOrders)(nil)

func (u *ListOrders) Execute(ctx context.Context, in domain.ListOrders) (kakarot.Page[domain.Order], error) {
	return u.Orders.ListByCustomer(ctx, in.CustomerID, in.Cursor, in.Size(20, 100))
}

// ExportOrders streams all of a customer's orders, page by page, so memory
// stays flat however many orders there are.
type ExportOrders struct {
	Orders OrderRepository
}

func (u *ExportOrders) Stream(ctx context.Context, in domain.ExportOrders) iter.Seq2[domain.Order, error] {
	return func(yield func(domain.Order, error) bool) {
		cursor := ""
		for {
			page, err := u.Orders.ListByCustomer(ctx, in.CustomerID, cursor, 100)
			if err != nil {
				yield(domain.Order{}, err)
				return
			}
			for _, o := range page.Items {
				if !yield(o, nil) {
					return
				}
			}
			if page.Next == "" {
				return
			}
			cursor = page.Next
		}
	}
}
