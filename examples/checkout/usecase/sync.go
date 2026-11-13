package usecase

import (
	"context"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/checkout/domain"
)

// The synchronous usecases: the caller waits for the answer.

// AddProduct puts a product in the catalog.
type AddProduct struct{ Catalog Catalog }

func (u *AddProduct) Execute(ctx context.Context, in domain.AddProduct) (domain.Product, error) {
	return u.Catalog.Add(ctx, domain.Product{SKU: in.SKU, Name: in.Name, Price: in.Price, Stock: in.Stock})
}

// GetProduct reads a product and its current stock.
type GetProduct struct{ Catalog Catalog }

func (u *GetProduct) Execute(ctx context.Context, in domain.GetProduct) (domain.Product, error) {
	return u.Catalog.Get(ctx, in.SKU)
}

// PlaceOrder is the part of a checkout the customer waits for. In one
// transaction it reserves the stock, stores the order as pending and records
// the order.placed event. It does not call the payment provider: that is
// slow and can fail, so it happens afterwards, driven by the event.
type PlaceOrder struct {
	Catalog Catalog
	Orders  Orders
	Events  Events
	Tx      kakarot.TxManager
}

func (u *PlaceOrder) Execute(ctx context.Context, in domain.PlaceOrder) (domain.Order, error) {
	var out domain.Order
	err := u.Tx.Within(ctx, func(ctx context.Context) error {
		lines, err := u.Catalog.Reserve(ctx, in.Items)
		if err != nil {
			return err
		}
		o := domain.Order{Customer: in.Customer, Status: domain.StatusPending, Lines: lines, PaymentToken: in.PaymentToken}
		for _, l := range lines {
			o.Total += l.Price * l.Qty
		}
		if out, err = u.Orders.Create(ctx, o, "order received, stock reserved"); err != nil {
			return err
		}
		return u.Events.Placed(ctx, domain.OrderPlaced{OrderID: out.ID})
	})
	return out, err
}

// GetOrder reads an order with its timeline. Callers poll it to follow the
// asynchronous steps.
type GetOrder struct{ Orders Orders }

func (u *GetOrder) Execute(ctx context.Context, in domain.GetOrder) (domain.Order, error) {
	return u.Orders.Get(ctx, in.ID)
}

// CancelOrder cancels an order that is still waiting for payment. It may run
// at the same moment as the payment handler; the row lock decides who goes
// first, and the handler copes with either outcome.
type CancelOrder struct {
	Catalog Catalog
	Orders  Orders
	Events  Events
	Tx      kakarot.TxManager
}

func (u *CancelOrder) Execute(ctx context.Context, in domain.CancelOrder) (domain.Order, error) {
	var out domain.Order
	err := u.Tx.Within(ctx, func(ctx context.Context) error {
		o, err := u.Orders.Lock(ctx, in.ID)
		if err != nil {
			return err
		}
		switch o.Status {
		case domain.StatusCancelled: // a repeated request: nothing to do
		case domain.StatusPending:
			if o, err = cancel(ctx, u.Catalog, u.Orders, u.Events, o, "cancelled by customer"); err != nil {
				return err
			}
		default:
			return kakarot.Conflict("order is already "+string(o.Status)+" and cannot be cancelled", nil)
		}
		out = o
		return nil
	})
	return out, err
}

// cancel is the compensation for PlaceOrder: it returns the stock and marks
// the order cancelled. Call it on an order returned by Lock.
func cancel(ctx context.Context, c Catalog, os Orders, ev Events, o domain.Order, reason string) (domain.Order, error) {
	if err := c.Release(ctx, o.Lines); err != nil {
		return o, err
	}
	o.Status, o.Reason = domain.StatusCancelled, reason
	if err := os.Save(ctx, o, reason+", stock released"); err != nil {
		return o, err
	}
	return o, ev.Updated(ctx, domain.OrderUpdated{OrderID: o.ID, Customer: o.Customer, Status: o.Status, Note: reason})
}
