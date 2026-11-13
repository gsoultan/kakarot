package usecase

import (
	"context"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/checkout/domain"
)

// The asynchronous usecases: each one handles an event. Messages are
// delivered at least once and in no guaranteed order, so every handler
//
//  1. checks the order's status first and does nothing on a repeat,
//  2. calls the outside system without holding a database transaction,
//  3. then locks the order, checks the status again and records the result
//     together with the next event.
//
// Returning an error asks for the message to be delivered again.

// ChargePayment handles order.placed: it takes the payment.
type ChargePayment struct {
	Catalog  Catalog
	Orders   Orders
	Events   Events
	Payments Payments
	Tx       kakarot.TxManager
}

func (u *ChargePayment) Execute(ctx context.Context, in domain.OrderPlaced) (domain.Handled, error) {
	o, err := u.Orders.Get(ctx, in.OrderID)
	if err != nil {
		return domain.Handled{}, err
	}
	if o.Status != domain.StatusPending {
		return domain.Handled{Skipped: true}, nil
	}
	// The order id is the provider's idempotency key: a redelivered message
	// gets the first charge back instead of charging twice.
	chargeID, err := u.Payments.Charge(ctx, o.ID, o.PaymentToken, o.Total)
	declined := kakarot.Is(err, domain.CodeDeclined)
	if err != nil && !declined {
		return domain.Handled{}, err // an outage: try again later
	}
	return domain.Handled{}, u.Tx.Within(ctx, func(ctx context.Context) error {
		o, err := u.Orders.Lock(ctx, in.OrderID)
		if err != nil {
			return err
		}
		switch {
		case declined && o.Status == domain.StatusPending:
			_, err = cancel(ctx, u.Catalog, u.Orders, u.Events, o, "payment declined")
			return err
		case declined:
			return nil
		case o.Status == domain.StatusPending:
			o.Status, o.ChargeID = domain.StatusPaid, chargeID
			if err := u.Orders.Save(ctx, o, "payment approved"); err != nil {
				return err
			}
			if err := u.Events.Paid(ctx, domain.OrderPaid{OrderID: o.ID}); err != nil {
				return err
			}
			return u.Events.Updated(ctx, domain.OrderUpdated{OrderID: o.ID, Customer: o.Customer, Status: o.Status, Note: "payment approved"})
		case o.Status == domain.StatusCancelled && o.ChargeID == "":
			// The order was cancelled while the provider was working. The
			// money was taken, so it has to go back.
			o.ChargeID = chargeID
			if err := u.Orders.Save(ctx, o, "payment arrived after cancellation, refund requested"); err != nil {
				return err
			}
			return u.Events.RefundDue(ctx, domain.RefundDue{OrderID: o.ID, ChargeID: chargeID, Amount: o.Total})
		}
		return nil // a repeat of a message that was already recorded
	})
}

// ShipOrder handles order.paid: it books the shipment.
type ShipOrder struct {
	Orders    Orders
	Events    Events
	Warehouse Warehouse
	Tx        kakarot.TxManager
}

func (u *ShipOrder) Execute(ctx context.Context, in domain.OrderPaid) (domain.Handled, error) {
	o, err := u.Orders.Get(ctx, in.OrderID)
	if err != nil {
		return domain.Handled{}, err
	}
	if o.Status != domain.StatusPaid {
		return domain.Handled{Skipped: true}, nil
	}
	tracking, err := u.Warehouse.Ship(ctx, o)
	if err != nil {
		return domain.Handled{}, err
	}
	return domain.Handled{}, u.Tx.Within(ctx, func(ctx context.Context) error {
		o, err := u.Orders.Lock(ctx, in.OrderID)
		if err != nil || o.Status != domain.StatusPaid {
			return err
		}
		o.Status, o.Tracking = domain.StatusShipped, tracking
		if err := u.Orders.Save(ctx, o, "shipped, tracking "+tracking); err != nil {
			return err
		}
		return u.Events.Updated(ctx, domain.OrderUpdated{OrderID: o.ID, Customer: o.Customer, Status: o.Status, Note: "shipped, tracking " + tracking})
	})
}

// RefundPayment handles order.refund_due: it returns a charge.
type RefundPayment struct {
	Orders   Orders
	Events   Events
	Payments Payments
	Tx       kakarot.TxManager
}

func (u *RefundPayment) Execute(ctx context.Context, in domain.RefundDue) (domain.Handled, error) {
	o, err := u.Orders.Get(ctx, in.OrderID)
	if err != nil {
		return domain.Handled{}, err
	}
	if o.Refunded {
		return domain.Handled{Skipped: true}, nil
	}
	if err := u.Payments.Refund(ctx, in.ChargeID); err != nil {
		return domain.Handled{}, err
	}
	return domain.Handled{}, u.Tx.Within(ctx, func(ctx context.Context) error {
		o, err := u.Orders.Lock(ctx, in.OrderID)
		if err != nil || o.Refunded {
			return err
		}
		o.Refunded = true
		if err := u.Orders.Save(ctx, o, "payment refunded"); err != nil {
			return err
		}
		return u.Events.Updated(ctx, domain.OrderUpdated{OrderID: o.ID, Customer: o.Customer, Status: o.Status, Note: "payment refunded"})
	})
}

// NotifyCustomer handles order.updated: it tells the customer.
type NotifyCustomer struct{ Notifier Notifier }

func (u *NotifyCustomer) Execute(ctx context.Context, in domain.OrderUpdated) (domain.Handled, error) {
	return domain.Handled{}, u.Notifier.Notify(ctx, in)
}

// ExpireOrders is a scheduled job, not a message handler. It cancels orders
// that are still pending after the payment window: the payment event may be
// stuck because the provider is down. If the charge goes through later,
// ChargePayment finds the order cancelled and requests a refund.
type ExpireOrders struct {
	Catalog Catalog
	Orders  Orders
	Events  Events
	Tx      kakarot.TxManager
	After   time.Duration    // the payment window
	Now     func() time.Time // defaults to time.Now
}

// Run cancels the expired orders and reports how many.
func (u *ExpireOrders) Run(ctx context.Context) (int, error) {
	now := time.Now
	if u.Now != nil {
		now = u.Now
	}
	ids, err := u.Orders.Stale(ctx, now().Add(-u.After), 100)
	if err != nil {
		return 0, err
	}
	expired := 0
	for _, id := range ids {
		err := u.Tx.Within(ctx, func(ctx context.Context) error {
			o, err := u.Orders.Lock(ctx, id)
			if err != nil || o.Status != domain.StatusPending {
				return err // paid or cancelled since the scan
			}
			_, err = cancel(ctx, u.Catalog, u.Orders, u.Events, o, "payment timed out")
			if err == nil {
				expired++
			}
			return err
		})
		if err != nil {
			return expired, err
		}
	}
	return expired, nil
}
