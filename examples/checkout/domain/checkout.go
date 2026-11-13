// Package domain holds the checkout's request, response and event types.
// They are the DTOs every layer shares; nothing here knows about HTTP,
// queues or SQL.
package domain

import (
	"strconv"
	"time"

	"github.com/gsoultan/kakarot"
)

// The checkout's own error codes.
var (
	CodeOutOfStock = kakarot.NewCode("out_of_stock")
	CodeDeclined   = kakarot.NewCode("payment_declined")
)

// Status is where an order is in its life.
//
//	pending -> paid -> shipped
//	pending -> cancelled            (declined, timed out, or cancelled by the customer)
type Status string

const (
	StatusPending   Status = "pending"
	StatusPaid      Status = "paid"
	StatusShipped   Status = "shipped"
	StatusCancelled Status = "cancelled"
)

// Product is something the shop sells. Price is in minor units (cents).
type Product struct {
	SKU   string `json:"sku"`
	Name  string `json:"name"`
	Price int64  `json:"price"`
	Stock int64  `json:"stock"`
}

// AddProduct puts a product in the catalog.
type AddProduct struct {
	SKU   string `json:"sku"`
	Name  string `json:"name"`
	Price int64  `json:"price"`
	Stock int64  `json:"stock"`
}

func (p AddProduct) Validate() error {
	fields := map[string]string{}
	if p.SKU == "" {
		fields["sku"] = "required"
	}
	if p.Name == "" {
		fields["name"] = "required"
	}
	if p.Price <= 0 {
		fields["price"] = "must be positive"
	}
	if p.Stock < 0 {
		fields["stock"] = "must not be negative"
	}
	if len(fields) > 0 {
		return kakarot.Invalid("invalid product", fields)
	}
	return nil
}

// GetProduct reads one product.
type GetProduct struct {
	SKU string `path:"sku" json:"sku"`
}

// Item is one requested product and quantity.
type Item struct {
	SKU string `json:"sku"`
	Qty int64  `json:"qty"`
}

// Line is an item on an order, with the price it was sold at.
type Line struct {
	SKU   string `json:"sku"`
	Qty   int64  `json:"qty"`
	Price int64  `json:"price"`
}

// Step is one entry in an order's timeline.
type Step struct {
	Status Status    `json:"status"`
	Note   string    `json:"note"`
	At     time.Time `json:"at"`
}

// Order is a customer's order and its progress.
type Order struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	Status    Status    `json:"status"`
	Total     int64     `json:"total"`
	Lines     []Line    `json:"lines"`
	Tracking  string    `json:"tracking,omitempty"`
	Reason    string    `json:"reason,omitempty"` // why it was cancelled
	Refunded  bool      `json:"refunded,omitempty"`
	Steps     []Step    `json:"steps,omitempty"`
	CreatedAt time.Time `json:"created_at"`

	// Kept for the payment steps, never sent to callers.
	PaymentToken string `json:"-"`
	ChargeID     string `json:"-"`
}

// PlaceOrder is the synchronous part of a checkout.
type PlaceOrder struct {
	Customer string `json:"customer"`
	Items    []Item `json:"items"`
	// PaymentToken is the payment provider's token for the card. Card
	// numbers never reach this service.
	PaymentToken string `json:"payment_token"`
	// Key makes a retried request safe; HTTP callers send it as the
	// Idempotency-Key header.
	Key string `header:"Idempotency-Key" json:"idempotency_key,omitempty"`
}

func (p PlaceOrder) Validate() error {
	fields := map[string]string{}
	if p.Customer == "" {
		fields["customer"] = "required"
	}
	if p.PaymentToken == "" {
		fields["payment_token"] = "required"
	}
	if len(p.Items) == 0 {
		fields["items"] = "at least one item"
	}
	if len(p.Items) > 50 {
		fields["items"] = "at most 50 items"
	}
	seen := map[string]bool{}
	for i, it := range p.Items {
		at := "items[" + strconv.Itoa(i) + "]"
		switch {
		case it.SKU == "":
			fields[at+".sku"] = "required"
		case seen[it.SKU]:
			fields[at+".sku"] = "listed twice"
		}
		seen[it.SKU] = true
		if it.Qty <= 0 || it.Qty > 1000 {
			fields[at+".qty"] = "must be between 1 and 1000"
		}
	}
	if len(fields) > 0 {
		return kakarot.Invalid("invalid order", fields)
	}
	return nil
}

// IdempotencyKey implements middleware.Keyed. The key is scoped to the
// customer, so one customer cannot replay another's order.
func (p PlaceOrder) IdempotencyKey() string {
	if p.Key == "" {
		return ""
	}
	return p.Customer + "/" + p.Key
}

// GetOrder reads one order with its timeline.
type GetOrder struct {
	ID string `path:"id" json:"id"`
}

// CancelOrder cancels an order that has not been paid yet.
type CancelOrder struct {
	ID string `path:"id" json:"id"`
}

// Events. Each one is written to the outbox in the same transaction as the
// change it reports, and delivered at least once.

// OrderPlaced asks for the order's payment to be taken.
type OrderPlaced struct {
	OrderID string `json:"order_id"`
}

// OrderPaid asks for the order to be shipped.
type OrderPaid struct {
	OrderID string `json:"order_id"`
}

// RefundDue asks for a charge to be returned: the payment went through
// after the order had already been cancelled.
type RefundDue struct {
	OrderID  string `json:"order_id"`
	ChargeID string `json:"charge_id"`
	Amount   int64  `json:"amount"`
}

// OrderUpdated tells the customer that the order changed.
type OrderUpdated struct {
	OrderID  string `json:"order_id"`
	Customer string `json:"customer"`
	Status   Status `json:"status"`
	Note     string `json:"note"`
}

// Handled is what a message handler answers. Skipped is true when the
// message was a repeat and there was nothing left to do.
type Handled struct {
	Skipped bool `json:"skipped,omitempty"`
}
