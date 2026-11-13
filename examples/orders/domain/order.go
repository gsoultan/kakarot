// Package domain holds the domain types. They are also the DTOs: transport,
// endpoint, service, usecase and the repository interface all pass them.
package domain

import (
	"strconv"
	"time"

	"github.com/gsoultan/kakarot"
)

// OrderStatus is the lifecycle state of an order.
type OrderStatus string

const (
	StatusPlaced    OrderStatus = "placed"
	StatusCancelled OrderStatus = "cancelled"
)

// LineItem is one product on an order.
type LineItem struct {
	SKU      string `json:"sku"`
	Quantity int32  `json:"quantity"`
}

// Order is an order as the business sees it.
type Order struct {
	ID         string      `json:"id"`
	CustomerID string      `json:"customer_id"`
	Status     OrderStatus `json:"status"`
	Items      []LineItem  `json:"items"`
	CreatedAt  time.Time   `json:"created_at"`
}

// PlaceOrder is the input to place an order.
type PlaceOrder struct {
	CustomerID string     `json:"customer_id"`
	Items      []LineItem `json:"items"`
	// Key makes a retried request safe: the same key replays the first
	// response. HTTP callers send it as the Idempotency-Key header.
	Key string `header:"Idempotency-Key" json:"idempotency_key,omitempty"`
}

// IdempotencyKey implements middleware.Keyed. The key is scoped to the
// customer, so one customer cannot replay another's order.
func (p PlaceOrder) IdempotencyKey() string {
	if p.Key == "" {
		return ""
	}
	return p.CustomerID + "/" + p.Key
}

// Validate implements middleware.Validatable.
func (p PlaceOrder) Validate() error {
	fields := map[string]string{}
	if p.CustomerID == "" {
		fields["customer_id"] = "required"
	}
	if len(p.Items) == 0 {
		fields["items"] = "at least one item"
	}
	for i, it := range p.Items {
		if it.SKU == "" {
			fields["items["+strconv.Itoa(i)+"].sku"] = "required"
		}
		if it.Quantity <= 0 {
			fields["items["+strconv.Itoa(i)+"].quantity"] = "must be positive"
		}
	}
	if len(fields) > 0 {
		return kakarot.Invalid("invalid order", fields)
	}
	return nil
}

// ListOrders pages through one customer's orders, oldest first.
type ListOrders struct {
	CustomerID string `query:"customer_id"`
	kakarot.PageRequest
}

// Validate implements middleware.Validatable.
func (l ListOrders) Validate() error {
	if l.CustomerID == "" {
		return kakarot.Invalid("customer_id is required", map[string]string{"customer_id": "required"})
	}
	return nil
}

// ExportOrders streams every order of a customer.
type ExportOrders struct {
	CustomerID string `query:"customer_id"`
}

// GetOrder is the input to read one order.
type GetOrder struct {
	ID string `path:"id"`
}

// Notified is the result of handling an order.placed event.
type Notified struct {
	OrderID string `json:"order_id"`
}
