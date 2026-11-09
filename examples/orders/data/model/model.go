// Package model holds the storm models: one plain struct per table.
// These are the repository's database types; no layer above data imports them.
package model

import "github.com/gsoultan/storm"

// Order is the orders table.
type Order struct {
	storm.Model // id uuid, created_at, updated_at

	CustomerID string
	Status     string

	Items []OrderItem // has-many
}

func (o *Order) Schema(t *storm.Table) {
	t.Col(&o.CustomerID).Size(64)
	t.Col(&o.Status).Size(32)
	t.Index(&o.CustomerID)
}

// OrderItem is the order_items table.
type OrderItem struct {
	storm.Model

	SKU      string
	Quantity int32

	Order Order // foreign key order_id -> orders.id
}

func (i *OrderItem) Schema(t *storm.Table) {
	t.Col(&i.SKU).Size(64)
	t.Col(&i.Order).OnDelete(storm.Cascade)
}
