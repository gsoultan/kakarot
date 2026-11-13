// Package model holds the storm models: the checkout's database types.
package model

import "github.com/gsoultan/storm"

// Product is the products table.
type Product struct {
	storm.Model // id uuid, created_at, updated_at

	SKU   string
	Name  string
	Price int64
	Stock int64
}

func (p *Product) Schema(t *storm.Table) {
	t.Col(&p.SKU).Size(64)
	t.Col(&p.Name).Size(200)
	t.Col(&p.Stock).Default("0")
	t.Unique(&p.SKU)
	t.Check(storm.RawSQL(`stock >= 0`)) // the database refuses to oversell too
}

// Order is the orders table.
type Order struct {
	storm.Model

	Customer     string
	Status       string
	Total        int64
	PaymentToken string
	ChargeID     string
	Tracking     string
	Reason       string
	Refunded     bool
}

func (o *Order) Schema(t *storm.Table) {
	t.Col(&o.Customer).Size(64)
	t.Col(&o.Status).Size(16)
	t.Col(&o.PaymentToken).Size(128)
	t.Col(&o.ChargeID).Size(64).Default("''")
	t.Col(&o.Tracking).Size(64).Default("''")
	t.Col(&o.Reason).Size(200).Default("''")
	t.Col(&o.Refunded).Default("false")
	t.Index(&o.Status, &o.CreatedAt) // the expiry job scans pending orders by age
}

// OrderLine is the order_lines table.
type OrderLine struct {
	storm.Model

	SKU   string
	Qty   int64
	Price int64

	Order Order // foreign key order_id -> orders.id
}

func (l *OrderLine) Schema(t *storm.Table) {
	t.Col(&l.SKU).Size(64)
	t.Col(&l.Order).OnDelete(storm.Cascade)
}

// OrderStep is the order_steps table: the order's timeline.
type OrderStep struct {
	storm.Model

	Status string
	Note   string

	Order Order
}

func (s *OrderStep) Schema(t *storm.Table) {
	t.Col(&s.Status).Size(16)
	t.Col(&s.Note).Size(200)
	t.Col(&s.Order).OnDelete(storm.Cascade)
}
