// Package usecase holds the checkout's business rules and the ports they
// need. Nothing here imports a database, an HTTP or a queue library.
package usecase

import (
	"context"
	"time"

	"github.com/gsoultan/kakarot/examples/checkout/domain"
)

// Catalog is the product and stock port.
type Catalog interface {
	Add(ctx context.Context, p domain.Product) (domain.Product, error)
	Get(ctx context.Context, sku string) (domain.Product, error)
	// Reserve takes the items out of stock and returns them priced. It
	// locks the products in SKU order, so two orders for the same products
	// wait for each other instead of deadlocking. It needs a transaction.
	Reserve(ctx context.Context, items []domain.Item) ([]domain.Line, error)
	// Release puts reserved lines back in stock.
	Release(ctx context.Context, lines []domain.Line) error
}

// Orders is the order port.
type Orders interface {
	Create(ctx context.Context, o domain.Order, note string) (domain.Order, error)
	// Get reads an order with its lines and timeline.
	Get(ctx context.Context, id string) (domain.Order, error)
	// Lock reads an order and holds its row until the transaction in ctx
	// ends. Every status change goes through Lock, so a synchronous request
	// and an asynchronous handler can never both change the same order.
	Lock(ctx context.Context, id string) (domain.Order, error)
	// Save writes the order's status fields and adds note to its timeline.
	Save(ctx context.Context, o domain.Order, note string) error
	// Stale lists pending orders created before the given time.
	Stale(ctx context.Context, before time.Time, limit int) ([]string, error)
}

// Events is the port for domain events (the transactional outbox).
type Events interface {
	Placed(ctx context.Context, e domain.OrderPlaced) error
	Paid(ctx context.Context, e domain.OrderPaid) error
	RefundDue(ctx context.Context, e domain.RefundDue) error
	Updated(ctx context.Context, e domain.OrderUpdated) error
}

// Payments is the payment provider. Both calls must be idempotent on the
// provider's side: Charge by key, Refund by charge id. A declined card
// answers domain.CodeDeclined; an outage answers kakarot.CodeUnavailable.
type Payments interface {
	Charge(ctx context.Context, key, token string, amount int64) (chargeID string, err error)
	Refund(ctx context.Context, chargeID string) error
}

// Warehouse books shipments. Ship must be idempotent per order.
type Warehouse interface {
	Ship(ctx context.Context, o domain.Order) (tracking string, err error)
}

// Notifier tells the customer about an order.
type Notifier interface {
	Notify(ctx context.Context, e domain.OrderUpdated) error
}
