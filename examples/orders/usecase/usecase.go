// Package usecase holds one type per business operation, plus the repository
// ports those operations need. Ports live here, next to their consumer, so
// the dependency arrow points inward: data implements usecase, never the
// reverse.
package usecase

import (
	"context"

	"github.com/gsoultan/kakarot"

	"github.com/gsoultan/kakarot/examples/orders/domain"
)

// OrderRepository is the port the order usecases persist through.
type OrderRepository interface {
	Create(ctx context.Context, o *domain.Order) error
	Get(ctx context.Context, id string) (domain.Order, error)
	ListByCustomer(ctx context.Context, customerID, cursor string, limit int) (kakarot.Page[domain.Order], error)
}

// Events is the port for domain events. The data layer writes them to the
// transactional outbox, so an event exists only if the transaction commits.
type Events interface {
	OrderPlaced(ctx context.Context, o domain.Order) error
}
