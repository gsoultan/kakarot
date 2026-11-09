package data

import (
	"context"

	"github.com/gsoultan/kakarot/data/outbox"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/usecase"
	"github.com/gsoultan/storm"
)

// OrderPlaced is the typed topic: producers and consumers share the payload type.
var OrderPlaced = outbox.Topic[domain.Order]("order.placed")

// Events writes domain events to the transactional outbox.
type Events struct {
	DB storm.DB
}

var _ usecase.Events = (*Events)(nil)

func (e *Events) OrderPlaced(ctx context.Context, o domain.Order) error {
	return OrderPlaced.Add(ctx, e.DB, o.ID, o)
}
