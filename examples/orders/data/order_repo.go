// Package data implements the order repository with storm. The storm models
// in data/model are the database types; data/store is generated from them by
// `storm generate examples/orders/data/store`. Only this package sees either.
package data

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/gsoultan/kakarot"
	kdata "github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/kakarot/examples/orders/data/store"
	"github.com/gsoultan/kakarot/examples/orders/data/store/order"
	"github.com/gsoultan/kakarot/examples/orders/data/store/orderitem"
	"github.com/gsoultan/kakarot/examples/orders/domain"
	"github.com/gsoultan/kakarot/examples/orders/usecase"
	"github.com/gsoultan/storm"
)

// OrderRepo stores orders through storm.
type OrderRepo struct {
	DB storm.DB
}

var _ usecase.OrderRepository = (*OrderRepo)(nil)

// Create inserts the order and its items. Run it inside TxManager.Within so
// both inserts share one transaction.
func (r *OrderRepo) Create(ctx context.Context, o *domain.Order) error {
	ex := kdata.Executor(ctx, r.DB)

	c := order.Create()
	c.SetCustomerID(o.CustomerID)
	c.SetStatus(string(o.Status))
	row, err := c.Insert(ctx, ex)
	if err != nil {
		return kdata.MapErr(err)
	}
	for _, it := range o.Items {
		ic := orderitem.Create()
		ic.SetOrderID(row.ID)
		ic.SetSku(it.SKU)
		ic.SetQuantity(it.Quantity)
		if _, err := ic.Insert(ctx, ex); err != nil {
			return kdata.MapErr(err)
		}
	}
	o.ID = uuid.UUID(row.ID).String()
	o.CreatedAt = row.CreatedAt
	return nil
}

// Get loads an order with its items in two queries.
func (r *OrderRepo) Get(ctx context.Context, id string) (domain.Order, error) {
	key, err := uuid.Parse(id)
	if err != nil {
		return domain.Order{}, kakarot.NotFound("order", id)
	}
	rows, err := store.OrderWithItems().Where(order.ID.Eq(key)).Limit(1).All(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return domain.Order{}, kdata.MapErr(err)
	}
	if len(rows) == 0 {
		return domain.Order{}, kakarot.NotFound("order", id)
	}
	return toDomain(rows[0]), nil
}

type orderCursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"id"`
}

// ListByCustomer pages through a customer's orders with a keyset cursor on
// (created_at, id), using storm's After.
func (r *OrderRepo) ListByCustomer(ctx context.Context, customerID, cursor string, limit int) (kakarot.Page[domain.Order], error) {
	q := order.New().
		Where(order.CustomerID.Eq(customerID)).
		Order(order.CreatedAt.Asc(), order.ID.Asc()).
		Limit(int64(limit + 1))
	c, ok, err := kakarot.DecodeCursor[orderCursor](cursor)
	if err != nil {
		return kakarot.Page[domain.Order]{}, err
	}
	if ok {
		q = q.After(order.Row{CreatedAt: c.CreatedAt, ID: c.ID})
	}
	rows, err := q.List(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return kakarot.Page[domain.Order]{}, kdata.MapErr(err)
	}
	// Items of a list carry no line items; GetOrder loads them.
	return kakarot.NextPage(rows, limit,
		func(row order.Row) domain.Order { return toDomain(store.OrderWithItemsRow{Row: row}) },
		func(row order.Row) orderCursor { return orderCursor{CreatedAt: row.CreatedAt, ID: row.ID} })
}

// toDomain maps storm rows (database types) to the domain type.
func toDomain(r store.OrderWithItemsRow) domain.Order {
	o := domain.Order{
		ID:         uuid.UUID(r.ID).String(),
		CustomerID: r.CustomerID,
		Status:     domain.OrderStatus(r.Status),
		CreatedAt:  r.CreatedAt,
		Items:      make([]domain.LineItem, 0, len(r.Items)),
	}
	for _, it := range r.Items {
		o.Items = append(o.Items, domain.LineItem{SKU: it.Sku, Quantity: it.Quantity})
	}
	return o
}
