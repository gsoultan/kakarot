// Package data implements the checkout's ports with storm. data/model holds
// the database types and data/store the code storm generates from them;
// nothing above this package sees either.
package data

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/gsoultan/kakarot"
	kdata "github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/kakarot/data/outbox"
	"github.com/gsoultan/kakarot/examples/checkout/data/store/order"
	"github.com/gsoultan/kakarot/examples/checkout/data/store/orderline"
	"github.com/gsoultan/kakarot/examples/checkout/data/store/orderstep"
	"github.com/gsoultan/kakarot/examples/checkout/data/store/product"
	"github.com/gsoultan/kakarot/examples/checkout/domain"
	"github.com/gsoultan/kakarot/examples/checkout/usecase"
	"github.com/gsoultan/storm"
)

// CatalogRepo stores products and their stock.
type CatalogRepo struct{ DB storm.DB }

var _ usecase.Catalog = (*CatalogRepo)(nil)

func (r *CatalogRepo) Add(ctx context.Context, p domain.Product) (domain.Product, error) {
	c := product.Create()
	c.SetSku(p.SKU)
	c.SetName(p.Name)
	c.SetPrice(p.Price)
	c.SetStock(p.Stock)
	row, err := c.Insert(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return domain.Product{}, kdata.MapErr(err) // a duplicate SKU is a conflict
	}
	return toProduct(row), nil
}

func (r *CatalogRepo) Get(ctx context.Context, sku string) (domain.Product, error) {
	row, found, err := product.New().Where(product.Sku.Eq(sku)).One(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return domain.Product{}, kdata.MapErr(err)
	}
	if !found {
		return domain.Product{}, kakarot.NotFound("product", sku)
	}
	return toProduct(row), nil
}

// lock selects the products FOR UPDATE, ordered by SKU.
func (r *CatalogRepo) lock(ctx context.Context, skus []string) (map[string]product.Row, error) {
	if !kdata.InTx(ctx) {
		return nil, kakarot.E(kakarot.CodeInternal, "stock changes need a transaction", nil)
	}
	rows, err := product.New().Where(product.Sku.In(skus...)).Order(product.Sku.Asc()).ForUpdate().
		List(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return nil, kdata.MapErr(err)
	}
	out := make(map[string]product.Row, len(rows))
	for _, row := range rows {
		out[row.Sku] = row
	}
	return out, nil
}

func (r *CatalogRepo) Reserve(ctx context.Context, items []domain.Item) ([]domain.Line, error) {
	skus := make([]string, len(items))
	for i, it := range items {
		skus[i] = it.SKU
	}
	rows, err := r.lock(ctx, skus)
	if err != nil {
		return nil, err
	}
	// Check everything before changing anything, and report every problem.
	problems := map[string]string{}
	for _, it := range items {
		row, ok := rows[it.SKU]
		switch {
		case !ok:
			return nil, kakarot.NotFound("product", it.SKU)
		case row.Stock < it.Qty:
			problems[it.SKU] = "only " + strconv.FormatInt(row.Stock, 10) + " left"
		}
	}
	if len(problems) > 0 {
		e := kakarot.E(domain.CodeOutOfStock, "not enough stock", nil)
		e.Fields = problems
		return nil, e
	}
	ex := kdata.Executor(ctx, r.DB)
	lines := make([]domain.Line, 0, len(items))
	for _, it := range items {
		row := rows[it.SKU]
		m := product.MutateKey(row.ID)
		m.AddStock(-it.Qty) // stock = stock - qty, in SQL
		if err := m.Update(ctx, ex); err != nil {
			return nil, kdata.MapErr(err)
		}
		lines = append(lines, domain.Line{SKU: it.SKU, Qty: it.Qty, Price: row.Price})
	}
	return lines, nil
}

func (r *CatalogRepo) Release(ctx context.Context, lines []domain.Line) error {
	skus := make([]string, len(lines))
	for i, l := range lines {
		skus[i] = l.SKU
	}
	rows, err := r.lock(ctx, skus)
	if err != nil {
		return err
	}
	ex := kdata.Executor(ctx, r.DB)
	for _, l := range lines {
		row, ok := rows[l.SKU]
		if !ok {
			continue // the product was removed; nothing to restock
		}
		m := product.MutateKey(row.ID)
		m.AddStock(l.Qty)
		if err := m.Update(ctx, ex); err != nil {
			return kdata.MapErr(err)
		}
	}
	return nil
}

// OrderRepo stores orders, their lines and their timeline.
type OrderRepo struct{ DB storm.DB }

var _ usecase.Orders = (*OrderRepo)(nil)

func (r *OrderRepo) Create(ctx context.Context, o domain.Order, note string) (domain.Order, error) {
	ex := kdata.Executor(ctx, r.DB)
	c := order.Create()
	c.SetCustomer(o.Customer)
	c.SetStatus(string(o.Status))
	c.SetTotal(o.Total)
	c.SetPaymentToken(o.PaymentToken)
	row, err := c.Insert(ctx, ex)
	if err != nil {
		return domain.Order{}, kdata.MapErr(err)
	}
	for _, l := range o.Lines {
		lc := orderline.Create()
		lc.SetOrderID(row.ID)
		lc.SetSku(l.SKU)
		lc.SetQty(l.Qty)
		lc.SetPrice(l.Price)
		if _, err := lc.Insert(ctx, ex); err != nil {
			return domain.Order{}, kdata.MapErr(err)
		}
	}
	if err := r.step(ctx, row.ID, o.Status, note); err != nil {
		return domain.Order{}, err
	}
	out := toOrder(row)
	out.Lines = o.Lines
	return out, nil
}

func (r *OrderRepo) step(ctx context.Context, orderID [16]byte, status domain.Status, note string) error {
	s := orderstep.Create()
	s.SetOrderID(orderID)
	s.SetStatus(string(status))
	s.SetNote(note)
	_, err := s.Insert(ctx, kdata.Executor(ctx, r.DB))
	return kdata.MapErr(err)
}

func (r *OrderRepo) Get(ctx context.Context, id string) (domain.Order, error) {
	o, err := r.read(ctx, id, false)
	if err != nil {
		return domain.Order{}, err
	}
	key, _ := uuid.Parse(id)
	steps, err := orderstep.New().Where(orderstep.OrderID.Eq(key)).Order(orderstep.CreatedAt.Asc()).
		List(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return domain.Order{}, kdata.MapErr(err)
	}
	for _, s := range steps {
		o.Steps = append(o.Steps, domain.Step{Status: domain.Status(s.Status), Note: s.Note, At: s.CreatedAt})
	}
	return o, nil
}

func (r *OrderRepo) Lock(ctx context.Context, id string) (domain.Order, error) {
	if !kdata.InTx(ctx) {
		return domain.Order{}, kakarot.E(kakarot.CodeInternal, "Lock needs a transaction", nil)
	}
	return r.read(ctx, id, true)
}

func (r *OrderRepo) read(ctx context.Context, id string, lock bool) (domain.Order, error) {
	key, err := uuid.Parse(id)
	if err != nil {
		return domain.Order{}, kakarot.NotFound("order", id)
	}
	ex := kdata.Executor(ctx, r.DB)
	q := order.New().Where(order.ID.Eq(key))
	if lock {
		q = q.ForUpdate()
	}
	row, found, err := q.One(ctx, ex)
	if err != nil {
		return domain.Order{}, kdata.MapErr(err)
	}
	if !found {
		return domain.Order{}, kakarot.NotFound("order", id)
	}
	lines, err := orderline.New().Where(orderline.OrderID.Eq(key)).Order(orderline.Sku.Asc()).List(ctx, ex)
	if err != nil {
		return domain.Order{}, kdata.MapErr(err)
	}
	o := toOrder(row)
	for _, l := range lines {
		o.Lines = append(o.Lines, domain.Line{SKU: l.Sku, Qty: l.Qty, Price: l.Price})
	}
	return o, nil
}

func (r *OrderRepo) Save(ctx context.Context, o domain.Order, note string) error {
	key, err := uuid.Parse(o.ID)
	if err != nil {
		return kakarot.NotFound("order", o.ID)
	}
	m := order.MutateKey(key)
	m.SetStatus(string(o.Status))
	m.SetChargeID(o.ChargeID)
	m.SetTracking(o.Tracking)
	m.SetReason(o.Reason)
	m.SetRefunded(o.Refunded)
	if err := m.Update(ctx, kdata.Executor(ctx, r.DB)); err != nil {
		return kdata.MapErr(err)
	}
	return r.step(ctx, key, o.Status, note)
}

func (r *OrderRepo) Stale(ctx context.Context, before time.Time, limit int) ([]string, error) {
	rows, err := order.New().
		Where(order.Status.Eq(string(domain.StatusPending)), order.CreatedAt.Lt(before)).
		Order(order.CreatedAt.Asc()).Limit(int64(limit)).
		List(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return nil, kdata.MapErr(err)
	}
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = uuid.UUID(row.ID).String()
	}
	return ids, nil
}

func toProduct(r product.Row) domain.Product {
	return domain.Product{SKU: r.Sku, Name: r.Name, Price: r.Price, Stock: r.Stock}
}

func toOrder(r order.Row) domain.Order {
	return domain.Order{
		ID: uuid.UUID(r.ID).String(), Customer: r.Customer, Status: domain.Status(r.Status), Total: r.Total,
		Tracking: r.Tracking, Reason: r.Reason, Refunded: r.Refunded, CreatedAt: r.CreatedAt,
		PaymentToken: r.PaymentToken, ChargeID: r.ChargeID,
	}
}

// The outbox side of each topic. The names match the //kakarot:queue lines
// on the service interface.
var (
	orderPlaced  = outbox.Topic[domain.OrderPlaced]("order.placed")
	orderPaid    = outbox.Topic[domain.OrderPaid]("order.paid")
	refundDue    = outbox.Topic[domain.RefundDue]("order.refund_due")
	orderUpdated = outbox.Topic[domain.OrderUpdated]("order.updated")
)

// Events writes domain events to the transactional outbox: an event exists
// if and only if the change it reports was committed.
type Events struct{ DB storm.DB }

var _ usecase.Events = (*Events)(nil)

func (e *Events) Placed(ctx context.Context, ev domain.OrderPlaced) error {
	return orderPlaced.Add(ctx, e.DB, ev.OrderID, ev)
}

func (e *Events) Paid(ctx context.Context, ev domain.OrderPaid) error {
	return orderPaid.Add(ctx, e.DB, ev.OrderID, ev)
}

func (e *Events) RefundDue(ctx context.Context, ev domain.RefundDue) error {
	return refundDue.Add(ctx, e.DB, ev.OrderID, ev)
}

func (e *Events) Updated(ctx context.Context, ev domain.OrderUpdated) error {
	return orderUpdated.Add(ctx, e.DB, ev.OrderID, ev)
}
