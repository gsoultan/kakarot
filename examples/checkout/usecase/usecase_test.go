package usecase_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/checkout/domain"
	"github.com/gsoultan/kakarot/examples/checkout/usecase"
)

// world is every port in memory. The business rules are tested with no
// database, broker or provider because usecases depend only on ports.
type world struct {
	stock   map[string]int64
	orders  map[string]domain.Order
	placed  []domain.OrderPlaced
	paid    []domain.OrderPaid
	refunds []domain.RefundDue
	updates []domain.OrderUpdated

	chargeErr error
	charged   int
	refunded  []string
	// duringCharge runs while the provider is "working", to interleave a
	// synchronous request with the handler.
	duringCharge func()
}

func newWorld() *world {
	return &world{stock: map[string]int64{"tea": 5}, orders: map[string]domain.Order{}}
}

// Catalog
func (w *world) Add(_ context.Context, p domain.Product) (domain.Product, error) { return p, nil }
func (w *world) Get(_ context.Context, sku string) (domain.Product, error) {
	return domain.Product{SKU: sku, Stock: w.stock[sku]}, nil
}

func (w *world) Reserve(_ context.Context, items []domain.Item) ([]domain.Line, error) {
	var lines []domain.Line
	for _, it := range items {
		if w.stock[it.SKU] < it.Qty {
			return nil, kakarot.E(domain.CodeOutOfStock, "not enough stock", nil)
		}
		w.stock[it.SKU] -= it.Qty
		lines = append(lines, domain.Line{SKU: it.SKU, Qty: it.Qty, Price: 500})
	}
	return lines, nil
}

func (w *world) Release(_ context.Context, lines []domain.Line) error {
	for _, l := range lines {
		w.stock[l.SKU] += l.Qty
	}
	return nil
}

// Orders
func (w *world) Create(_ context.Context, o domain.Order, _ string) (domain.Order, error) {
	o.ID = "o" + strconv.Itoa(len(w.orders)+1)
	o.CreatedAt = time.Unix(1000, 0)
	w.orders[o.ID] = o
	return o, nil
}

func (w *world) order(id string) (domain.Order, error) {
	o, ok := w.orders[id]
	if !ok {
		return domain.Order{}, kakarot.NotFound("order", id)
	}
	return o, nil
}

// ordersOf is the Orders view of the world: Catalog and Orders both have a
// Get, so they cannot be one type.
type ordersOf struct{ *world }

func (o ordersOf) Get(_ context.Context, id string) (domain.Order, error) { return o.order(id) }

func (w *world) Lock(_ context.Context, id string) (domain.Order, error) { return w.order(id) }
func (w *world) Save(_ context.Context, o domain.Order, _ string) error {
	w.orders[o.ID] = o
	return nil
}

func (w *world) Stale(_ context.Context, before time.Time, _ int) ([]string, error) {
	var ids []string
	for id, o := range w.orders {
		if o.Status == domain.StatusPending && o.CreatedAt.Before(before) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// Events
func (w *world) Placed(_ context.Context, e domain.OrderPlaced) error {
	w.placed = append(w.placed, e)
	return nil
}
func (w *world) Paid(_ context.Context, e domain.OrderPaid) error {
	w.paid = append(w.paid, e)
	return nil
}
func (w *world) RefundDue(_ context.Context, e domain.RefundDue) error {
	w.refunds = append(w.refunds, e)
	return nil
}
func (w *world) Updated(_ context.Context, e domain.OrderUpdated) error {
	w.updates = append(w.updates, e)
	return nil
}

// Payments
func (w *world) Charge(_ context.Context, key, _ string, _ int64) (string, error) {
	if w.duringCharge != nil {
		w.duringCharge()
	}
	if w.chargeErr != nil {
		return "", w.chargeErr
	}
	w.charged++
	return "ch_" + key, nil
}

func (w *world) Refund(_ context.Context, chargeID string) error {
	w.refunded = append(w.refunded, chargeID)
	return nil
}

// Warehouse
func (w *world) Ship(_ context.Context, o domain.Order) (string, error) { return "TRK-" + o.ID, nil }

type noTx struct{}

func (noTx) Within(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

func (w *world) place(t *testing.T) domain.Order {
	t.Helper()
	uc := &usecase.PlaceOrder{Catalog: w, Orders: ordersOf{w}, Events: w, Tx: noTx{}}
	o, err := uc.Execute(t.Context(), domain.PlaceOrder{Customer: "ana", PaymentToken: "tok", Items: []domain.Item{{SKU: "tea", Qty: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func (w *world) charge() *usecase.ChargePayment {
	return &usecase.ChargePayment{Catalog: w, Orders: ordersOf{w}, Events: w, Payments: w, Tx: noTx{}}
}

func TestPlaceOrderReservesAndAsksForPayment(t *testing.T) {
	w := newWorld()
	o := w.place(t)
	if o.Status != domain.StatusPending || o.Total != 1000 || w.stock["tea"] != 3 {
		t.Fatalf("%+v stock=%d", o, w.stock["tea"])
	}
	if len(w.placed) != 1 || w.placed[0].OrderID != o.ID {
		t.Fatalf("events = %+v", w.placed)
	}
	if w.charged != 0 {
		t.Fatal("the request must not call the payment provider")
	}
}

func TestChargeApprovedThenRepeatedMessage(t *testing.T) {
	w := newWorld()
	o := w.place(t)
	if _, err := w.charge().Execute(t.Context(), domain.OrderPlaced{OrderID: o.ID}); err != nil {
		t.Fatal(err)
	}
	if got := w.orders[o.ID]; got.Status != domain.StatusPaid || got.ChargeID == "" || len(w.paid) != 1 {
		t.Fatalf("%+v paid=%v", got, w.paid)
	}
	// The broker delivers the same message again.
	h, err := w.charge().Execute(t.Context(), domain.OrderPlaced{OrderID: o.ID})
	if err != nil || !h.Skipped || w.charged != 1 || len(w.paid) != 1 {
		t.Fatalf("repeat: %+v %v charged=%d paid=%d", h, err, w.charged, len(w.paid))
	}
}

func TestChargeDeclinedCompensates(t *testing.T) {
	w := newWorld()
	o := w.place(t)
	w.chargeErr = kakarot.E(domain.CodeDeclined, "card declined", nil)
	if _, err := w.charge().Execute(t.Context(), domain.OrderPlaced{OrderID: o.ID}); err != nil {
		t.Fatalf("a declined card is a result, not a failure to retry: %v", err)
	}
	if got := w.orders[o.ID]; got.Status != domain.StatusCancelled || got.Reason != "payment declined" || w.stock["tea"] != 5 {
		t.Fatalf("%+v stock=%d", got, w.stock["tea"])
	}
}

func TestChargeOutageAsksForRedelivery(t *testing.T) {
	w := newWorld()
	o := w.place(t)
	w.chargeErr = kakarot.E(kakarot.CodeUnavailable, "provider down", nil)
	if _, err := w.charge().Execute(t.Context(), domain.OrderPlaced{OrderID: o.ID}); !kakarot.Is(err, kakarot.CodeUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if w.orders[o.ID].Status != domain.StatusPending || w.stock["tea"] != 3 {
		t.Fatal("an outage must leave the order waiting")
	}
}

func TestCancelDuringChargeRequestsRefund(t *testing.T) {
	w := newWorld()
	o := w.place(t)
	cancel := &usecase.CancelOrder{Catalog: w, Orders: ordersOf{w}, Events: w, Tx: noTx{}}
	w.duringCharge = func() {
		if _, err := cancel.Execute(t.Context(), domain.CancelOrder{ID: o.ID}); err != nil {
			t.Error(err)
		}
	}
	if _, err := w.charge().Execute(t.Context(), domain.OrderPlaced{OrderID: o.ID}); err != nil {
		t.Fatal(err)
	}
	if got := w.orders[o.ID]; got.Status != domain.StatusCancelled || w.stock["tea"] != 5 || len(w.paid) != 0 {
		t.Fatalf("%+v stock=%d paid=%v", got, w.stock["tea"], w.paid)
	}
	if len(w.refunds) != 1 || w.refunds[0].Amount != 1000 {
		t.Fatalf("refunds = %+v", w.refunds)
	}

	refund := &usecase.RefundPayment{Orders: ordersOf{w}, Events: w, Payments: w, Tx: noTx{}}
	for range 2 { // delivered twice, refunded once
		if _, err := refund.Execute(t.Context(), w.refunds[0]); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.refunded) != 1 || !w.orders[o.ID].Refunded {
		t.Fatalf("refunded = %v", w.refunded)
	}
}

func TestCancelRules(t *testing.T) {
	w := newWorld()
	o := w.place(t)
	if _, err := w.charge().Execute(t.Context(), domain.OrderPlaced{OrderID: o.ID}); err != nil {
		t.Fatal(err)
	}
	cancel := &usecase.CancelOrder{Catalog: w, Orders: ordersOf{w}, Events: w, Tx: noTx{}}
	if _, err := cancel.Execute(t.Context(), domain.CancelOrder{ID: o.ID}); !kakarot.Is(err, kakarot.CodeConflict) {
		t.Fatalf("cancelling a paid order: %v", err)
	}
	if _, err := cancel.Execute(t.Context(), domain.CancelOrder{ID: "missing"}); !kakarot.Is(err, kakarot.CodeNotFound) {
		t.Fatalf("unknown order: %v", err)
	}
}

func TestShipOnlyPaidOrders(t *testing.T) {
	w := newWorld()
	o := w.place(t)
	ship := &usecase.ShipOrder{Orders: ordersOf{w}, Events: w, Warehouse: w, Tx: noTx{}}
	if h, _ := ship.Execute(t.Context(), domain.OrderPaid{OrderID: o.ID}); !h.Skipped {
		t.Fatal("shipped an unpaid order")
	}
	if _, err := w.charge().Execute(t.Context(), domain.OrderPlaced{OrderID: o.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := ship.Execute(t.Context(), domain.OrderPaid{OrderID: o.ID}); err != nil {
		t.Fatal(err)
	}
	if got := w.orders[o.ID]; got.Status != domain.StatusShipped || got.Tracking != "TRK-"+o.ID {
		t.Fatalf("%+v", got)
	}
}

func TestExpireCancelsOnlyOldPendingOrders(t *testing.T) {
	w := newWorld()
	old, paid := w.place(t), w.place(t)
	if _, err := w.charge().Execute(t.Context(), domain.OrderPlaced{OrderID: paid.ID}); err != nil {
		t.Fatal(err)
	}
	expire := &usecase.ExpireOrders{
		Catalog: w, Orders: ordersOf{w}, Events: w, Tx: noTx{}, After: 15 * time.Minute,
		Now: func() time.Time { return time.Unix(1000, 0).Add(10 * time.Minute) },
	}
	if n, err := expire.Run(t.Context()); n != 0 || err != nil {
		t.Fatalf("inside the window: %d %v", n, err)
	}
	expire.Now = func() time.Time { return time.Unix(1000, 0).Add(time.Hour) }
	if n, err := expire.Run(t.Context()); n != 1 || err != nil {
		t.Fatalf("after the window: %d %v", n, err)
	}
	if w.orders[old.ID].Reason != "payment timed out" || w.orders[paid.ID].Status != domain.StatusPaid || w.stock["tea"] != 3 {
		t.Fatalf("%+v stock=%d", w.orders, w.stock["tea"])
	}
}
