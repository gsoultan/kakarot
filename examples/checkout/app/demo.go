package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/checkout/domain"
)

// DemoPayments stands in for a payment provider so the sample runs with no
// account anywhere. The token chooses what happens:
//
//	tok_declined  the card is declined
//	tok_flaky     the provider is down for the first four calls, then approves
//	tok_slow      the provider takes five seconds, then approves
//	anything else approved at once
//
// Like a real provider, it is idempotent: a second Charge with the same key
// returns the first charge.
type DemoPayments struct {
	Log *slog.Logger

	mu      sync.Mutex
	charges map[string]string
	calls   map[string]int
}

func (p *DemoPayments) Charge(ctx context.Context, key, token string, amount int64) (string, error) {
	p.mu.Lock()
	if p.charges == nil {
		p.charges, p.calls = map[string]string{}, map[string]int{}
	}
	if id, ok := p.charges[key]; ok {
		p.mu.Unlock()
		return id, nil
	}
	p.calls[key]++
	calls := p.calls[key]
	p.mu.Unlock()

	switch token {
	case "tok_declined":
		return "", kakarot.E(domain.CodeDeclined, "card declined", nil)
	case "tok_flaky":
		if calls <= 4 {
			return "", kakarot.E(kakarot.CodeUnavailable, "payment provider is down", nil)
		}
	case "tok_slow":
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return "", kakarot.E(kakarot.CodeTimeout, "payment provider timed out", ctx.Err())
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id := "ch_" + key
	p.charges[key] = id
	p.Log.InfoContext(ctx, "charged", "charge", id, "amount", amount)
	return id, nil
}

func (p *DemoPayments) Refund(ctx context.Context, chargeID string) error {
	p.Log.InfoContext(ctx, "refunded", "charge", chargeID)
	return nil
}

// DemoWarehouse books a shipment and derives the tracking number from the
// order id, so booking twice gives the same answer.
type DemoWarehouse struct{}

func (DemoWarehouse) Ship(_ context.Context, o domain.Order) (string, error) {
	return "TRK-" + o.ID[:8], nil
}

// LogNotifier writes customer notifications to the log.
type LogNotifier struct{ Log *slog.Logger }

func (n LogNotifier) Notify(ctx context.Context, e domain.OrderUpdated) error {
	n.Log.InfoContext(ctx, "notify customer", "customer", e.Customer, "order", e.OrderID, "status", e.Status, "note", e.Note)
	return nil
}
