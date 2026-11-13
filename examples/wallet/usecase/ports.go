// Package usecase holds the wallet's business rules and the ports they need.
package usecase

import (
	"context"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/auth"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
)

// Accounts is the persistence port.
type Accounts interface {
	Create(ctx context.Context, owner string) (domain.Account, error)
	Get(ctx context.Context, id string) (domain.Account, error)
	// Lock returns the accounts locked for update until the transaction in
	// ctx ends, always in the same order so two transfers cannot deadlock.
	Lock(ctx context.Context, ids ...string) (map[string]domain.Account, error)
	// Apply changes an account's balance by delta, records the ledger entry
	// and returns the new balance.
	Apply(ctx context.Context, a domain.Account, kind domain.EntryKind, delta int64, ref string) (int64, error)
	Entries(ctx context.Context, accountID, cursor string, limit int) (kakarot.Page[domain.Entry], error)
}

// Events is the port for domain events (the transactional outbox).
type Events interface {
	TransferCompleted(ctx context.Context, t domain.TransferDone) error
}

// Audit is the port a compliance system implements.
type Audit interface {
	Record(ctx context.Context, t domain.TransferDone) error
}

// caller returns the authenticated user. Transport middleware put it in ctx;
// the same code serves HTTP and gRPC.
func caller(ctx context.Context) (domain.User, error) {
	u, ok := auth.From[domain.User](ctx)
	if !ok {
		return domain.User{}, kakarot.E(kakarot.CodeUnauthorized, "authentication required", nil)
	}
	return u, nil
}

// owned checks that the caller owns the account. A foreign account answers
// not_found, not forbidden, so account ids cannot be probed.
func owned(u domain.User, a domain.Account) error {
	if a.Owner != u.ID {
		return kakarot.NotFound("account", a.ID)
	}
	return nil
}
