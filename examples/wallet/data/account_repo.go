// Package data implements the wallet's ports with storm. data/model holds
// the database types and data/store the code storm generates from them;
// nothing above this package sees either.
package data

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/gsoultan/kakarot"
	kdata "github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/kakarot/data/outbox"
	"github.com/gsoultan/kakarot/examples/wallet/data/store/account"
	"github.com/gsoultan/kakarot/examples/wallet/data/store/entry"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
	"github.com/gsoultan/kakarot/examples/wallet/usecase"
	"github.com/gsoultan/storm"
)

// AccountRepo stores accounts and their ledger.
type AccountRepo struct{ DB storm.DB }

var _ usecase.Accounts = (*AccountRepo)(nil)

func (r *AccountRepo) Create(ctx context.Context, owner string) (domain.Account, error) {
	c := account.Create()
	c.SetOwner(owner)
	row, err := c.Insert(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return domain.Account{}, kdata.MapErr(err)
	}
	return toAccount(row), nil
}

func (r *AccountRepo) Get(ctx context.Context, id string) (domain.Account, error) {
	key, err := uuid.Parse(id)
	if err != nil {
		return domain.Account{}, kakarot.NotFound("account", id)
	}
	row, found, err := account.New().Where(account.ID.Eq(key)).One(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return domain.Account{}, kdata.MapErr(err)
	}
	if !found {
		return domain.Account{}, kakarot.NotFound("account", id)
	}
	return toAccount(row), nil
}

// Lock selects the accounts FOR UPDATE, ordered by id. Every transaction
// takes its locks in that order, so concurrent transfers between the same
// accounts wait for each other instead of deadlocking.
func (r *AccountRepo) Lock(ctx context.Context, ids ...string) (map[string]domain.Account, error) {
	if !kdata.InTx(ctx) {
		return nil, kakarot.E(kakarot.CodeInternal, "Lock needs a transaction", nil)
	}
	keys := make([][16]byte, 0, len(ids))
	for _, id := range ids {
		key, err := uuid.Parse(id)
		if err != nil {
			return nil, kakarot.NotFound("account", id)
		}
		keys = append(keys, key)
	}
	rows, err := account.New().Where(account.ID.In(keys...)).Order(account.ID.Asc()).ForUpdate().
		List(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return nil, kdata.MapErr(err)
	}
	out := make(map[string]domain.Account, len(rows))
	for _, row := range rows {
		a := toAccount(row)
		out[a.ID] = a
	}
	for _, id := range ids {
		if _, ok := out[id]; !ok {
			return nil, kakarot.NotFound("account", id)
		}
	}
	return out, nil
}

// Apply adds delta to the balance in SQL (balance = balance + delta) and
// writes the ledger entry. Call it on an account returned by Lock.
func (r *AccountRepo) Apply(ctx context.Context, a domain.Account, kind domain.EntryKind, delta int64, ref string) (int64, error) {
	ex := kdata.Executor(ctx, r.DB)
	key, err := uuid.Parse(a.ID)
	if err != nil {
		return 0, kakarot.NotFound("account", a.ID)
	}
	m := account.MutateKey(key)
	m.AddBalance(delta)
	if err := m.Update(ctx, ex); err != nil {
		return 0, kdata.MapErr(err)
	}
	e := entry.Create()
	e.SetAccountID(key)
	e.SetKind(string(kind))
	e.SetAmount(delta)
	e.SetRef(ref)
	if _, err := e.Insert(ctx, ex); err != nil {
		return 0, kdata.MapErr(err)
	}
	return a.Balance + delta, nil
}

type entryCursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"id"`
}

// Entries pages through an account's ledger with a keyset cursor.
func (r *AccountRepo) Entries(ctx context.Context, accountID, cursor string, limit int) (kakarot.Page[domain.Entry], error) {
	key, err := uuid.Parse(accountID)
	if err != nil {
		return kakarot.Page[domain.Entry]{}, kakarot.NotFound("account", accountID)
	}
	q := entry.New().Where(entry.AccountID.Eq(key)).
		Order(entry.CreatedAt.Asc(), entry.ID.Asc()).Limit(int64(limit + 1))
	c, ok, err := kakarot.DecodeCursor[entryCursor](cursor)
	if err != nil {
		return kakarot.Page[domain.Entry]{}, err
	}
	if ok {
		q = q.After(entry.Row{CreatedAt: c.CreatedAt, ID: c.ID})
	}
	rows, err := q.List(ctx, kdata.Executor(ctx, r.DB))
	if err != nil {
		return kakarot.Page[domain.Entry]{}, kdata.MapErr(err)
	}
	return kakarot.NextPage(rows, limit, toEntry,
		func(row entry.Row) entryCursor { return entryCursor{CreatedAt: row.CreatedAt, ID: row.ID} })
}

func toAccount(r account.Row) domain.Account {
	return domain.Account{ID: uuid.UUID(r.ID).String(), Owner: r.Owner, Balance: r.Balance, CreatedAt: r.CreatedAt}
}

func toEntry(r entry.Row) domain.Entry {
	return domain.Entry{
		ID: uuid.UUID(r.ID).String(), AccountID: uuid.UUID(r.AccountID).String(),
		Kind: domain.EntryKind(r.Kind), Amount: r.Amount, Ref: r.Ref, CreatedAt: r.CreatedAt,
	}
}

// TransferCompleted is the outbox side of the transfer.completed topic.
var TransferCompleted = outbox.Topic[domain.TransferDone]("transfer.completed")

// Events writes domain events to the transactional outbox.
type Events struct{ DB storm.DB }

var _ usecase.Events = (*Events)(nil)

func (e *Events) TransferCompleted(ctx context.Context, t domain.TransferDone) error {
	return TransferCompleted.Add(ctx, e.DB, t.ID, t)
}
