// Package model holds the storm models: the wallet's database types.
package model

import "github.com/gsoultan/storm"

// Account is the accounts table.
type Account struct {
	storm.Model // id uuid, created_at, updated_at

	Owner   string
	Balance int64
}

func (a *Account) Schema(t *storm.Table) {
	t.Col(&a.Owner).Size(64)
	t.Col(&a.Balance).Default("0")
	t.Index(&a.Owner)
	t.Check(storm.RawSQL(`balance >= 0`)) // the database refuses an overdraft too
}

// Entry is the entries table: the ledger.
type Entry struct {
	storm.Model

	Kind   string
	Amount int64
	Ref    string

	Account Account // foreign key account_id -> accounts.id
}

func (e *Entry) Schema(t *storm.Table) {
	t.Col(&e.Kind).Size(32)
	t.Col(&e.Ref).Size(64).Default("''")
	t.Col(&e.Account).OnDelete(storm.Restrict)
}
