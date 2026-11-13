// Package domain holds the wallet's domain types, which are also its DTOs.
// Amounts are integer minor units (cents), never floats.
package domain

import (
	"time"

	"github.com/gsoultan/kakarot"
)

// CodeInsufficientFunds is this service's own error code; main maps it to
// HTTP 422.
var CodeInsufficientFunds = kakarot.NewCode("insufficient_funds")

// User is the authenticated caller: the typed principal carried in context.
type User struct {
	ID string `json:"id"`
}

// Account holds a balance for one owner.
type Account struct {
	ID        string    `json:"id"`
	Owner     string    `json:"owner"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

// OpenAccount opens an account for the caller. It has no fields: the owner
// comes from the principal, never from the request.
type OpenAccount struct{}

// GetAccount reads one account.
type GetAccount struct {
	ID string `path:"id" json:"id"`
}

// Deposit adds money to an account.
type Deposit struct {
	AccountID string `path:"id" json:"account_id"`
	Amount    int64  `json:"amount"`
}

func (d Deposit) Validate() error {
	fields := map[string]string{}
	if d.AccountID == "" {
		fields["account_id"] = "required"
	}
	if d.Amount <= 0 {
		fields["amount"] = "must be positive"
	}
	if len(fields) > 0 {
		return kakarot.Invalid("invalid deposit", fields)
	}
	return nil
}

// Transfer moves money between two accounts.
type Transfer struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int64  `json:"amount"`
	// Key makes a retried transfer safe; HTTP callers send it as the
	// Idempotency-Key header.
	Key string `header:"Idempotency-Key" json:"idempotency_key,omitempty"`
}

func (t Transfer) Validate() error {
	fields := map[string]string{}
	if t.From == "" {
		fields["from"] = "required"
	}
	if t.To == "" {
		fields["to"] = "required"
	}
	if t.From != "" && t.From == t.To {
		fields["to"] = "must differ from from"
	}
	if t.Amount <= 0 {
		fields["amount"] = "must be positive"
	}
	if len(fields) > 0 {
		return kakarot.Invalid("invalid transfer", fields)
	}
	return nil
}

// IdempotencyKey implements middleware.Keyed. The wiring scopes keys to the
// authenticated caller, so one user can never replay another's transfer.
func (t Transfer) IdempotencyKey() string { return t.Key }

// TransferDone is the result of a transfer and the payload of the
// transfer.completed event.
type TransferDone struct {
	ID          string    `json:"id"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Amount      int64     `json:"amount"`
	FromBalance int64     `json:"from_balance"`
	At          time.Time `json:"at"`
}

// EntryKind says why a balance changed.
type EntryKind string

const (
	KindDeposit     EntryKind = "deposit"
	KindTransferOut EntryKind = "transfer_out"
	KindTransferIn  EntryKind = "transfer_in"
)

// Entry is one line of an account's ledger. Amount is signed.
type Entry struct {
	ID        string    `json:"id"`
	AccountID string    `json:"account_id"`
	Kind      EntryKind `json:"kind"`
	Amount    int64     `json:"amount"`
	Ref       string    `json:"ref,omitempty"` // transfer id
	CreatedAt time.Time `json:"created_at"`
}

// ListEntries pages through an account's ledger, oldest first.
type ListEntries struct {
	AccountID string `path:"id" json:"account_id"`
	kakarot.PageRequest
}

// ImportResult summarizes a bulk deposit import.
type ImportResult struct {
	Deposits int   `json:"deposits"`
	Total    int64 `json:"total"`
}

// Audited is the result of recording a completed transfer.
type Audited struct {
	TransferID string `json:"transfer_id"`
}
