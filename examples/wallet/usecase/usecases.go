package usecase

import (
	"context"
	"iter"
	"time"

	"github.com/google/uuid"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
)

// OpenAccount opens an account owned by the caller.
type OpenAccount struct{ Accounts Accounts }

func (u *OpenAccount) Execute(ctx context.Context, _ domain.OpenAccount) (domain.Account, error) {
	who, err := caller(ctx)
	if err != nil {
		return domain.Account{}, err
	}
	return u.Accounts.Create(ctx, who.ID)
}

// GetAccount reads one of the caller's accounts.
type GetAccount struct{ Accounts Accounts }

func (u *GetAccount) Execute(ctx context.Context, in domain.GetAccount) (domain.Account, error) {
	who, err := caller(ctx)
	if err != nil {
		return domain.Account{}, err
	}
	a, err := u.Accounts.Get(ctx, in.ID)
	if err != nil {
		return domain.Account{}, err
	}
	return a, owned(who, a)
}

// Deposit adds money to one of the caller's accounts.
type Deposit struct {
	Accounts Accounts
	Tx       kakarot.TxManager
}

func (u *Deposit) Execute(ctx context.Context, in domain.Deposit) (domain.Account, error) {
	who, err := caller(ctx)
	if err != nil {
		return domain.Account{}, err
	}
	var out domain.Account
	err = u.Tx.Within(ctx, func(ctx context.Context) error {
		locked, err := u.Accounts.Lock(ctx, in.AccountID)
		if err != nil {
			return err
		}
		a := locked[in.AccountID]
		if err := owned(who, a); err != nil {
			return err
		}
		a.Balance, err = u.Accounts.Apply(ctx, a, domain.KindDeposit, in.Amount, "")
		out = a
		return err
	})
	return out, err
}

// Transfer moves money from one of the caller's accounts to any account.
// Both rows are locked, both balances and ledger entries change, and the
// event is written, in one transaction: all of it happens or none of it.
type Transfer struct {
	Accounts Accounts
	Events   Events
	Tx       kakarot.TxManager
}

func (u *Transfer) Execute(ctx context.Context, in domain.Transfer) (domain.TransferDone, error) {
	who, err := caller(ctx)
	if err != nil {
		return domain.TransferDone{}, err
	}
	done := domain.TransferDone{ID: uuid.NewString(), From: in.From, To: in.To, Amount: in.Amount, At: time.Now().UTC()}
	err = u.Tx.Within(ctx, func(ctx context.Context) error {
		locked, err := u.Accounts.Lock(ctx, in.From, in.To)
		if err != nil {
			return err
		}
		from, to := locked[in.From], locked[in.To]
		if err := owned(who, from); err != nil {
			return err
		}
		if from.Balance < in.Amount {
			return kakarot.E(domain.CodeInsufficientFunds, "balance is lower than the transfer amount", nil)
		}
		if done.FromBalance, err = u.Accounts.Apply(ctx, from, domain.KindTransferOut, -in.Amount, done.ID); err != nil {
			return err
		}
		if _, err = u.Accounts.Apply(ctx, to, domain.KindTransferIn, in.Amount, done.ID); err != nil {
			return err
		}
		return u.Events.TransferCompleted(ctx, done)
	})
	if err != nil {
		return domain.TransferDone{}, err
	}
	return done, nil
}

// ListEntries pages through the ledger of one of the caller's accounts.
type ListEntries struct{ Accounts Accounts }

func (u *ListEntries) Execute(ctx context.Context, in domain.ListEntries) (kakarot.Page[domain.Entry], error) {
	who, err := caller(ctx)
	if err != nil {
		return kakarot.Page[domain.Entry]{}, err
	}
	a, err := u.Accounts.Get(ctx, in.AccountID)
	if err != nil {
		return kakarot.Page[domain.Entry]{}, err
	}
	if err := owned(who, a); err != nil {
		return kakarot.Page[domain.Entry]{}, err
	}
	return u.Accounts.Entries(ctx, in.AccountID, in.Cursor, in.Size(20, 100))
}

// ImportDeposits applies a stream of deposits, each in its own transaction,
// and stops at the first one that fails.
type ImportDeposits struct {
	Deposit kakarot.Usecase[domain.Deposit, domain.Account]
}

func (u *ImportDeposits) Stream(ctx context.Context, in iter.Seq2[domain.Deposit, error]) (domain.ImportResult, error) {
	var res domain.ImportResult
	for d, err := range in {
		if err != nil {
			return res, err
		}
		if err := d.Validate(); err != nil {
			return res, err
		}
		if _, err := u.Deposit.Execute(ctx, d); err != nil {
			return res, err
		}
		res.Deposits++
		res.Total += d.Amount
	}
	return res, nil
}

// AuditTransfer records a completed transfer. It runs from the queue, so it
// has no caller; events arrive at least once and Audit must accept repeats.
type AuditTransfer struct{ Audit Audit }

func (u *AuditTransfer) Execute(ctx context.Context, t domain.TransferDone) (domain.Audited, error) {
	if err := u.Audit.Record(ctx, t); err != nil {
		return domain.Audited{}, err
	}
	return domain.Audited{TransferID: t.ID}, nil
}
