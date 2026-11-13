package usecase_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/auth"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
	"github.com/gsoultan/kakarot/examples/wallet/usecase"
	"github.com/gsoultan/kakarot/kakarottest"
)

// memAccounts is an in-memory Accounts: the business rules are tested with
// no database because usecases depend only on ports.
type memAccounts struct {
	rows    map[string]domain.Account
	entries []domain.Entry
	failOn  domain.EntryKind
}

func (m *memAccounts) Create(_ context.Context, owner string) (domain.Account, error) {
	a := domain.Account{ID: "acc-" + strconv.Itoa(len(m.rows)+1), Owner: owner}
	m.rows[a.ID] = a
	return a, nil
}

func (m *memAccounts) Get(_ context.Context, id string) (domain.Account, error) {
	a, ok := m.rows[id]
	if !ok {
		return domain.Account{}, kakarot.NotFound("account", id)
	}
	return a, nil
}

func (m *memAccounts) Lock(ctx context.Context, ids ...string) (map[string]domain.Account, error) {
	out := map[string]domain.Account{}
	for _, id := range ids {
		a, err := m.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		out[id] = a
	}
	return out, nil
}

func (m *memAccounts) Apply(_ context.Context, a domain.Account, kind domain.EntryKind, delta int64, ref string) (int64, error) {
	if kind == m.failOn {
		return 0, errors.New("disk full")
	}
	a.Balance += delta
	m.rows[a.ID] = a
	m.entries = append(m.entries, domain.Entry{AccountID: a.ID, Kind: kind, Amount: delta, Ref: ref})
	return a.Balance, nil
}

func (m *memAccounts) Entries(context.Context, string, string, int) (kakarot.Page[domain.Entry], error) {
	return kakarot.Page[domain.Entry]{Items: m.entries}, nil
}

// tx gives the fake real transaction semantics: kakarottest.Tx calls the
// hooks, and a failed function leaves no change behind.
func (m *memAccounts) tx() *kakarottest.Tx {
	var rows map[string]domain.Account
	var entries []domain.Entry
	return &kakarottest.Tx{
		OnBegin: func() {
			rows = map[string]domain.Account{}
			for k, v := range m.rows {
				rows[k] = v
			}
			entries = append([]domain.Entry{}, m.entries...)
		},
		OnRollback: func() { m.rows, m.entries = rows, entries },
	}
}

type memEvents struct{ done []domain.TransferDone }

func (e *memEvents) TransferCompleted(_ context.Context, t domain.TransferDone) error {
	e.done = append(e.done, t)
	return nil
}

func as(t *testing.T, user string) context.Context {
	return auth.With(t.Context(), domain.User{ID: user})
}

func fixture() (*memAccounts, *memEvents, *usecase.Transfer) {
	m := &memAccounts{rows: map[string]domain.Account{
		"a": {ID: "a", Owner: "alice", Balance: 100},
		"b": {ID: "b", Owner: "bob", Balance: 5},
	}}
	ev := &memEvents{}
	return m, ev, &usecase.Transfer{Accounts: m, Events: ev, Tx: m.tx()}
}

func TestTransferMovesMoneyAndEmitsEvent(t *testing.T) {
	m, ev, uc := fixture()
	done, err := uc.Execute(as(t, "alice"), domain.Transfer{From: "a", To: "b", Amount: 30})
	if err != nil {
		t.Fatal(err)
	}
	if m.rows["a"].Balance != 70 || m.rows["b"].Balance != 35 || done.FromBalance != 70 {
		t.Fatalf("balances a=%d b=%d done=%+v", m.rows["a"].Balance, m.rows["b"].Balance, done)
	}
	if len(m.entries) != 2 || m.entries[0].Amount != -30 || m.entries[1].Amount != 30 || m.entries[0].Ref != done.ID {
		t.Fatalf("ledger = %+v", m.entries)
	}
	if len(ev.done) != 1 || ev.done[0].ID != done.ID {
		t.Fatalf("events = %+v", ev.done)
	}
}

func TestTransferRules(t *testing.T) {
	cases := []struct {
		name string
		user string
		in   domain.Transfer
		want kakarot.Code
	}{
		{"insufficient funds", "alice", domain.Transfer{From: "a", To: "b", Amount: 101}, domain.CodeInsufficientFunds},
		{"someone else's account looks missing", "bob", domain.Transfer{From: "a", To: "b", Amount: 1}, kakarot.CodeNotFound},
		{"unknown destination", "alice", domain.Transfer{From: "a", To: "zzz", Amount: 1}, kakarot.CodeNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, ev, uc := fixture()
			_, err := uc.Execute(as(t, c.user), c.in)
			if kakarot.CodeOf(err) != c.want {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			if m.rows["a"].Balance != 100 || m.rows["b"].Balance != 5 || len(m.entries) != 0 || len(ev.done) != 0 {
				t.Fatal("a refused transfer changed state")
			}
		})
	}
	_, _, uc := fixture()
	if _, err := uc.Execute(t.Context(), domain.Transfer{From: "a", To: "b", Amount: 1}); kakarot.CodeOf(err) != kakarot.CodeUnauthorized {
		t.Fatalf("no caller: %v", err)
	}
}

func TestTransferIsAllOrNothing(t *testing.T) {
	m, ev, uc := fixture()
	m.failOn = domain.KindTransferIn // the debit succeeds, the credit fails
	if _, err := uc.Execute(as(t, "alice"), domain.Transfer{From: "a", To: "b", Amount: 30}); err == nil {
		t.Fatal("want failure")
	}
	if m.rows["a"].Balance != 100 || len(m.entries) != 0 || len(ev.done) != 0 {
		t.Fatalf("money left the account without arriving: a=%d entries=%d", m.rows["a"].Balance, len(m.entries))
	}
}

func TestImportStopsAtFirstBadDeposit(t *testing.T) {
	m, _, _ := fixture()
	imp := &usecase.ImportDeposits{Deposit: &usecase.Deposit{Accounts: m, Tx: m.tx()}}
	res, err := imp.Stream(as(t, "alice"), kakarot.SliceStream([]domain.Deposit{
		{AccountID: "a", Amount: 10},
		{AccountID: "a", Amount: 5},
		{AccountID: "a", Amount: -1},
		{AccountID: "a", Amount: 99},
	}))
	if kakarot.CodeOf(err) != kakarot.CodeInvalid || res.Deposits != 2 || res.Total != 15 || m.rows["a"].Balance != 115 {
		t.Fatalf("res=%+v err=%v balance=%d", res, err, m.rows["a"].Balance)
	}
}
