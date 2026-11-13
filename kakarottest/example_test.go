package kakarottest_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kakarottest"
)

// Rename is a usecase: a rule, a port and a transaction.
type Rename struct {
	Names interface {
		Set(ctx context.Context, name string) error
	}
	Tx kakarot.TxManager
}

func (u *Rename) Execute(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", kakarot.Invalid("invalid name", map[string]string{"name": "required"})
	}
	return name, u.Tx.Within(ctx, func(ctx context.Context) error { return u.Names.Set(ctx, name) })
}

type memNames struct{ name string }

func (m *memNames) Set(_ context.Context, name string) error { m.name = name; return nil }

// A usecase test needs an in-memory port and kakarottest.Tx: no database.
func ExampleTx() {
	t := &testing.T{} // in a real test this is the test's own t
	names, tx := &memNames{}, &kakarottest.Tx{}
	uc := &Rename{Names: names, Tx: tx}

	got, err := uc.Execute(context.Background(), "ana")
	fmt.Println(got, err, names.name, tx.Commits())

	_, err = uc.Execute(context.Background(), "")
	kakarottest.WantCode(t, err, kakarot.CodeInvalid)
	kakarottest.WantField(t, err, "name")
	fmt.Println(tx.Commits(), tx.Rollbacks())
	// Output:
	// ana <nil> ana 1
	// 1 0
}
