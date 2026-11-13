// Package endpoint is the endpoint layer: one typed endpoint or stream per
// service method. Every transport binds these same values.
package endpoint

import (
	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
	"github.com/gsoultan/kakarot/examples/wallet/service"
	"github.com/gsoultan/kakarot/middleware"
)

// Wallet holds the wallet's endpoints.
type Wallet struct {
	OpenAccount    kakarot.Endpoint[domain.OpenAccount, domain.Account]
	GetAccount     kakarot.Endpoint[domain.GetAccount, domain.Account]
	Deposit        kakarot.Endpoint[domain.Deposit, domain.Account]
	Transfer       kakarot.Endpoint[domain.Transfer, domain.TransferDone]
	ListEntries    kakarot.Endpoint[domain.ListEntries, kakarot.Page[domain.Entry]]
	ImportDeposits kakarot.ClientStream[domain.Deposit, domain.ImportResult]
	AuditTransfer  kakarot.Endpoint[domain.TransferDone, domain.Audited]
}

// New builds the endpoints from the service. A method value already has the
// endpoint shape. Validated only compiles for requests that have a Validate
// method, and makes validation run on every transport.
func New(svc service.WalletService) Wallet {
	return Wallet{
		OpenAccount:    svc.OpenAccount,
		GetAccount:     svc.GetAccount,
		Deposit:        middleware.Validated(svc.Deposit),
		Transfer:       middleware.Validated(svc.Transfer),
		ListEntries:    svc.ListEntries,
		ImportDeposits: svc.ImportDeposits,
		AuditTransfer:  svc.AuditTransfer,
	}
}
