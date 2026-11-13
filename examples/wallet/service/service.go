// Package service is the wallet's API: what it can do, with no knowledge of
// HTTP, gRPC, queues or SQL.
package service

import (
	"context"
	"iter"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
)

// WalletService is the wallet's contract. Each method has the endpoint
// shape, so the endpoint and transport layers bind it directly.
type WalletService interface {
	OpenAccount(ctx context.Context, in domain.OpenAccount) (domain.Account, error)
	GetAccount(ctx context.Context, in domain.GetAccount) (domain.Account, error)
	Deposit(ctx context.Context, in domain.Deposit) (domain.Account, error)
	Transfer(ctx context.Context, in domain.Transfer) (domain.TransferDone, error)
	ListEntries(ctx context.Context, in domain.ListEntries) (kakarot.Page[domain.Entry], error)
	// ImportDeposits applies a stream of deposits.
	ImportDeposits(ctx context.Context, in iter.Seq2[domain.Deposit, error]) (domain.ImportResult, error)
	// AuditTransfer handles the transfer.completed event.
	AuditTransfer(ctx context.Context, in domain.TransferDone) (domain.Audited, error)
}

// Usecases groups what the service composes.
type Usecases struct {
	Open    kakarot.Usecase[domain.OpenAccount, domain.Account]
	Get     kakarot.Usecase[domain.GetAccount, domain.Account]
	Deposit kakarot.Usecase[domain.Deposit, domain.Account]
	Move    kakarot.Usecase[domain.Transfer, domain.TransferDone]
	Entries kakarot.Usecase[domain.ListEntries, kakarot.Page[domain.Entry]]
	Import  kakarot.ClientStream[domain.Deposit, domain.ImportResult]
	Audit   kakarot.Usecase[domain.TransferDone, domain.Audited]
}

type walletService struct{ uc Usecases }

// New returns the wallet service.
func New(uc Usecases) WalletService { return &walletService{uc: uc} }

func (s *walletService) OpenAccount(ctx context.Context, in domain.OpenAccount) (domain.Account, error) {
	return s.uc.Open.Execute(ctx, in)
}

func (s *walletService) GetAccount(ctx context.Context, in domain.GetAccount) (domain.Account, error) {
	return s.uc.Get.Execute(ctx, in)
}

func (s *walletService) Deposit(ctx context.Context, in domain.Deposit) (domain.Account, error) {
	return s.uc.Deposit.Execute(ctx, in)
}

func (s *walletService) Transfer(ctx context.Context, in domain.Transfer) (domain.TransferDone, error) {
	return s.uc.Move.Execute(ctx, in)
}

func (s *walletService) ListEntries(ctx context.Context, in domain.ListEntries) (kakarot.Page[domain.Entry], error) {
	return s.uc.Entries.Execute(ctx, in)
}

func (s *walletService) ImportDeposits(ctx context.Context, in iter.Seq2[domain.Deposit, error]) (domain.ImportResult, error) {
	return s.uc.Import(ctx, in)
}

func (s *walletService) AuditTransfer(ctx context.Context, in domain.TransferDone) (domain.Audited, error) {
	return s.uc.Audit.Execute(ctx, in)
}
