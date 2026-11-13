package transport

import (
	"google.golang.org/grpc"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
	"github.com/gsoultan/kakarot/examples/wallet/endpoint"
	"github.com/gsoultan/kakarot/transport/grpcx"
)

// GRPCService is the wallet's gRPC service name.
const GRPCService = "wallet.WalletService"

// GRPC serves the wallet on r, with the domain types as JSON messages (no
// .proto needed).
func GRPC(r grpc.ServiceRegistrar, eps endpoint.Wallet, mws ...kakarot.Middleware) {
	svc := grpcx.NewService(GRPCService, mws...)
	grpcx.AddUnary(svc, "OpenAccount", eps.OpenAccount)
	grpcx.AddUnary(svc, "GetAccount", eps.GetAccount)
	grpcx.AddUnary(svc, "Deposit", eps.Deposit)
	grpcx.AddUnary(svc, "Transfer", eps.Transfer)
	grpcx.AddUnary(svc, "ListEntries", eps.ListEntries)
	grpcx.AddClientStream(svc, "ImportDeposits", eps.ImportDeposits)
	svc.Register(r)
}

// GRPCClient returns the same endpoints as the server, each one calling the
// remote wallet over cc. Callers cannot tell local from remote, and wrap
// them with kakarot.Wrap for client-side Retry or CircuitBreaker.
func GRPCClient(cc grpc.ClientConnInterface) endpoint.Wallet {
	return endpoint.Wallet{
		OpenAccount:    grpcx.Invoke[domain.OpenAccount, domain.Account](cc, GRPCService, "OpenAccount"),
		GetAccount:     grpcx.Invoke[domain.GetAccount, domain.Account](cc, GRPCService, "GetAccount"),
		Deposit:        grpcx.Invoke[domain.Deposit, domain.Account](cc, GRPCService, "Deposit"),
		Transfer:       grpcx.Invoke[domain.Transfer, domain.TransferDone](cc, GRPCService, "Transfer"),
		ListEntries:    grpcx.Invoke[domain.ListEntries, kakarot.Page[domain.Entry]](cc, GRPCService, "ListEntries"),
		ImportDeposits: grpcx.Send[domain.Deposit, domain.ImportResult](cc, GRPCService, "ImportDeposits"),
	}
}
