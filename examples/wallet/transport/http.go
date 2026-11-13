// Package transport is the transport layer: it binds the endpoints to HTTP,
// gRPC and the message queue. Each binding is one line of ordinary Go.
package transport

import (
	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/wallet/endpoint"
	"github.com/gsoultan/kakarot/transport/httpx"
)

// HTTP mounts the wallet's routes. mws are endpoint middleware shared by
// every route.
func HTTP(r *httpx.Router, eps endpoint.Wallet, mws ...kakarot.Middleware) {
	g := r.Group("wallet", mws...)
	g.Handle("POST /accounts", eps.OpenAccount, httpx.Name("OpenAccount"))
	g.Handle("GET /accounts/{id}", eps.GetAccount, httpx.Name("GetAccount"))
	g.Handle("POST /accounts/{id}/deposits", eps.Deposit, httpx.Name("Deposit"))
	g.Handle("POST /transfers", eps.Transfer, httpx.Name("Transfer"))
	g.Handle("GET /accounts/{id}/entries", eps.ListEntries, httpx.Name("ListEntries"))
	g.Upload("POST /deposits/import", eps.ImportDeposits, httpx.Name("ImportDeposits")) // one deposit per NDJSON line
}
