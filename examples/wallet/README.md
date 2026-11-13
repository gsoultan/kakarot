# wallet

A small money-transfer service built on kakarot. It shows every layer, with all bindings written as plain Go: no comment directives and no generated transport code.

What it does:

- Accounts belong to the signed-in user. Another user's account answers 404, so its existence is not revealed.
- A transfer debits one account, credits the other, writes two ledger entries and records a `transfer.completed` event, all in one database transaction. Rows are locked in id order, so opposite transfers cannot deadlock.
- A transfer is idempotent per caller: send the same `Idempotency-Key` again and you get the first response back, and the money moves once.
- Overdrawing answers 422 with the custom code `insufficient_funds`. A `CHECK (balance >= 0)` in the database backs the rule.
- The outbox relay publishes the event to a queue, and a consumer writes the audit log.
- The same endpoints are served over HTTP and gRPC, both behind bearer-token auth.

## Layout

```text
domain/      request, response and event types (the DTOs), custom error code
usecase/     business rules behind ports (Accounts, Events, Audit); unit-tested with fakes
service/     WalletService: the contract, one method per operation
endpoint/    one typed endpoint per service method, validation attached
transport/   http.go, grpc.go, queue.go: one line of Go per binding
data/        storm models, generated store, the repository
app/         hand-written wiring of all of the above
cmd/server/  main: reads the environment and runs the servers
```

A binding is one line, checked by the compiler:

```go
g.Handle("POST /transfers", eps.Transfer, httpx.Name("Transfer"))      // transport/http.go
grpcx.AddUnary(svc, "Transfer", eps.Transfer)                          // transport/grpc.go
queue.Handle(c, TransferCompleted, "AuditTransfer", eps.AuditTransfer) // transport/queue.go
```

## Run it

```console
$ createdb wallet
$ STORM_DSN=postgres://user:pass@localhost:5432/wallet \
  AUTO_MIGRATE=1 \
  WALLET_TOKENS=alice-secret=alice,bob-secret=bob \
  go run ./cmd/server
```

`WALLET_TOKENS` maps bearer tokens to users and stands in for a real identity provider. The server refuses to start without it. HTTP listens on `:8080` and gRPC on `:9090`.

```console
$ A=$(curl -s -X POST localhost:8080/accounts -H 'Authorization: Bearer alice-secret' | jq -r .id)
$ B=$(curl -s -X POST localhost:8080/accounts -H 'Authorization: Bearer bob-secret' | jq -r .id)
$ curl -s -X POST localhost:8080/accounts/$A/deposits -H 'Authorization: Bearer alice-secret' -d '{"amount":10000}'
$ curl -s -X POST localhost:8080/transfers -H 'Authorization: Bearer alice-secret' \
    -H 'Idempotency-Key: rent-november' -d "{\"from\":\"$A\",\"to\":\"$B\",\"amount\":2500}"
$ curl -s "localhost:8080/accounts/$A/entries?limit=10" -H 'Authorization: Bearer alice-secret'
$ curl -s localhost:8080/openapi.json
```

Amounts are whole minor units (cents), stored as integers.

## Test it

```console
$ go test -race ./usecase/...                                                        # no database needed
$ KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/wallet go test -race ./...    # full stack
```

The full-stack tests cover HTTP and gRPC, authorization, idempotent replay, the custom error code on both transports, the NDJSON import, the audit event, and 40 concurrent opposite transfers that must conserve the total. Use a database of its own: the tests migrate it to the wallet's tables.

## Regenerate the store

After changing `data/model`, run `go generate ./data/...`. It calls storm, which is pinned as a tool in `go.mod`.
