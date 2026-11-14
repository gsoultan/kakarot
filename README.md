# kakarot

A layered Go framework: **transport → middleware → endpoint → service → usecase → repository**, each layer with a typed contract.

- **Type safe end to end.** Every operation is a `kakarot.Endpoint[Req, Resp]` or a typed stream. Shared middleware (`func(ctx, op, next) error`) never touches the request; middleware that needs it is a generic `TypedMiddleware[Req, Resp]`, checked by the compiler. No `any` and no casts in the public API.
- **One service, many transports.** The same endpoint serves HTTP, gRPC and WebSocket. Errors are domain codes that each transport maps once.
- **Domain types are the DTOs**; **storm models are the database types** and never leave the repository. Repositories use [storm](https://github.com/gsoultan/storm).
- **Generated glue, hand-written logic.** `kakarot gen` writes the endpoint and transport layers from an annotated service interface.
- **Go 1.27.4**: generic methods (`Router.Handle[Req, Resp]`), iterators for streams, `errors.AsType`, `log/slog`, `http.ServeMux` patterns.

## Quick start

```console
$ go run github.com/gsoultan/kakarot/cmd/kakarot new example.com/acme/notes
$ cd notes
$ go get -tool github.com/gsoultan/kakarot/cmd/kakarot github.com/gsoultan/storm/cmd/storm
$ go generate ./...
$ go mod tidy
$ go test ./...            # the new project's tests pass with no database
$ go tool kakarot check    # layer rules, //kakarot: comments, generated code is current
$ STORM_DSN=postgres://localhost/notes AUTO_MIGRATE=1 go run ./cmd/server
$ curl -XPOST localhost:8080/notes -d '{"title":"hi"}'
```

## Modules

The core has no dependencies outside the Go standard library. Anything that pulls in a third-party library is its own module, so you only download what you use.

| Module | Contents | Depends on |
|---|---|---|
| `github.com/gsoultan/kakarot` | core, `middleware`, `auth`, `queue`, `transport/httpx`, `kakarottest` | standard library only |
| `github.com/gsoultan/kakarot/transport/grpcx` | gRPC: interface-first service and clients, `.proto` file and protobuf clients, proto adapters | grpc-go |
| `github.com/gsoultan/kakarot/transport/wsx` | WebSocket streams | coder/websocket |
| `github.com/gsoultan/kakarot/data` | storm `TxManager`, error mapping, `outbox`, `idempotency` | storm |
| `github.com/gsoultan/kakarot/otelx` | OpenTelemetry tracing and metrics | opentelemetry-go |
| `github.com/gsoultan/kakarot/natsx` | NATS JetStream broker for queue consumers and the outbox relay | nats.go |
| `github.com/gsoultan/kakarot/cmd/kakarot` | CLI: `new`, `gen`, `check` | x/tools |
| `examples/orders` | a complete service on every layer | all of the above |
| `examples/checkout` | a synchronous request followed by asynchronous payment, shipping, compensation and expiry | core, `data` |
| `examples/wallet` | a money-transfer service: auth, idempotent transfers, ledger, outbox, gRPC | all of the above |

```text
endpoint.go stream.go errors.go   core: Endpoint, streams, Middleware, Wrap, Decorate, Usecase, Error
app.go job.go page.go             App (graceful run of many servers), Job (scheduled work), keyset Page / cursors
transport/httpx/                  HTTP: Router, Handle, Stream (SSE/NDJSON), Upload, OpenAPI 3.1
middleware/                       Recover, Logging, Timeout, Validated, Retry, CircuitBreaker,
                                  RateLimit, LoadShed, Idempotent
auth/                             typed principal in context, Bearer, Require
queue/                            message consumers on typed topics, in-process broker
kakarottest/                      test kit: Tx, Do, WantCode, Eventually, Serve, Publish, Clock
```

## The layers

```go
// domain: DTOs with validation
type PlaceOrder struct {
    CustomerID string     `json:"customer_id"`
    Items      []LineItem `json:"items"`
}
func (p PlaceOrder) Validate() error { ... }

// usecase: one operation; ports (interfaces) live next to it
type PlaceOrder struct { Orders OrderRepository; Events Events; Tx kakarot.TxManager }
func (u *PlaceOrder) Execute(ctx context.Context, in domain.PlaceOrder) (domain.Order, error) {
    var o domain.Order
    err := u.Tx.Within(ctx, func(ctx context.Context) error { // order + event in one transaction
        ...
    })
    return o, err
}

// service: the contract, annotated for the generator
//go:generate go tool kakarot gen
//kakarot:service order
type OrderService interface {
    //kakarot:http POST /orders
    //kakarot:grpc
    PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error)
    //kakarot:http GET /orders/{id}
    GetOrder(ctx context.Context, in domain.GetOrder) (domain.Order, error)
    //kakarot:http GET /orders
    ListOrders(ctx context.Context, in domain.ListOrders) (kakarot.Page[domain.Order], error)
    //kakarot:http GET /orders/export
    ExportOrders(ctx context.Context, in domain.ExportOrders) iter.Seq2[domain.Order, error]
    //kakarot:queue order.placed
    NotifyOrderPlaced(ctx context.Context, in domain.Order) (domain.Notified, error)
}

// main: wiring is plain Go
r := httpx.NewRouter()
transport.RegisterOrderHTTP(r, endpoint.MakeOrderEndpoints(svc), middleware.Default(log, 5*time.Second))
r.ServeOpenAPI("GET /openapi.json", httpx.Info{Title: "orders", Version: "1.0.0"})
app := &kakarot.App{Name: "orders", Servers: []kakarot.Server{httpx.NewServer(":8080", r), relay}}
app.Run(ctx) // graceful shutdown on SIGINT/SIGTERM
```

## Code generation

`kakarot gen` (run by `go generate` in the service package) reads the comments on your service interface:

| Comment | Generates |
|---|---|
| `//kakarot:service order` on the interface | `endpoint/endpoints_gen.go`: one typed endpoint per method |
| `//kakarot:http GET /orders/{id}` | `transport/http_gen.go`: `RegisterOrderHTTP` |
| `//kakarot:grpc` | `transport/grpc_gen.go`: `OrderGRPC`, `RegisterOrderGRPC` and `NewOrderGRPCClient` |
| `//kakarot:queue order.placed` | `transport/queue_gen.go`: a typed topic and `RegisterOrderQueue` |

The method shape picks the kind:

| Method shape | Kind | HTTP | gRPC |
|---|---|---|---|
| `(ctx, Req) (Resp, error)` | `Endpoint` | `Handle` (JSON) | unary |
| `(ctx, Req) iter.Seq2[Msg, error]` | `ServerStream` | `Stream` (SSE or NDJSON) | server streaming |
| `(ctx, iter.Seq2[Msg, error]) (Resp, error)` | `ClientStream` | `Upload` (NDJSON body) | client streaming |
| `(ctx, iter.Seq2[In, error], func(Out) error) error` | `BidiStream` | `wsx.Handle` (WebSocket) | bidirectional |

Requests with a `Validate() error` method are wrapped with `middleware.Validated`, so validation runs on every transport. A method with no transport comment gets an endpoint but no binding. Dependency wiring is not generated; `main.go` stays explicit Go.

A mistake in a comment is an error with the place and the fix, never a silently missing route:

```text
service/service.go:21: OrderService.GetOrder: unknown directive //kakarot:htp (did you mean //kakarot:http?); known directives are ...
```

## Checking a project

`kakarot check` verifies what the compiler cannot. Run it before a commit and in CI:

```console
$ go tool kakarot check
usecase/place_order.go:6: usecase must not import net/http: business rules reach the outside only through ports (interfaces declared in usecase)
transport/http_gen.go: out of date with the service interface: run go generate ./...
kakarot check: 2 problem(s)
```

It checks three things:

- **Layer rules.** Each layer imports only the layers below it. `domain`, `usecase` and `service` import no transport and no database library.
- **Comments.** Every `//kakarot:` line is a known directive with valid arguments.
- **Generated code.** `endpoint/` and `transport/` match the service interface. Nothing is written.

## Testing

Every layer is testable on its own, and only repository tests need a database. `kakarottest` (standard library only) removes the repetitive parts:

```go
// A usecase: in-memory port, no database.
func TestTransferRollsBackOnFailure(t *testing.T) {
    repo := newMemAccounts()
    tx := &kakarottest.Tx{OnBegin: repo.snapshot, OnRollback: repo.restore}
    uc := &usecase.Transfer{Accounts: repo, Tx: tx}

    _, err := uc.Execute(t.Context(), domain.Transfer{From: "a", To: "missing", Amount: 10})

    kakarottest.WantCode(t, err, kakarot.CodeNotFound)
    if repo.balance("a") != 100 || tx.Rollbacks() != 1 { ... }
}

// The API: router, decoding, validation, usecases and error mapping, still no database.
res := kakarottest.Do[domain.Order](t, router, "POST", "/orders", domain.PlaceOrder{...}, "Idempotency-Key", "cart-1")
if res.Status != http.StatusCreated || res.Body.Total != 2700 { ... }
bad := kakarottest.Do[kakarottest.None](t, router, "POST", "/orders", domain.PlaceOrder{})
if bad.Error.Fields["items"] == "" { ... }

// Asynchronous work: run the consumer for the test, publish, wait for the result.
kakarottest.Serve(t, consumer, relay)
err := kakarottest.Publish(t, broker, transport.ChargePaymentTopic, order.ID, domain.OrderPlaced{OrderID: order.ID})
kakarottest.Eventually(t, 5*time.Second, "order shipped", func() bool { return status(order.ID) == "shipped" })

// Time: a clock the test moves by hand.
clock := kakarottest.NewClock(start)
expire := &usecase.ExpireOrders{After: 15 * time.Minute, Now: clock.Now}
clock.Advance(time.Hour)
```

| Helper | Use |
|---|---|
| `Tx` | a `TxManager` that counts commits and rollbacks, can fail at commit, and calls hooks so a fake repository can roll back |
| `Do[T]` | one HTTP request against any handler; typed body, status, headers and decoded error |
| `WantCode`, `WantField` | assert an error's code or a field problem |
| `Eventually` | wait for an asynchronous result, with a clear failure message |
| `Serve` | run servers (consumer, relay, job) until the test ends |
| `Publish` | send a typed message to `queue.Memory` and get the handler's error back |
| `Clock` | a manual clock for code that takes `func() time.Time` |

Three things in the design make test-first work practical: usecases depend only on interfaces they declare, a service method is callable as a plain function, and a scaffolded project starts with passing usecase and API tests to copy.

## Coding agents

kakarot is built to be changed safely by tools as well as by people:

- **`AGENTS.md`** in this repository and in every scaffolded project states the layers, the rules, the commands and the steps to add an operation.
- **One place per kind of code.** The layer decides the package, so there is one right answer to "where does this go".
- **The compiler and `kakarot check` catch mistakes.** Types catch a wrong request or response; `check` catches a wrong import, a misspelled comment and stale generated code. Each message has `file:line` and the fix.
- **A fast loop with no setup.** `go generate ./... && go test ./... && go tool kakarot check` needs no database and no network.
- **Runnable examples** in the package documentation (`go doc`, `Example` tests), which cannot go out of date.

## HTTP

Requests decode from `path`, `query` and `header` tags plus a JSON body (unknown fields rejected, 4 MB limit). POST answers 201. `httpx.Name`, `httpx.Status`, `httpx.Errors` and `httpx.With` tune a route; `HandleCodec` takes your own typed decoder and encoder.

`Router.OpenAPI(info)` builds an OpenAPI 3.1 document from the registered routes' Go types; `ServeOpenAPI` serves it.

## Streaming and WebSocket

```go
// HTTP server stream: SSE when Accept: text/event-stream, NDJSON otherwise
g.Stream("GET /orders/export", eps.ExportOrders)

// WebSocket: JSON frames both ways
wsx.Handle(r, "GET /chat", eps.Chat, wsx.Options{OriginPatterns: []string{"app.example.com"}})
```

Streams take the same shared middleware as endpoints; it wraps the whole stream.

## gRPC

**Interface-first (generated, no `.proto`).** Messages are your domain types encoded as JSON over standard gRPC framing, so deadlines, metadata, status codes, streaming and interceptors work as usual.

```go
// server
gs := grpc.NewServer()
transport.RegisterOrderGRPC(gs, eps, mws)
servers = append(servers, &grpcx.Server{GRPC: gs, Addr: ":9090"})

// client: the same OrderEndpoints type, each field calling the remote service
client := transport.NewOrderGRPCClient(conn)
order, err := client.GetOrder(ctx, domain.GetOrder{ID: id}) // err is a *kakarot.Error with the server's code
get := kakarot.Wrap(op, client.GetOrder, middleware.Retry(policy)) // client-side middleware
```

**Clients in other languages** get a `.proto` file written from the same service, and the server answers protobuf as well as JSON:

```go
grpcx.EnableProtobuf()                            // once in main
schema, err := transport.OrderGRPC(eps).Proto()   // the contract: commit it as api/order.proto
```

```proto
service OrderService {
  rpc GetOrder(GetOrder) returns (Order);
  rpc ExportOrders(ExportOrders) returns (stream Order);
}
message Order {
  string id = 1;
  repeated LineItem items = 4;
  google.protobuf.Timestamp created_at = 5 [json_name = "created_at"];
}
```

One description of the Go types drives both the file and the wire format, so they cannot disagree. Field numbers follow the order of the struct's fields: add fields at the end, or pin a number with a `proto:"N"` tag. `examples/orders` keeps the file under a test that fails when it drifts. The full type mapping is in [docs/protobuf.md](docs/protobuf.md).

If you already have protoc output, use the **proto adapters** next to it:

```go
s.getOrder = grpcx.Unary(op, eps.GetOrder, fromProto, toProto, mws...)
// grpcx.ServerStream, ClientStream and Bidi adapt the generic grpc stream interfaces.
```

## Queue consumers

A message handler is an ordinary endpoint, so it gets the same middleware, validation and tests as an HTTP route.

```go
broker := &queue.Memory{} // in process; natsx.Broker for NATS, or your own queue.Subscriber
consumer := &queue.Consumer{Service: "order", Source: broker, Logger: log}
transport.RegisterOrderQueue(consumer, eps, mws) // generated from //kakarot:queue
app.Servers = append(app.Servers, consumer)      // runs and stops with the app
```

A handler error asks the broker to redeliver, except errors a retry cannot fix (`invalid`, `not_found`, `unauthorized`, `forbidden`) and payloads that do not decode: those go to `Consumer.DeadLetter`. A broker is one method, `Subscribe(ctx, topic, handler)`.

**NATS JetStream** ships as `natsx`. One `Broker` is both sides: the consumer's source and the outbox relay's publisher.

```go
broker := &natsx.Broker{Conn: nc, Stream: "ORDERS", Subjects: []string{"order.>"}, Group: "billing"}
consumer := &queue.Consumer{Service: "billing", Source: broker}
relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
```

Instances that share a `Group` share the work; a failed handler gets the message again after `Backoff`, up to `MaxDeliver` times; an event's ID deduplicates a relay retry.

## Observability

```go
mws := kakarot.Chain(otelx.Middleware(), middleware.Default(log, 5*time.Second))
r.Use(otelx.HTTP()) // continue the caller's trace (W3C traceparent)
```

One span and one `kakarot.call.duration` histogram point per call, on every transport, named by operation with the result code as an attribute. Only server faults (`internal`, `unavailable`, `timeout`) mark a span as an error. Request and response values are never recorded. Install any OpenTelemetry SDK and exporter in `main`; without one the middleware is a no-op.

A trace continues across every hop, not only HTTP:

```go
trace := grpcx.Carrier{Inject: otelx.Headers, Extract: otelx.WithHeaders}
unary, stream := trace.Server() // trace.Client() gives the dialing side
gs := grpc.NewServer(grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))

msg.Headers = otelx.Headers(ctx)                                   // publisher
consumer := &queue.Consumer{Source: broker, Extract: otelx.WithHeaders} // handler span is a child
```

The outbox does not store headers yet, so a trace stops at an event that goes through the relay.

## Auth

```go
type User struct{ ID string; Admin bool }

r.Use(auth.Bearer(func(ctx context.Context, token string) (User, error) { ... })) // 401 on failure
admin := auth.Require(func(u User) error {
    if !u.Admin { return kakarot.E(kakarot.CodeForbidden, "admins only", nil) }
    return nil
})
u, ok := auth.From[User](ctx) // typed, anywhere below transport

// the same verifier on gRPC, for unary calls and streams
unary, stream := grpcx.Bearer(verify)
gs := grpc.NewServer(grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
ctx = grpcx.WithBearer(ctx, token) // client side
```

## Resilience

```go
mws := []kakarot.Middleware{
    middleware.LoadShed(512),                                              // over 512 in flight -> 503 at once
    middleware.RateLimit(100, 20),                                         // per op token bucket -> 429
    middleware.CircuitBreaker(middleware.BreakerPolicy{Failures: 5, OpenFor: 10 * time.Second}),
    middleware.Retry(middleware.RetryPolicy{Attempts: 3, BaseDelay: 50 * time.Millisecond}),
}
// Idempotency-Key replay, typed by the response
type PlaceOrder struct { Key string `header:"Idempotency-Key"`; ... }
func (p PlaceOrder) IdempotencyKey() string { return p.Key }
ep := kakarot.Decorate(op, eps.PlaceOrder, middleware.Idempotent[PlaceOrder, Order](store))
```

`MemoryIdempotencyStore` is for one process. `data/idempotency.Store[Resp]{DB: db}` keeps responses in PostgreSQL through storm, so every instance replays them; add `&model.IdempotencyRecord{}` to your storm models and call `Purge` on a schedule.

## Scheduled jobs

```go
expire := &kakarot.Job{Name: "expire-orders", Every: time.Minute, Run: expireOrders.Run}
app := &kakarot.App{Servers: []kakarot.Server{httpServer, consumer, relay, expire}}
```

A `Job` starts and stops with the application. Runs never overlap, and a failed run is logged without stopping the job.

## Events: transactional outbox

```go
var OrderPlaced = outbox.Topic[domain.Order]("order.placed")

OrderPlaced.Add(ctx, db, o.ID, o)   // inside TxManager.Within: committed with the order
relay := &outbox.Relay{DB: db, Publisher: kafkaPublisher} // a kakarot.Server; FOR UPDATE SKIP LOCKED
relay = &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)} // or straight into queue consumers
```

Delivery is at least once; consumers should be idempotent.

## Pagination

```go
type ListOrders struct {
    CustomerID string `query:"customer_id"`
    kakarot.PageRequest // ?cursor=&limit=
}
// repository: fetch limit+1 rows after the cursor, then
return kakarot.NextPage(rows, limit, toDomain, func(r order.Row) cursor { return cursor{r.CreatedAt, r.ID} })
```

Cursors are opaque, typed (`DecodeCursor[C]`), and a malformed cursor is a 400.

## Custom errors

```go
// 1. Your own codes, with their HTTP status
var CodeQuotaExceeded = kakarot.NewCode("quota_exceeded")
httpx.RegisterStatus(CodeQuotaExceeded, http.StatusTooManyRequests)
return kakarot.E(CodeQuotaExceeded, "monthly quota used up", nil)
kakarot.Is(err, CodeQuotaExceeded) // true; false for a nil error

// 2. Your own body shape (e.g. RFC 9457 problem+json), app-wide; groups inherit it
r.SetErrorEncoder(func(ctx context.Context, w http.ResponseWriter, err error) {
    if p, ok := errors.AsType[*MyProblem](err); ok { /* your own error type */ }
    e := kakarot.AsError(err) // domain code, message, per-field problems
    ...
})

// 3. One route only
r.Handle("POST /pay", eps.Pay, httpx.Errors(payErrors))
```

Internal errors are logged with their cause and shown to callers only as `internal error`.

Custom codes also survive gRPC. The server sends the code name and the per-field problems as status details, and the client restores them:

```go
grpcx.RegisterCode(CodeQuotaExceeded, codes.ResourceExhausted) // the gRPC status to send
_, err := client.Pay(ctx, req)
kakarot.AsError(err).Code == CodeQuotaExceeded // true on the client
```

## Security

- **Go 1.27.4 or later is required.** 1.27.3 has a known `net/http` vulnerability (GO-2026-6859); every `go.mod` here and in scaffolded projects pins 1.27.4.
- CI runs `govulncheck` on every module on each pull request and weekly.
- Defaults: 4 MB request body and NDJSON line limit, unknown JSON fields rejected, header read timeout, same-origin WebSocket unless `OriginPatterns` is set, internal error causes never sent to callers.
- Idempotency keys are scoped per operation and capped at 255 bytes. When responses are private, use `middleware.IdempotentFor(store, caller)`: it also scopes each key to the caller, so one user can never replay another user's response. `MemoryIdempotencyStore` is bounded by `MaxEntries`.
- Pagination cursors are encoded, not signed: use them as a position inside an already-authorized query, never as proof of access.
- `RateLimit` is per operation, not per client; put per-client limits at your gateway or key your own limiter by principal.

## Testing kakarot itself

Each module is tested on its own:

```console
$ for m in . transport/grpcx transport/wsx data otelx cmd/kakarot; do (cd $m && go test -race ./...); done
$ cd examples/orders && KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/orders go test -race ./...   # live storm tests
$ cd examples/wallet && KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/wallet go test -race ./...   # use a separate database
$ go run golang.org/x/vuln/cmd/govulncheck@latest ./...   # in any module
```

## Documentation

Guides live in [`docs/`](docs/index.md): a step by step [tutorial](docs/tutorial.md), [protobuf clients](docs/protobuf.md), [events with NATS](docs/events.md) and [releasing](docs/releasing.md). Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## Not built yet

- Brokers other than NATS: `queue.Subscriber` is one method, but no Kafka or SQS implementation ships with kakarot.
- Trace headers on outbox events.
- Protobuf `enum` and `oneof`: a named Go string is a protobuf `string`.
- Dependency wiring generation: `main.go` is hand-written by design.
