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
$ STORM_DSN=postgres://localhost/notes AUTO_MIGRATE=1 go run ./cmd/server
$ curl -XPOST localhost:8080/notes -d '{"title":"hi"}'
```

## Modules

The core has no dependencies outside the Go standard library. Anything that pulls in a third-party library is its own module, so you only download what you use.

| Module | Contents | Depends on |
|---|---|---|
| `github.com/gsoultan/kakarot` | core, `middleware`, `auth`, `queue`, `transport/httpx` | standard library only |
| `github.com/gsoultan/kakarot/transport/grpcx` | gRPC: interface-first service and clients, proto adapters | grpc-go |
| `github.com/gsoultan/kakarot/transport/wsx` | WebSocket streams | coder/websocket |
| `github.com/gsoultan/kakarot/data` | storm `TxManager`, error mapping, `outbox`, `idempotency` | storm |
| `github.com/gsoultan/kakarot/otelx` | OpenTelemetry tracing and metrics | opentelemetry-go |
| `github.com/gsoultan/kakarot/cmd/kakarot` | CLI: `new`, `gen` | x/tools |
| `examples/orders` | a complete service on every layer | all of the above |
| `examples/wallet` | a money-transfer service: auth, idempotent transfers, ledger, outbox, gRPC | all of the above |

```text
endpoint.go stream.go errors.go   core: Endpoint, streams, Middleware, Wrap, Decorate, Usecase, Error
app.go page.go                    App (graceful run of many servers), keyset Page / cursors
transport/httpx/                  HTTP: Router, Handle, Stream (SSE/NDJSON), Upload, OpenAPI 3.1
middleware/                       Recover, Logging, Timeout, Validated, Retry, CircuitBreaker,
                                  RateLimit, LoadShed, Idempotent
auth/                             typed principal in context, Bearer, Require
queue/                            message consumers on typed topics, in-process broker
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
| `//kakarot:grpc` | `transport/grpc_gen.go`: `RegisterOrderGRPC` and `NewOrderGRPCClient` |
| `//kakarot:queue order.placed` | `transport/queue_gen.go`: a typed topic and `RegisterOrderQueue` |

The method shape picks the kind:

| Method shape | Kind | HTTP | gRPC |
|---|---|---|---|
| `(ctx, Req) (Resp, error)` | `Endpoint` | `Handle` (JSON) | unary |
| `(ctx, Req) iter.Seq2[Msg, error]` | `ServerStream` | `Stream` (SSE or NDJSON) | server streaming |
| `(ctx, iter.Seq2[Msg, error]) (Resp, error)` | `ClientStream` | `Upload` (NDJSON body) | client streaming |
| `(ctx, iter.Seq2[In, error], func(Out) error) error` | `BidiStream` | `wsx.Handle` (WebSocket) | bidirectional |

Requests with a `Validate() error` method are wrapped with `middleware.Validated`, so validation runs on every transport. A method with no transport comment gets an endpoint but no binding. Dependency wiring is not generated; `main.go` stays explicit Go.

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

Callers in other languages need a gRPC client with a JSON codec (content-subtype `json`). When they need protobuf instead, use the **proto adapters** next to your protoc output:

```go
s.getOrder = grpcx.Unary(op, eps.GetOrder, fromProto, toProto, mws...)
// grpcx.ServerStream, ClientStream and Bidi adapt the generic grpc stream interfaces.
```

## Queue consumers

A message handler is an ordinary endpoint, so it gets the same middleware, validation and tests as an HTTP route.

```go
broker := &queue.Memory{} // or your Kafka / NATS / SQS client behind queue.Subscriber
consumer := &queue.Consumer{Service: "order", Source: broker, Logger: log}
transport.RegisterOrderQueue(consumer, eps, mws) // generated from //kakarot:queue
app.Servers = append(app.Servers, consumer)      // runs and stops with the app
```

A handler error asks the broker to redeliver, except errors a retry cannot fix (`invalid`, `not_found`, `unauthorized`, `forbidden`) and payloads that do not decode: those go to `Consumer.DeadLetter`. A broker is one method, `Subscribe(ctx, topic, handler)`.

## Observability

```go
mws := kakarot.Chain(otelx.Middleware(), middleware.Default(log, 5*time.Second))
r.Use(otelx.HTTP()) // continue the caller's trace (W3C traceparent)
```

One span and one `kakarot.call.duration` histogram point per call, on every transport, named by operation with the result code as an attribute. Only server faults (`internal`, `unavailable`, `timeout`) mark a span as an error. Request and response values are never recorded. Install any OpenTelemetry SDK and exporter in `main`; without one the middleware is a no-op.

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

## Tests

Each module is tested on its own:

```console
$ for m in . transport/grpcx transport/wsx data otelx cmd/kakarot; do (cd $m && go test -race ./...); done
$ cd examples/orders && KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/orders go test -race ./...   # live storm tests
$ cd examples/wallet && KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/wallet go test -race ./...   # use a separate database
$ go run golang.org/x/vuln/cmd/govulncheck@latest ./...   # in any module
```

## Not built yet

- Broker adapters: `queue.Subscriber` is one method, but no Kafka, NATS or SQS implementation ships with kakarot.
- A `.proto` file generated from the service interface, for protobuf clients in other languages.
- gRPC trace propagation (use the `otelgrpc` stats handler on the gRPC server).
- Dependency wiring generation: `main.go` is hand-written by design.
