# kakarot

A layered Go framework: **transport → middleware → endpoint → service → usecase → repository**, each layer with a typed contract.

- **Type safe end to end.** Every operation is a `kakarot.Endpoint[Req, Resp]` or a typed stream. Shared middleware (`func(ctx, op, next) error`) never touches the request; middleware that needs it is a generic `TypedMiddleware[Req, Resp]`, checked by the compiler. No `any` and no casts in the public API.
- **One service, many transports.** The same endpoint serves HTTP, gRPC and WebSocket. Errors are domain codes that each transport maps once.
- **Domain types are the DTOs**; **storm models are the database types** and never leave the repository. Repositories use [storm](https://github.com/gsoultan/storm).
- **Generated glue, hand-written logic.** `kakarot gen` writes the endpoint and transport layers from an annotated service interface.
- **Go 1.27.3**: generic methods (`Router.Handle[Req, Resp]`), iterators for streams, `errors.AsType`, `log/slog`, `http.ServeMux` patterns.

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

## Layout

```text
endpoint.go stream.go errors.go   core: Endpoint, streams, Middleware, Wrap, Decorate, Usecase, Error
app.go page.go                    App (graceful run of many servers), keyset Page / cursors
transport/httpx/                  HTTP: Router, Handle, Stream (SSE/NDJSON), Upload, OpenAPI 3.1
transport/grpcx/                  gRPC: Unary, ServerStream, ClientStream, Bidi, status mapping
transport/wsx/                    WebSocket bidirectional streams
middleware/                       Recover, Logging, Timeout, Validated, Retry, CircuitBreaker,
                                  RateLimit, Idempotent
auth/                             typed principal in context, Bearer, Require
data/                             storm TxManager, Executor, error mapping
data/outbox/                      transactional outbox with a typed Topic[T] and a relay
cmd/kakarot/                      CLI: new, gen
examples/orders/                  a complete service on every layer (its own module)
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
    PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error)
    //kakarot:http GET /orders/{id}
    GetOrder(ctx context.Context, in domain.GetOrder) (domain.Order, error)
    //kakarot:http GET /orders
    ListOrders(ctx context.Context, in domain.ListOrders) (kakarot.Page[domain.Order], error)
    //kakarot:http GET /orders/export
    ExportOrders(ctx context.Context, in domain.ExportOrders) iter.Seq2[domain.Order, error]
}

// main: wiring is plain Go
r := httpx.NewRouter()
transport.RegisterOrderHTTP(r, endpoint.MakeOrderEndpoints(svc), middleware.Default(log, 5*time.Second))
r.ServeOpenAPI("GET /openapi.json", httpx.Info{Title: "orders", Version: "1.0.0"})
app := &kakarot.App{Name: "orders", Servers: []kakarot.Server{httpx.NewServer(":8080", r), relay}}
app.Run(ctx) // graceful shutdown on SIGINT/SIGTERM
```

## Code generation

`kakarot gen` (run by `go generate` in the service package) writes `endpoint/endpoints_gen.go` and `transport/http_gen.go`. The method shape picks the kind:

| Method shape | Kind | HTTP binding |
|---|---|---|
| `(ctx, Req) (Resp, error)` | `Endpoint` | `Handle` (JSON) |
| `(ctx, Req) iter.Seq2[Msg, error]` | `ServerStream` | `Stream` (SSE or NDJSON) |
| `(ctx, iter.Seq2[Msg, error]) (Resp, error)` | `ClientStream` | `Upload` (NDJSON body) |
| `(ctx, iter.Seq2[In, error], func(Out) error) error` | `BidiStream` | `wsx.Handle` (WebSocket) |

Requests with a `Validate() error` method are wrapped with `middleware.Validated`. Methods without `//kakarot:http` get an endpoint but no route. Dependency wiring is not generated; `main.go` stays explicit Go.

## HTTP

Requests decode from `path`, `query` and `header` tags plus a JSON body (unknown fields rejected, 4 MB limit). POST answers 201. `httpx.Name`, `httpx.Status`, `httpx.Errors` and `httpx.With` tune a route; `HandleCodec` takes your own typed decoder and encoder.

`Router.OpenAPI(info)` builds an OpenAPI 3.1 document from the registered routes' Go types; `ServeOpenAPI` serves it.

## Streaming, gRPC, WebSocket

```go
// HTTP server stream: SSE when Accept: text/event-stream, NDJSON otherwise
g.Stream("GET /orders/export", eps.ExportOrders)

// gRPC: the same endpoint behind a generated proto server. in/out convert proto <-> domain.
func (s *server) GetOrder(ctx context.Context, r *pb.GetOrderRequest) (*pb.Order, error) {
    return s.getOrder(ctx, r)
}
s.getOrder = grpcx.Unary(op, eps.GetOrder, fromProto, toProto, middleware.Default(log, time.Second))
// grpcx.ServerStream, ClientStream and Bidi adapt the generic grpc stream interfaces.

// WebSocket: JSON frames both ways
wsx.Handle(r, "GET /chat", eps.Chat, wsx.Options{OriginPatterns: []string{"app.example.com"}})
```

Streams take the same shared middleware as endpoints; it wraps the whole stream.

## Auth

```go
type User struct{ ID string; Admin bool }

r.Use(auth.Bearer(func(ctx context.Context, token string) (User, error) { ... })) // 401 on failure
admin := auth.Require(func(u User) error {
    if !u.Admin { return kakarot.E(kakarot.CodeForbidden, "admins only", nil) }
    return nil
})
u, ok := auth.From[User](ctx) // typed, anywhere below transport
```

## Resilience

```go
mws := []kakarot.Middleware{
    middleware.RateLimit(100, 20),                                         // per op token bucket -> 429
    middleware.CircuitBreaker(middleware.BreakerPolicy{Failures: 5, OpenFor: 10 * time.Second}),
    middleware.Retry(middleware.RetryPolicy{Attempts: 3, BaseDelay: 50 * time.Millisecond}),
}
// Idempotency-Key replay, typed by the response
type PlaceOrder struct { Key string `header:"Idempotency-Key"`; ... }
func (p PlaceOrder) IdempotencyKey() string { return p.Key }
ep := kakarot.Decorate(op, eps.PlaceOrder, middleware.Idempotent[PlaceOrder, Order](store))
```

`MemoryIdempotencyStore` is for one process; implement `IdempotencyStore[Resp]` on your database or Redis for more.

## Events: transactional outbox

```go
var OrderPlaced = outbox.Topic[domain.Order]("order.placed")

OrderPlaced.Add(ctx, db, o.ID, o)   // inside TxManager.Within: committed with the order
relay := &outbox.Relay{DB: db, Publisher: kafkaPublisher} // a kakarot.Server; FOR UPDATE SKIP LOCKED
order, err := OrderPlaced.Decode(msg) // consumer side, typed
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

## Tests

```console
$ go test -race ./...
$ cd examples/orders && KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/orders go test -race ./...   # live storm tests
```

## Not built yet

- OpenTelemetry tracing and metrics middleware
- gRPC code generation (`grpcx` adapters are written by hand next to the protoc output)
- Message queue consumers (the outbox publishes; consuming is up to your broker client)
- Dependency wiring generation
