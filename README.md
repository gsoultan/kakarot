# kakarot

A layered Go framework: **transport → middleware → endpoint → service → usecase → repository**, each layer with a typed contract.

- **Type safe end to end.** Every operation is a `kakarot.Endpoint[Req, Resp]`. Shared middleware (`func(ctx, op, next) error`) never sees an untyped request; middleware that needs the request is a generic `TypedMiddleware[Req, Resp]` such as `middleware.Validated`, checked by the compiler. No `any` and no casts in the public API.
- **Standard library HTTP.** `httpx` binds endpoints to Go's `http.ServeMux` patterns (`GET /orders/{id}`); any `func(http.Handler) http.Handler` middleware works.
- **Domain types are the DTOs.** They flow from transport down to the repository interface.
- **Repositories use [storm](https://github.com/gsoultan/storm).** storm models (one plain struct per table) are the database types and never leave the repository. `kakarot/data` carries storm transactions in `context.Context`, so usecases open transactions without importing storm.
- **Go 1.27.**

Design notes and the comparison with go-kit, gin, echo, fiber, chi, kratos and go-zero: see the design doc linked from the first PR.

## Layout

```text
endpoint.go, errors.go, app.go   core: Endpoint, Middleware, Wrap, Usecase, TxManager, Error, App
transport/httpx/                 HTTP transport: Router, Handle, tag-based decoding, error mapping
middleware/                      Recover, Logging (slog), Timeout, Default; typed Validated
data/                            storm integration: TxManager, Executor, MapErr
examples/orders/                 a complete service built on every layer
```

## Example

```go
// domain: the DTOs
type GetOrder struct{ ID string `path:"id"` }

// usecase: one operation, ports declared next to it
type GetOrderUC struct{ Orders OrderRepository }
func (u *GetOrderUC) Execute(ctx context.Context, in domain.GetOrder) (domain.Order, error) { ... }

// service: a plain interface; method values are endpoints
eps := endpoint.Make(service.New(place, get))

// transport
r := httpx.NewRouter()
g := r.Group("order", middleware.Default(log, 5*time.Second))
httpx.Handle(g, "GET /orders/{id}", eps.GetOrder, httpx.Name("GetOrder"))

app := &kakarot.App{Name: "orders", Servers: []kakarot.Server{httpx.NewServer(":8080", r)}}
app.Run(ctx) // graceful shutdown on SIGINT/SIGTERM
```

Run the example against PostgreSQL:

```console
$ export STORM_DSN=postgres://user:pass@localhost:5432/orders
$ AUTO_MIGRATE=1 go run ./examples/orders/cmd/server
$ curl -XPOST localhost:8080/orders -d '{"customer_id":"c-1","items":[{"sku":"pen","quantity":2}]}'
```

After changing a storm model, regenerate the store:

```console
$ go install github.com/gsoultan/storm/cmd/storm@v1.12.1
$ storm generate examples/orders/data/store
```

## Tests

```console
$ go test -race ./...
$ KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/orders go test ./examples/orders/data/   # live storm tests
```

## Roadmap

1. Core + HTTP (this milestone)
2. `kakarot new` / `kakarot gen`: endpoints, routes, DI wiring and OpenAPI generated from service interfaces
3. gRPC transport, OpenTelemetry middleware
4. Circuit breaker, retry, rate limit, load shedding; queue consumers
