# kakarot

A layered Go framework: **transport → middleware → endpoint → service → usecase → repository**, each layer with a typed contract. You write the domain types, the usecases and one annotated service interface; `kakarot gen` writes the endpoint and transport layers.

## Guides

| Guide | What it covers |
|---|---|
| [Tutorial](tutorial.md) | A new service from nothing: scaffold, add an operation test-first, run it |
| [Protobuf clients](protobuf.md) | The `.proto` file written from a service, the type mapping, keeping field numbers stable |
| [Events](events.md) | Outbox, queue consumers, NATS JetStream, tracing across services |
| [Releasing](releasing.md) | Modules, versions and tags |

The [README](https://github.com/gsoultan/kakarot#readme) is the reference for every package. Three complete services live in `examples/`: `orders` (every layer and transport), `wallet` (auth, idempotent money transfers) and `checkout` (a synchronous request followed by asynchronous payment, shipping and compensation).

## The idea in one screen

```go
//kakarot:service order
type OrderService interface {
    //kakarot:http POST /orders
    //kakarot:grpc
    PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error)

    //kakarot:queue order.placed
    NotifyOrderPlaced(ctx context.Context, in domain.Order) (domain.Notified, error)
}
```

From this interface you get typed endpoints, HTTP routes with OpenAPI, a gRPC service with a Go client and a `.proto` file, and queue handlers. Every one of them runs the same middleware and returns the same error codes.
