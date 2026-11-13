# checkout

An order checkout built on kakarot. It is the sample for a use case that is part synchronous and part asynchronous: the customer gets an answer at once, and the slow and unreliable work (payment, shipping, notifications) happens afterwards, driven by events.

## The scenario

```text
SYNCHRONOUS (HTTP, the customer waits)             ASYNCHRONOUS (queue, nobody waits)

POST /orders ──┐
               │ one transaction:
               │   reserve stock
               │   store order (pending)
               │   outbox: order.placed ─────────►  ChargePayment
201 pending ◄──┘                                      │ call the payment provider (no transaction held)
                                                      │
                                    approved ─────────┤ order -> paid, outbox: order.paid ──► ShipOrder
                                    declined ─────────┤ order -> cancelled, stock returned       │ book the shipment
                                    provider down ────┘ fail the message: delivered again        │ order -> shipped
                                                                                                 ▼
GET /orders/{id}   read the status and timeline              every change: order.updated ──► NotifyCustomer

POST /orders/{id}/cancel ── may run while ChargePayment is in the provider call.
                            The cancel wins; when the charge arrives:
                            outbox: order.refund_due ──────────────────────────────────────► RefundPayment

every few seconds (scheduled job) ── ExpireOrders cancels orders still pending after the payment window
```

## What it shows

| Problem | How the sample solves it |
|---|---|
| The request must be fast, but payment is slow | `PlaceOrder` only reserves stock and stores the order. It never calls the provider. |
| The order and its event must not disagree | Both are written in one transaction (the outbox). The relay publishes the event afterwards. |
| A message can be delivered twice | Every handler checks the order's status first and skips a repeat. The provider is called with the order id as its idempotency key. |
| A request and a handler touch the same order at once | Every status change locks the order row and checks the status again. |
| A slow provider call would hold a database transaction | Handlers call the provider first, with no transaction, then record the result in a short one. |
| A later step fails after an earlier one committed | Compensation: a declined payment returns the stock; a charge that lands on a cancelled order is refunded. |
| The provider is down | A short blip is retried inside the handler (`middleware.Retry`). A longer outage fails the message and the relay delivers it again. |
| The payment event never succeeds | `ExpireOrders`, a scheduled job, cancels the order after the payment window and returns the stock. |
| Two customers want the last item | Products are locked in SKU order, and the database has `CHECK (stock >= 0)`. |
| The customer retries the request | `Idempotency-Key` returns the first order instead of placing a second one. |

## Layout

```text
domain/      requests, responses, events, the order's statuses, custom error codes
usecase/     sync.go (PlaceOrder, CancelOrder, ...), async.go (ChargePayment, ShipOrder,
             RefundPayment, NotifyCustomer, ExpireOrders), ports.go (what they need)
service/     CheckoutService: five HTTP methods and four queue handlers, declared with kakarot comments
endpoint/    generated: one typed endpoint per method
transport/   generated: HTTP routes and queue bindings
data/        storm models, generated store, repositories, outbox events
app/         wiring; the scheduled job; demo payment provider, warehouse and notifier
cmd/server/  main
```

The synchronous and asynchronous sides are the same kind of code. A queue handler is a service method like any other, so it gets the same middleware, logging and tests:

```go
//kakarot:http POST /orders
PlaceOrder(ctx context.Context, in domain.PlaceOrder) (domain.Order, error)

//kakarot:queue order.placed
ChargePayment(ctx context.Context, in domain.OrderPlaced) (domain.Handled, error)
```

## Run it

```console
$ createdb checkout
$ STORM_DSN=postgres://user:pass@localhost:5432/checkout AUTO_MIGRATE=1 PAYMENT_WINDOW=20s go run ./cmd/server
```

The payment provider is a stand-in, and the payment token chooses what it does: `tok_declined` is declined, `tok_flaky` is down for the first four calls, `tok_slow` takes five seconds, and anything else is approved.

```console
$ curl -s -X POST localhost:8080/products -d '{"sku":"tea","name":"Green tea","price":500,"stock":10}'

# Happy path: the answer is "pending"; a moment later the order is shipped.
$ ID=$(curl -s -X POST localhost:8080/orders -H 'Idempotency-Key: cart-1' \
    -d '{"customer":"ana","payment_token":"tok_ok","items":[{"sku":"tea","qty":2}]}' | jq -r .id)
$ curl -s localhost:8080/orders/$ID | jq '.status, .steps'

# Declined: accepted first, cancelled in the background, stock returned.
$ curl -s -X POST localhost:8080/orders -d '{"customer":"ana","payment_token":"tok_declined","items":[{"sku":"tea","qty":3}]}'

# Cancel during a slow charge: cancelled now, refunded when the charge arrives.
$ ID=$(curl -s -X POST localhost:8080/orders -d '{"customer":"ana","payment_token":"tok_slow","items":[{"sku":"tea","qty":1}]}' | jq -r .id)
$ curl -s -X POST localhost:8080/orders/$ID/cancel | jq .status
$ sleep 6; curl -s localhost:8080/orders/$ID | jq '.refunded, .steps'

# Not enough stock: refused at once with 409 and the code out_of_stock.
$ curl -s -X POST localhost:8080/orders -d '{"customer":"ana","payment_token":"tok_ok","items":[{"sku":"tea","qty":99}]}'
```

## Test it

```console
$ go test -race ./usecase/...                                                         # no database needed
$ KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/checkout go test -race ./...   # full stack
```

The usecase tests run every rule with in-memory ports, including a cancel that arrives in the middle of a charge. The full-stack tests run the HTTP router, outbox relay, queue consumer and expiry job against PostgreSQL: paid and shipped in the background, declined, out of stock, a provider outage that needs redelivery, cancel during charge with refund, expiry, a retried request, and twenty concurrent orders for five items. Use a database of its own: the tests migrate it to the checkout's tables.

## Limits of the sample

- There is no authentication, to keep the focus on the flow. See `examples/wallet` for bearer auth and per-caller idempotency.
- `queue.Memory` runs the handlers inside the relay's process, one event at a time, so a slow charge delays the events behind it. With a real broker the relay only publishes, and consumers scale on their own.
- An event that keeps failing stops being retried after the relay's `MaxAttempts` (10 by default) and stays in the `outbox_events` table with its last error. The expiry job then cancels the order.
- A paid order cannot be cancelled here. That needs a return flow with the warehouse.
