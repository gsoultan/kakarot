# Events

Work that happens after the response (charging a card, sending mail, updating another service) runs as message handlers. A handler is an ordinary endpoint, so it has the same middleware, validation and tests as an HTTP route.

## The pieces

```text
usecase ── Tx ──> order row + outbox row        one database transaction
                         │
                   outbox.Relay                 publishes committed events, at least once
                         │
                      broker                    queue.Memory in one process, natsx.Broker across services
                         │
                  queue.Consumer ──> endpoint   //kakarot:queue order.placed
```

**Write the event with the data.** Inside `TxManager.Within`, add the event to the outbox. It commits or rolls back with the rows it describes, so no event is lost and none is sent for a change that did not happen.

```go
var OrderPlaced = outbox.Topic[domain.Order]("order.placed")

err := u.Tx.Within(ctx, func(ctx context.Context) error {
    if err := u.Orders.Create(ctx, &o); err != nil {
        return err
    }
    return u.Events.OrderPlaced(ctx, o) // the repository calls OrderPlaced.Add(ctx, db, o.ID, o)
})
```

**Handle it.** Mark a service method with the topic and run `go generate`:

```go
//kakarot:queue order.placed
ChargePayment(ctx context.Context, in domain.Order) (domain.Charged, error)
```

## One process

```go
broker := &queue.Memory{}
relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
consumer := &queue.Consumer{Service: "order", Source: broker}
transport.RegisterOrderQueue(consumer, eps, mws)

app := &kakarot.App{Servers: []kakarot.Server{httpServer, relay, consumer}}
```

## Across services, with NATS JetStream

```go
nc, err := nats.Connect(url)

// the service that owns the orders: relay publishes to the stream
pub := &natsx.Broker{Conn: nc, Stream: "ORDERS", Subjects: []string{"order.>"}}
relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(pub)}

// another service: consume it
sub := &natsx.Broker{Conn: nc, Stream: "ORDERS", Group: "billing"}
consumer := &queue.Consumer{Service: "billing", Source: sub}
```

| Setting | Meaning | Default |
|---|---|---|
| `Stream` | the JetStream stream | required |
| `Subjects` | create the stream, or update it, to capture these subjects; leave empty when the stream is managed elsewhere | none |
| `Group` | name of the durable consumers. Instances with the same group share the work; different groups each get every message | required to subscribe |
| `AckWait` | how long a handler may run before the message is delivered again | 30s |
| `MaxDeliver` | deliveries of one message before JetStream gives up | 10 |
| `Backoff` | pause before a failed message is delivered again | 1s |

A topic is a NATS subject. An event's ID is the JetStream message ID, so a relay that publishes the same event twice delivers it once.

## Failures

- A handler error asks for redelivery.
- Errors a retry cannot fix (`invalid`, `not_found`, `unauthorized`, `forbidden`) and payloads that do not decode go to `Consumer.DeadLetter` instead. The default logs them.
- Delivery is at least once. Make handlers idempotent: check state before acting, or wrap them with `middleware.Idempotent`.

`examples/checkout` shows the whole pattern, including compensation when a later step fails.

## Tracing

A trace follows a message when the publisher writes its headers and the consumer reads them:

```go
msg.Headers = otelx.Headers(ctx)
consumer := &queue.Consumer{Source: broker, Extract: otelx.WithHeaders}
```

The handler's span is then a child of the publisher's span. The outbox does not store headers yet, so a trace stops at an event that goes through the relay.

## Other brokers

A broker is two small methods, and either side can be used alone:

```go
Subscribe(ctx context.Context, topic string, h queue.Handler) error // queue.Subscriber
Publish(ctx context.Context, m queue.Message) error                 // outbox.QueuePublisher
```

`natsx/natsx.go` is about 170 lines and is a template for Kafka or SQS. Its tests in `natsx/integration` run against a NATS server started inside the test.
