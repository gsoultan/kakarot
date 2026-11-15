# Events

Work that happens after the response (charging a card, sending mail, updating another service) runs as message handlers. A handler is an ordinary endpoint, so it has the same middleware, validation and tests as an HTTP route.

## The pieces

```text
usecase ── Tx ──> order row + outbox row        one database transaction
                         │
                   outbox.Relay                 publishes committed events, at least once
                         │
                      broker                    queue.Memory in one process; natsx, kafkax or sqsx across services
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

`natsx.Broker` settings:

| Setting | Meaning | Default |
|---|---|---|
| `Stream` | the JetStream stream | required |
| `Subjects` | create the stream, or update it, to capture these subjects; leave empty when the stream is managed elsewhere | none |
| `Group` | name of the durable consumers. Instances with the same group share the work; different groups each get every message | required to subscribe |
| `AckWait` | how long a handler may run before the message is delivered again | 30s |
| `MaxDeliver` | deliveries of one message before JetStream gives up | 10 |
| `Backoff` | pause before a failed message is delivered again | 1s |

A topic is a NATS subject. An event's ID is the JetStream message ID, so a relay that publishes the same event twice delivers it once.

## With Kafka

```go
broker := &kafkax.Broker{Brokers: []string{"localhost:9092"}, Group: "billing", DeadLetter: ".dead"}
defer broker.Close()

consumer := &queue.Consumer{Service: "billing", Source: broker}
relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
```

| Setting | Meaning | Default |
|---|---|---|
| `Brokers` | seed brokers, `host:port` | required |
| `Group` | the consumer group. Instances with the same group share the work; different groups each get every message | required to subscribe |
| `Options` | extra franz-go client options: TLS, SASL, a client ID | none |
| `MaxDeliver` | how often one message is handed to a handler | 10 |
| `Backoff` | pause between those tries | 1s |
| `DeadLetter` | suffix of the topic that takes a message after `MaxDeliver` failures | none: the message is logged and skipped |

A message's key is the record key, so messages with one key stay in order. An offset is committed only after its handler returned. Kafka cannot redeliver a single record, so a failed message is tried again in place, and its partition waits until it succeeds or reaches `MaxDeliver`. A new group starts at the oldest message.

## With Amazon SQS

```go
cfg, err := config.LoadDefaultConfig(ctx)
broker := &sqsx.Broker{Client: sqs.NewFromConfig(cfg), Queues: map[string]string{
    "order.placed": "https://sqs.eu-west-1.amazonaws.com/123456789012/billing-order-placed",
}}
```

| Setting | Meaning | Default |
|---|---|---|
| `Client` | an `*sqs.Client` | required |
| `Queues` | topic to queue URL | required |
| `Backoff` | how long a failed message stays hidden before SQS delivers it again | 5s |
| `Wait` | long-poll time of one receive call, at most 20s | 20s |

A queue hands each message to one consumer. Give every service that handles a topic its own queue and fan out to them with an SNS topic. A message that keeps failing moves to the dead-letter queue of the queue's redrive policy. On a FIFO queue the key is the message group and the event's ID deduplicates a repeated publish. Headers travel as message attributes; SQS allows ten per message and the broker uses up to two.

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

The handler's span is then a child of the publisher's span.

Events written through the outbox keep the trace as well. `outbox.Carry` stores the headers of the call with each event, and the relay hands them to the broker:

```go
mws := kakarot.Chain(otelx.Middleware(), outbox.Carry(otelx.Headers), middleware.Default(log, 5*time.Second))
```

The `outbox_events` table has a `headers` column for this. Run your migration after upgrading.

## Other brokers

A broker is two small methods, and either side can be used alone:

```go
Subscribe(ctx context.Context, topic string, h queue.Handler) error // queue.Subscriber
Publish(ctx context.Context, m queue.Message) error                 // outbox.QueuePublisher
```

`natsx/natsx.go` is about 170 lines and is a template for another broker. The NATS and Kafka tests run against a server started inside the test (`natsx/integration`, `kafkax/integration`).
