# Events

Work that happens after the response (charging a card, sending mail, updating another service) runs as message handlers. A handler is an ordinary endpoint, so it has the same middleware, validation and tests as an HTTP route.

## The pieces

```text
usecase ── Tx ──> order row + outbox row        one database transaction
                         │
                   outbox.Relay                 publishes committed events, at least once
                         │
                      broker                    queue.Memory in one process; natsx, kafkax, sqsx, rabbitx or pubsubx across services
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

## With RabbitMQ

```go
broker := &rabbitx.Broker{URL: "amqp://guest:guest@localhost:5672/", Exchange: "orders", Group: "billing", DeadLetter: "orders.dead"}
defer broker.Close()

consumer := &queue.Consumer{Service: "billing", Source: broker}
relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
```

| Setting | Meaning | Default |
|---|---|---|
| `URL` | `amqp://` or `amqps://` URL | required |
| `Config` | an `amqp.Config`: TLS, credentials, heartbeat | library defaults |
| `Exchange` | the durable topic exchange to publish to and bind queues to; the broker declares it | required |
| `Group` | names the queues: `billing` reads `order.placed` from queue `billing.order.placed`. Instances with the same group share the work; different groups each get every message | required to subscribe |
| `Prefetch` | messages one subscription holds unacknowledged | 10 |
| `MaxDeliver` | deliveries of one message before it is rejected | 10 |
| `Backoff` | pause before a failed message goes back to its queue | 1s |
| `DeadLetter` | the exchange that takes a rejected message, with its original routing key; declare it yourself | none: RabbitMQ drops the message |

A topic is the routing key, so a subscription may use the topic exchange's `*` and `#` patterns. Each group's queue is a quorum queue, declared and bound when the consumer starts. A publish waits for RabbitMQ's confirm, and a message no queue is bound to fails with `unavailable` instead of being dropped, so the outbox relay keeps it until a consumer has declared its queue. The broker dials on first use and again after the connection drops; a subscription resumes after `Backoff`. RabbitMQ does not deduplicate by the event's ID, so make handlers idempotent. The delivery count comes from `x-acquired-count` on RabbitMQ 4 and from `x-delivery-count` on RabbitMQ 3.

## With Google Cloud Pub/Sub

```go
client, err := pubsub.NewClient(ctx, "my-project")
broker := &pubsubx.Broker{Client: client, Group: "billing", DeadLetter: ".dead"}
defer broker.Close()

consumer := &queue.Consumer{Service: "billing", Source: broker}
relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
```

| Setting | Meaning | Default |
|---|---|---|
| `Client` | a `*pubsub.Client` from `cloud.google.com/go/pubsub/v2` | required |
| `Group` | names the subscriptions: `billing` reads topic `order.placed` through subscription `billing.order.placed` | required to subscribe |
| `Create` | create missing topics and subscriptions with the settings below; for development and the emulator | off: they must exist |
| `Ordered` | publish the key as the ordering key, and create subscriptions with ordering on, so one key's messages arrive in order | off |
| `Concurrency` | messages one subscription handles at once | 10 |
| `Backoff` | minimum backoff of a created subscription's retry policy, at most 10m | 1s |
| `DeadLetter` | suffix of the topic that takes a message after `MaxDeliver` deliveries | none: Pub/Sub keeps delivering until retention ends |
| `MaxDeliver` | deliveries of one message before it is dead-lettered, 5 to 100 | 10 |

A topic is a Pub/Sub topic ID. Headers, the event's ID and the key travel as attributes. Without `Create`, the retry and dead-letter settings belong to the subscription you manage, for example in Terraform, and Pub/Sub's service account needs permission to publish to the dead-letter topic. Pub/Sub does not deduplicate by the event's ID, so make handlers idempotent. Point `PUBSUB_EMULATOR_HOST` at the emulator to run locally.

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

`natsx/natsx.go` is about 170 lines and is a template for another broker. The NATS and Kafka tests run against a server started inside the test (`natsx/integration`, `kafkax/integration`), and the Pub/Sub tests against the client library's in-memory server. The RabbitMQ tests named `TestLive...` need a server and are skipped unless `KAKAROT_TEST_AMQP` is set:

```console
$ docker run -d -p 5672:5672 rabbitmq:4
$ cd rabbitx && KAKAROT_TEST_AMQP=amqp://guest:guest@localhost:5672/ go test -race ./...
```
