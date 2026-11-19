# Changelog

## Unreleased

### Modules

- `rabbitx`: RabbitMQ broker for queue consumers and the outbox relay. Quorum queues per group, publisher confirms, unroutable messages fail instead of being dropped, redelivery with backoff, rejection to a dead-letter exchange after `MaxDeliver`, reconnection.
- `pubsubx`: Google Cloud Pub/Sub broker for queue consumers and the outbox relay. A subscription per group, optional ordering by key, optional creation of topics and subscriptions with a retry policy and a dead-letter topic.

## v0.1.0

The first release.

### Core (`github.com/gsoultan/kakarot`)

- `Endpoint[Req, Resp]` and typed streams (`ServerStream`, `ClientStream`, `BidiStream`), shared and typed middleware, `Wrap`, `Decorate`, `Usecase`.
- Errors as domain codes (`E`, `Invalid`, `CodeOf`, `Is`, custom codes) that every transport maps once.
- `App` runs servers with graceful shutdown; `Job` runs scheduled work; `Page` and typed cursors for keyset pagination.
- `transport/httpx`: router on `http.ServeMux`, typed `Handle`, SSE and NDJSON streams, uploads, OpenAPI 3.1.
- `middleware`: `Recover`, `Logging`, `Timeout`, `Validated`, `Retry`, `CircuitBreaker`, `RateLimit`, `LoadShed`, `Idempotent`.
- `auth`: typed principal, `Bearer`, `Require`.
- `queue`: consumers on typed topics, message headers, an in-process broker.
- `kakarottest`: `Tx`, `Do`, `WantCode`, `WantField`, `Eventually`, `Serve`, `Publish`, `Clock`.

### Modules

- `transport/grpcx`: interface-first gRPC services and typed clients, bearer auth, custom status codes, `Carrier` for trace and context propagation, a `.proto` file written from the service (`Service.Proto`) and protobuf clients (`EnableProtobuf`), with enums and oneofs.
- `transport/wsx`: WebSocket streams.
- `data`: storm `TxManager`, error mapping, transactional `outbox` with a relay that keeps trace headers (`outbox.Carry`), `idempotency` store.
- `otelx`: one span and one duration metric per call on every transport; trace headers for gRPC and queues.
- `natsx`, `kafkax`, `sqsx`: NATS JetStream, Kafka and Amazon SQS brokers for queue consumers and the outbox relay.
- `cmd/kakarot`: `new` scaffolds a project, `gen` writes the endpoint and transport layers from a service interface, `check` verifies layer rules, `//kakarot:` comments and generated code.

### Examples

- `examples/orders`, `examples/wallet`, `examples/checkout`.
