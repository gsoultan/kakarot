# Changelog

## Unreleased

### Fixed

- `kafkax`: publishing to a topic that does not exist yet, including the dead-letter topic, failed with `UNKNOWN_TOPIC_OR_PARTITION` even on a cluster that creates topics on first use; a missing dead-letter topic held its partition forever. The broker now asks the cluster to create it.
- `kafkax`: a message whose handler finished just before shutdown was not committed and was delivered again.
- `sqsx`: a payload that is not text SQS accepts (protobuf, any binary) was refused; it now travels base64-encoded.
- `sqsx`: a message without an ID was refused by a FIFO queue without content-based deduplication; it is now deduplicated by its payload.

### Tests

- `kafkax/integration` runs against a real Kafka cluster when `KAKAROT_TEST_KAFKA` is set, and `sqsx/integration` against LocalStack or Amazon SQS when `KAKAROT_TEST_SQS` is set. CI runs both against containers. The Kafka tests have run against Apache Kafka 4.1; the SQS tests have run against LocalStack only, not yet against Amazon SQS itself.

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
