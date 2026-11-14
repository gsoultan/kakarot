# Changelog

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

- `transport/grpcx`: interface-first gRPC services and typed clients, bearer auth, custom status codes, `Carrier` for trace and context propagation, a `.proto` file written from the service (`Service.Proto`) and protobuf clients (`EnableProtobuf`).
- `transport/wsx`: WebSocket streams.
- `data`: storm `TxManager`, error mapping, transactional `outbox` with a relay, `idempotency` store.
- `otelx`: one span and one duration metric per call on every transport; trace headers for gRPC and queues.
- `natsx`: NATS JetStream broker for queue consumers and the outbox relay.
- `cmd/kakarot`: `new` scaffolds a project, `gen` writes the endpoint and transport layers from a service interface, `check` verifies layer rules, `//kakarot:` comments and generated code.

### Examples

- `examples/orders`, `examples/wallet`, `examples/checkout`.
