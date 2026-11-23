# Changelog

## Unreleased

### `transport/connectx` (new module)

- Connect RPC with connect-go v1.21.1: `NewService`, `AddUnary`, `AddServerStream`, `AddClientStream` and `AddBidi` build one `http.Handler` from typed endpoints that answers the Connect protocol, gRPC and gRPC-Web, with JSON and protobuf. No `.proto` file or code generation is needed; `Service.Proto` writes the same file as `grpcx`.
- Typed clients `Invoke`, `Receive`, `Send` and `Duplex`, with `Protobuf()`, `connect.WithGRPC()` and `connect.WithGRPCWeb()`. A gRPC client also calls interface-first `grpcx` servers.
- Errors keep their kakarot code and fields (`Error`, `FromError`); `Bearer` and `WithBearer` for auth; `Carrier` for trace propagation; `NewServer` serves HTTP/1.1, HTTP/2 and h2c.

### `transport/grpcx`

- `ProtoCodec` returns the protobuf codec of interface-first services, so other transports encode the same layout.

### `cmd/kakarot`

- `kakarot gen` writes `api/<group>.proto` for every service with `//kakarot:grpc` methods, from the same `Service.Proto()` the server uses. `kakarot check` reports the file when it is out of date, like the generated Go code.
- `examples/wallet` now commits `api/wallet.proto`; `examples/orders` drops its `.proto` test, which `kakarot check` replaces.

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
