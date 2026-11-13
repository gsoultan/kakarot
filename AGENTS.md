# Working on kakarot

This file is for anyone changing this repository, people and coding agents alike. For building a service *with* kakarot, read `README.md`; a scaffolded project gets its own `AGENTS.md`.

## What this is

kakarot is a layered Go framework: transport, middleware, endpoint, service, usecase, repository. Every operation is a typed `kakarot.Endpoint[Req, Resp]` or a typed stream. The public API has no `any` and no casts.

## Repository map

Each directory with a `go.mod` is its own module. The core has no dependencies outside the standard library; anything that needs a third-party library is a separate module.

| Path | Module | Contents |
|---|---|---|
| `.` | `github.com/gsoultan/kakarot` | `Endpoint`, streams, `Middleware`, `Error` and codes, `App`, `Job`, `Page`; `middleware/`, `auth/`, `queue/`, `transport/httpx/`, `kakarottest/` |
| `transport/grpcx` | own module | gRPC server and clients |
| `transport/wsx` | own module | WebSocket streams |
| `data` | own module | storm `TxManager`, `outbox`, `idempotency` |
| `otelx` | own module | OpenTelemetry middleware |
| `cmd/kakarot` | own module | CLI: `new`, `gen`, `check`; project template in `skeleton/` |
| `examples/orders`, `examples/wallet`, `examples/checkout` | own modules | complete services |

Sub-modules use `replace` directives to the local core, so a change to the core is seen by all of them at once.

## Commands

Go 1.27.4 or later. Run these in each module you touched; CI runs them in every module.

```console
$ gofmt -l .                      # must print nothing
$ go vet ./...
$ go test -race ./...
$ go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

Tests named `TestLive...` need PostgreSQL and are skipped unless `KAKAROT_TEST_DSN` is set. Each example needs a database of its own, because its tests migrate the schema:

```console
$ cd examples/checkout && KAKAROT_TEST_DSN=postgres://user:pass@localhost:5432/checkout go test -race ./...
$ cd examples/checkout && go tool kakarot check
```

After changing a `service/service.go` or a `data/model` in an example, run `go generate ./...` there.

## Rules

- Keep the core standard-library only. A feature that needs a dependency goes in a new or existing sub-module.
- No `any`, `interface{}` or type assertion in an exported signature. Use type parameters.
- Errors are `*kakarot.Error` with a `Code`. Create them with `kakarot.E`, `NotFound`, `Invalid` or `Conflict`, and compare with `kakarot.Is`. Internal causes are logged, never sent to a caller.
- Shared middleware has the shape `func(ctx, op, next) error` and never sees the request. Middleware that needs the request is a `TypedMiddleware[Req, Resp]`.
- Anything long-running implements `kakarot.Server` (`Serve`, `Shutdown`), and `Shutdown` is safe before `Serve` and safe to call twice.
- Never edit `*_gen.go`, `*.gen.go` or `data/store/`. Change the source and regenerate. The generator's templates are in `cmd/kakarot/templates.go`; a change there means regenerating all three examples.
- Every behaviour change comes with a test. Write the test first; it should fail for the reason you expect.
- Every exported name has a doc comment that says what it does, in plain words.
- When the CLI rejects something, the message says where (`file:line`), what is wrong, and what to write instead.
- Update `README.md` in the same change when a public name or behaviour changes.

## Where to add things

| To add | Put it in | Test with |
|---|---|---|
| Shared middleware | `middleware/` | a table test over `kakarot.Wrap` |
| A transport feature | `transport/httpx`, or the transport's module | `httptest` and `kakarottest.Do` |
| A `//kakarot:` directive | `knownDirectives` and `checkDirectives` in `cmd/kakarot/gen.go`, then the template | `cmd/kakarot/gen_test.go`, `check_test.go` |
| A layer rule | `layerRules` in `cmd/kakarot/check.go` | `check_test.go` |
| A test helper | `kakarottest/` | `kakarottest_test.go` |
| A file in new projects | `cmd/kakarot/skeleton/` as `<name>.tmpl` (delimiters `[[ ]]`) | scaffold a project and run its tests |
