# Protobuf clients

A kakarot gRPC service is defined by a Go interface, and Go callers use the generated client, which speaks JSON over gRPC. Clients in other languages want a `.proto` file and the protobuf encoding. Both come from the same service, with no protoc step on the Go side.

## Turn it on

```go
grpcx.EnableProtobuf() // once in main, before serving

gs := grpc.NewServer()
transport.RegisterOrderGRPC(gs, eps, mws)
```

The server now answers each call in the encoding the client used. `EnableProtobuf` replaces the process's `proto` gRPC codec with one that also encodes plain Go values; generated protobuf messages are encoded exactly as before.

## Write the file

```go
schema, err := transport.OrderGRPC(eps).Proto()
```

Keep the result in the repository, for example as `api/order.proto`, and guard it with a test. `examples/orders/proto_test.go` is the pattern:

```console
$ go test ./...                                             # fails when the file is out of date
$ UPDATE_PROTO=1 go test -run TestProtoFileIsCurrent .      # refresh it
```

A change that would break existing clients then shows up as a diff of the `.proto` file in review. `Service.Descriptor()` returns the same schema as a `FileDescriptorProto` for tools that read descriptors.

## How Go types map

| Go | Protobuf |
|---|---|
| `string`, named string types | `string` |
| `bool` | `bool` |
| `int`, `int64` | `int64` |
| `int8`, `int16`, `int32` | `int32` |
| `uint`, `uint64` | `uint64` |
| `uint8`, `uint16`, `uint32` | `uint32` |
| `float64` / `float32` | `double` / `float` |
| `[]byte`, `[N]byte` | `bytes` |
| `time.Time` | `google.protobuf.Timestamp` |
| `time.Duration` | `google.protobuf.Duration` |
| a struct or array with `MarshalText` and `UnmarshalText` (a UUID, a decimal, `netip.Addr`) | `string` |
| struct | `message`, named after the Go type |
| `*T` | `optional T` (the client can tell "not sent" from zero) |
| `[]T` | `repeated T` |
| `map[K]V` | `map<K, V>` |

Further rules:

- **Field names** come from the `json` tag, in lower snake case. The JSON name is kept as `json_name`, so protobuf's JSON form matches kakarot's HTTP JSON.
- **Left out:** unexported fields and fields tagged `json:"-"` or `proto:"-"`.
- **Generic types** join their arguments: `kakarot.Page[domain.Order]` is `PageOrder`.
- **A request or response that is not a struct** (an `int`, a slice) is wrapped in a message with one field, `value = 1`, named `<Method>Request` or `<Method>Response`.
- **An embedded struct** is a nested message field.

`Proto` returns an error that names the method and the field for anything protobuf cannot carry: `any` and other interfaces, a list of lists, a map of lists, a map keyed by a float or a struct.

## Field numbers

A field's number is its position among the struct's exported fields, counted from 1. Protobuf clients identify fields by number, so:

- add new fields **at the end** of the struct, or
- pin numbers with a tag, after which fields may be reordered freely:

```go
type Order struct {
    ID     string `json:"id" proto:"1"`
    Status string `json:"status" proto:"3"`
    Note   string `json:"note" proto:"7"`
}
```

Removing a field shifts the fields after it unless they are pinned. Pin the numbers of any message that outside clients depend on.

## Not supported yet

Protobuf `enum` and `oneof`. A named Go string such as `domain.Status` is a `string` on the wire.
