module github.com/gsoultan/kakarot/examples/orders

go 1.27.4

require (
	github.com/google/uuid v1.6.0
	github.com/gsoultan/kakarot v0.0.0
	github.com/gsoultan/kakarot/data v0.0.0
	github.com/gsoultan/kakarot/otelx v0.0.0
	github.com/gsoultan/kakarot/transport/grpcx v0.0.0
	github.com/gsoultan/storm v1.12.1
	google.golang.org/grpc v1.85.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gsoultan/kakarot/cmd/kakarot v0.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.13.0 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	golang.org/x/mod v0.43.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	golang.org/x/tools v0.52.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261110000943-09e21b237f2c // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/gsoultan/kakarot => ../..

tool github.com/gsoultan/kakarot/cmd/kakarot

replace github.com/gsoultan/kakarot/data => ../../data

replace github.com/gsoultan/kakarot/cmd/kakarot => ../../cmd/kakarot

replace github.com/gsoultan/kakarot/transport/grpcx => ../../transport/grpcx

replace github.com/gsoultan/kakarot/otelx => ../../otelx
