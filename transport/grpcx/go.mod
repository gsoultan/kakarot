module github.com/gsoultan/kakarot/transport/grpcx

go 1.27.4

require (
	github.com/gsoultan/kakarot v0.0.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261110000943-09e21b237f2c
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
)

replace github.com/gsoultan/kakarot => ../..
