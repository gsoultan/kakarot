// Command kakarot generates the endpoint and transport layers from an
// annotated service interface, and scaffolds new projects.
//
//	kakarot new <module path> [dir]
//	kakarot gen [service dir]
//
// gen reads interfaces marked //kakarot:service [group] and writes
// ../endpoint/endpoints_gen.go and, for methods marked //kakarot:http, the
// HTTP bindings in ../transport/http_gen.go. Method shapes decide the kind:
//
//	(ctx, Req) (Resp, error)                        Endpoint
//	(ctx, Req) iter.Seq2[Msg, error]                ServerStream (SSE / NDJSON)
//	(ctx, iter.Seq2[Msg, error]) (Resp, error)      ClientStream (NDJSON upload)
//	(ctx, iter.Seq2[In, error], func(Out) error) error  BidiStream (WebSocket)
//
// Use it from go:generate in the service package:
//
//	//go:generate go run github.com/gsoultan/kakarot/cmd/kakarot gen
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "gen":
		dir := "."
		if len(args) > 1 {
			dir = args[1]
		}
		return generate(dir)
	case "new":
		if len(args) < 2 {
			return usage()
		}
		dir := ""
		if len(args) > 2 {
			dir = args[2]
		}
		return scaffold(args[1], dir)
	default:
		return usage()
	}
}

func usage() error {
	return fmt.Errorf("usage:\n  kakarot new <module path> [dir]\n  kakarot gen [service dir]")
}
