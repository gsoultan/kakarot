package orders_test

import (
	"os"
	"testing"

	"github.com/gsoultan/kakarot/examples/orders/transport"
)

// api/order.proto is the contract for clients in other languages. It is
// written from the Go service; this test fails when the two drift, so a
// change that renumbers a field is seen in review. Refresh the file with
//
//	UPDATE_PROTO=1 go test -run TestProtoFileIsCurrent .
func TestProtoFileIsCurrent(t *testing.T) {
	eps, _, _ := newEndpoints()
	want, err := transport.OrderGRPC(eps).Proto()
	if err != nil {
		t.Fatal(err)
	}
	const path = "api/order.proto"
	if os.Getenv("UPDATE_PROTO") != "" {
		if err := os.MkdirAll("api", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s is out of date: run UPDATE_PROTO=1 go test -run TestProtoFileIsCurrent .", path)
	}
}
