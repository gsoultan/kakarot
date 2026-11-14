package grpcx_test

import (
	"context"
	"io"
	"iter"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/transport/grpcx"
)

type shopLine struct {
	SKU string `json:"sku"`
	Qty int32  `json:"qty"`
}

type shopOrder struct {
	ID         string            `json:"id"`
	Lines      []shopLine        `json:"lines"`
	Note       *string           `json:"note"`
	Labels     map[string]string `json:"labels"`
	CreatedAt  time.Time         `json:"created_at"`
	TotalCents int64             `json:"totalCents"`
}

type shopPage[T any] struct {
	Items []T    `json:"items"`
	Next  string `json:"next"`
}

type findOrder struct {
	ID string `json:"id"`
}

func shopService() *grpcx.Service {
	svc := grpcx.NewService("shop.v1.Orders")
	grpcx.AddUnary(svc, "Get", func(_ context.Context, in findOrder) (shopOrder, error) {
		if in.ID == "" {
			return shopOrder{}, kakarot.E(kakarot.CodeInvalid, "id is required", nil)
		}
		note := "gift"
		return shopOrder{ID: in.ID, Lines: []shopLine{{SKU: "a", Qty: 2}}, Note: &note, Labels: map[string]string{"k": "v"},
			CreatedAt: time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC), TotalCents: 1250}, nil
	})
	grpcx.AddUnary(svc, "List", func(context.Context, struct{}) (shopPage[shopOrder], error) {
		return shopPage[shopOrder]{Items: []shopOrder{{ID: "o-1"}}, Next: "n"}, nil
	})
	grpcx.AddUnary(svc, "Count", func(context.Context, struct{}) (int, error) { return 3, nil })
	grpcx.AddServerStream(svc, "Watch", func(_ context.Context, in findOrder) iter.Seq2[shopOrder, error] {
		return kakarot.SliceStream([]shopOrder{{ID: in.ID}, {ID: in.ID + "-2"}})
	})
	grpcx.AddBidi(svc, "Echo", func(_ context.Context, in iter.Seq2[shopLine, error], send func(shopLine) error) error {
		for l, err := range in {
			if err != nil {
				return err
			}
			l.Qty *= 2
			if err := send(l); err != nil {
				return err
			}
		}
		return nil
	})
	return svc
}

const wantProto = `// Written by kakarot from the Go service shop.v1.Orders. Do not edit.
// Field numbers follow the order of the Go struct fields: add new fields at
// the end, or pin a number with a ` + "`proto:\"N\"`" + ` struct tag.

syntax = "proto3";

package shop.v1;

import "google/protobuf/timestamp.proto";

service Orders {
  rpc Get(FindOrder) returns (ShopOrder);
  rpc List(ListRequest) returns (ShopPageShopOrder);
  rpc Count(CountRequest) returns (CountResponse);
  rpc Watch(FindOrder) returns (stream ShopOrder);
  rpc Echo(stream ShopLine) returns (stream ShopLine);
}
`

func TestProtoFile(t *testing.T) {
	got, err := shopService().Proto()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, wantProto) {
		t.Fatalf("unexpected head:\n%s", got)
	}
	for _, want := range []string{
		"message ShopOrder {\n  string id = 1;\n  repeated ShopLine lines = 2;\n  optional string note = 3;\n  map<string, string> labels = 4;\n  google.protobuf.Timestamp created_at = 5 [json_name = \"created_at\"];\n  int64 total_cents = 6;\n}\n",
		"message ListRequest {}\n",
		"message CountResponse {\n  int64 value = 1;\n}\n",
		"message ShopLine {\n  string sku = 1;\n  int32 qty = 2;\n}\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing:\n%s\nin:\n%s", want, got)
		}
	}
}

// A client that knows only the written schema and speaks plain protobuf,
// as a client in another language does, calls the Go service.
func TestProtobufClientCallsTheService(t *testing.T) {
	grpcx.EnableProtobuf()
	svc := shopService()
	fdp, err := svc.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	_ = timestamppb.File_google_protobuf_timestamp_proto // the schema imports it
	file, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("the schema is not valid protobuf: %v", err)
	}
	msg := func(name string) *dynamicpb.Message {
		d := file.Messages().ByName(protoreflect.Name(name))
		if d == nil {
			t.Fatalf("no message %s", name)
		}
		return dynamicpb.NewMessage(d)
	}
	str := func(m *dynamicpb.Message, f string) string {
		return m.Get(m.Descriptor().Fields().ByName(protoreflect.Name(f))).String()
	}

	s := grpc.NewServer()
	svc.Register(s)
	lis := bufconn.Listen(1 << 20)
	srv := &grpcx.Server{GRPC: s, Listener: lis}
	go srv.Serve(t.Context())
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx := t.Context()

	// Unary, with nested messages, a map, an optional field and a timestamp.
	req, order := msg("FindOrder"), msg("ShopOrder")
	req.Set(req.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("o-9"))
	if err := conn.Invoke(ctx, "/shop.v1.Orders/Get", req, order); err != nil {
		t.Fatal(err)
	}
	fields := order.Descriptor().Fields()
	if str(order, "id") != "o-9" || str(order, "note") != "gift" || order.Get(fields.ByName("total_cents")).Int() != 1250 {
		t.Fatalf("got %v", order)
	}
	if !order.Has(fields.ByName("note")) {
		t.Fatal("optional note is not present")
	}
	l0 := order.Get(fields.ByName("lines")).List().Get(0).Message()
	if l0.Get(l0.Descriptor().Fields().ByName("qty")).Int() != 2 {
		t.Fatalf("lines: %v", order)
	}
	if order.Get(fields.ByName("labels")).Map().Get(protoreflect.ValueOfString("k").MapKey()).String() != "v" {
		t.Fatalf("labels: %v", order)
	}
	at := order.Get(fields.ByName("created_at")).Message()
	if at.Get(at.Descriptor().Fields().ByName("seconds")).Int() != time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("created_at: %v", order)
	}

	// A kakarot error keeps its gRPC status.
	err = conn.Invoke(ctx, "/shop.v1.Orders/Get", msg("FindOrder"), msg("ShopOrder"))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
	// Bytes that are not the message are the caller's fault, not the server's.
	bad := msg("CountResponse")
	bad.Set(bad.Descriptor().Fields().ByName("value"), protoreflect.ValueOfInt64(5))
	if err := conn.Invoke(ctx, "/shop.v1.Orders/Get", bad, msg("ShopOrder")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument for a malformed message", err)
	}

	// Empty request, generic response, and a wrapped scalar.
	page := msg("ShopPageShopOrder")
	if err := conn.Invoke(ctx, "/shop.v1.Orders/List", msg("ListRequest"), page); err != nil {
		t.Fatal(err)
	}
	if page.Get(page.Descriptor().Fields().ByName("items")).List().Len() != 1 || str(page, "next") != "n" {
		t.Fatalf("page: %v", page)
	}
	count := msg("CountResponse")
	if err := conn.Invoke(ctx, "/shop.v1.Orders/Count", msg("CountRequest"), count); err != nil {
		t.Fatal(err)
	}
	if count.Get(count.Descriptor().Fields().ByName("value")).Int() != 3 {
		t.Fatalf("count: %v", count)
	}

	// Server stream.
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/shop.v1.Orders/Watch")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(req); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseSend()
	var ids []string
	for {
		m := msg("ShopOrder")
		if err := stream.RecvMsg(m); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, str(m, "id"))
	}
	if strings.Join(ids, ",") != "o-9,o-9-2" {
		t.Fatalf("stream: %v", ids)
	}

	// Bidirectional stream.
	bidi, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, "/shop.v1.Orders/Echo")
	if err != nil {
		t.Fatal(err)
	}
	in := msg("ShopLine")
	in.Set(in.Descriptor().Fields().ByName("qty"), protoreflect.ValueOfInt32(21))
	if err := bidi.SendMsg(in); err != nil {
		t.Fatal(err)
	}
	out := msg("ShopLine")
	if err := bidi.RecvMsg(out); err != nil {
		t.Fatal(err)
	}
	if out.Get(out.Descriptor().Fields().ByName("qty")).Int() != 42 {
		t.Fatalf("echo: %v", out)
	}
	_ = bidi.CloseSend()

	// The Go client keeps working on the same server, in JSON.
	got, err := grpcx.Invoke[findOrder, shopOrder](conn, "shop.v1.Orders", "Get")(ctx, findOrder{ID: "o-1"})
	if err != nil || got.ID != "o-1" || got.Labels["k"] != "v" {
		t.Fatalf("json client: %+v %v", got, err)
	}
}

func TestProtoReportsTypesItCannotDescribe(t *testing.T) {
	svc := grpcx.NewService("x.Y")
	grpcx.AddUnary(svc, "Do", func(context.Context, struct{ V any }) (struct{}, error) { return struct{}{}, nil })
	if _, err := svc.Proto(); err == nil || !strings.Contains(err.Error(), "x.Y.Do request") {
		t.Fatalf("got %v", err)
	}
}
