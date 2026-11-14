package grpcx

import (
	"net/netip"
	"reflect"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type state string

type line struct {
	SKU string `json:"sku"`
	Qty int32  `json:"qty"`
}

type everything struct {
	ID       string            `json:"id"`
	Status   state             `json:"status"`
	Paid     bool              `json:"paid"`
	Total    int64             `json:"total"`
	Count    int               `json:"count"`
	Small    int8              `json:"small"`
	Big      uint64            `json:"big"`
	Port     uint16            `json:"port"`
	Rate     float64           `json:"rate"`
	Ratio    float32           `json:"ratio"`
	Blob     []byte            `json:"blob"`
	Sum      [4]byte           `json:"sum"`
	At       time.Time         `json:"at"`
	Took     time.Duration     `json:"took"`
	Addr     netip.Addr        `json:"addr"`
	Line     line              `json:"line"`
	Next     *everything       `json:"next"`
	Note     *string           `json:"note"`
	Zero     *int32            `json:"zero"`
	Lines    []line            `json:"lines"`
	Refs     []*line           `json:"refs"`
	Tags     []string          `json:"tags"`
	Scores   []int64           `json:"scores"`
	Flags    []bool            `json:"flags"`
	Weights  []float64         `json:"weights"`
	Labels   map[string]string `json:"labels"`
	ByID     map[int32]line    `json:"by_id"`
	Seen     map[string]*line  `json:"seen"`
	Stamps   []time.Time       `json:"stamps"`
	Secret   string            `json:"-"`
	Internal string            `proto:"-"`
	hidden   int
}

func TestProtobufRoundTrip(t *testing.T) {
	note, zero := "", int32(0)
	in := everything{
		ID: "o-1", Status: "paid", Paid: true, Total: -42, Count: 7, Small: -3, Big: 1 << 63, Port: 8080,
		Rate: 1.5, Ratio: 0.25, Blob: []byte{1, 2}, Sum: [4]byte{9, 8, 7, 6},
		At: time.Date(2026, 11, 14, 10, 0, 0, 5, time.UTC), Took: -1500 * time.Millisecond,
		Addr: netip.MustParseAddr("10.0.0.1"), Line: line{SKU: "a", Qty: 2},
		Next: &everything{ID: "o-2"}, Note: &note, Zero: &zero,
		Lines: []line{{SKU: "a", Qty: 1}, {}}, Refs: []*line{{SKU: "r"}},
		Tags: []string{"x", "", "y"}, Scores: []int64{1, -1, 0}, Flags: []bool{true, false}, Weights: []float64{0.5, 0},
		Labels: map[string]string{"a": "1", "b": ""}, ByID: map[int32]line{-1: {SKU: "neg"}, 2: {}},
		Seen:   map[string]*line{"k": {Qty: 3}},
		Stamps: []time.Time{time.Unix(1, 0).UTC()},
		Secret: "never sent", Internal: "never sent", hidden: 1,
	}
	b, err := marshalProto(in)
	if err != nil {
		t.Fatal(err)
	}
	var out everything
	if err := unmarshalProto(b, &out); err != nil {
		t.Fatal(err)
	}
	want := in
	want.Secret, want.Internal, want.hidden = "", "", 0
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("round trip changed the value\n got %+v\nwant %+v", out, want)
	}
	// Presence: a pointer to a zero value arrives as a pointer, a nil stays nil.
	if out.Note == nil || out.Zero == nil {
		t.Fatal("a set optional field lost its presence")
	}
	var empty everything
	if b, _ := marshalProto(everything{}); len(b) != 0 {
		t.Fatalf("a zero value encodes to %d bytes, want 0", len(b))
	} else if err := unmarshalProto(b, &empty); err != nil || empty.Note != nil || !empty.At.IsZero() {
		t.Fatalf("zero value: %+v %v", empty, err)
	}
}

// The encoding is real protobuf: generated messages read what the Go
// structs wrote, and the other way round.
func TestProtobufMatchesGeneratedMessages(t *testing.T) {
	type violation struct {
		Field       string
		Description string
	}
	type badRequest struct {
		FieldViolations []violation
	}
	b, err := marshalProto(badRequest{FieldViolations: []violation{{"qty", "must be positive"}, {"sku", "required"}}})
	if err != nil {
		t.Fatal(err)
	}
	var br errdetails.BadRequest
	if err := proto.Unmarshal(b, &br); err != nil {
		t.Fatal(err)
	}
	if len(br.FieldViolations) != 2 || br.FieldViolations[1].Field != "sku" || br.FieldViolations[0].Description != "must be positive" {
		t.Fatalf("got %v", &br)
	}

	type errorInfo struct {
		Reason   string
		Domain   string
		Metadata map[string]string
	}
	wire, _ := proto.Marshal(&errdetails.ErrorInfo{Reason: "declined", Domain: "pay", Metadata: map[string]string{"a": "1", "b": "2"}})
	var ei errorInfo
	if err := unmarshalProto(wire, &ei); err != nil {
		t.Fatal(err)
	}
	if ei.Reason != "declined" || ei.Domain != "pay" || ei.Metadata["b"] != "2" {
		t.Fatalf("got %+v", ei)
	}

	type stamped struct {
		At   time.Time
		Took time.Duration
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	b, _ = marshalProto(stamped{At: at, Took: 90 * time.Second})
	var ts timestamppb.Timestamp
	var d durationpb.Duration
	if err := eachTop(b, &ts, &d); err != nil {
		t.Fatal(err)
	}
	if !ts.AsTime().Equal(at) || d.AsDuration() != 90*time.Second {
		t.Fatalf("got %v %v", ts.AsTime(), d.AsDuration())
	}

	// Packed and unpacked repeated numbers both decode; unknown fields are skipped.
	type file struct {
		Name    string  `proto:"1"`
		Public  []int32 `proto:"10"`
		Missing string  `proto:"999"`
	}
	wire, _ = proto.Marshal(&descriptorpb.FileDescriptorProto{Name: proto.String("a.proto"), Package: proto.String("skipped"), PublicDependency: []int32{3, 4}})
	var f file
	if err := unmarshalProto(wire, &f); err != nil {
		t.Fatal(err)
	}
	if f.Name != "a.proto" || !reflect.DeepEqual(f.Public, []int32{3, 4}) {
		t.Fatalf("got %+v", f)
	}
	b, _ = marshalProto(file{Public: []int32{5, 6}})
	var fdp descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(b, &fdp); err != nil || !reflect.DeepEqual(fdp.PublicDependency, []int32{5, 6}) {
		t.Fatalf("got %v %v", fdp.PublicDependency, err)
	}
}

// eachTop reads a message whose field 1 is a Timestamp and field 2 a Duration.
func eachTop(b []byte, ts *timestamppb.Timestamp, d *durationpb.Duration) error {
	type raw struct {
		At   []byte
		Took []byte
	}
	var r raw
	if err := unmarshalProto(b, &r); err != nil {
		return err
	}
	if err := proto.Unmarshal(r.At, ts); err != nil {
		return err
	}
	return proto.Unmarshal(r.Took, d)
}

func TestProtobufRejectsWhatItCannotCarry(t *testing.T) {
	for name, v := range map[string]any{
		"interface":   struct{ V any }{},
		"nested list": struct{ V [][]string }{},
		"float key":   struct{ V map[float64]string }{},
		"channel":     struct{ V chan int }{},
		"twice": struct {
			A, B string `proto:"1"`
		}{},
		"bad number": struct {
			A string `proto:"0"`
		}{},
		"not a number": struct {
			A string `proto:"x"`
		}{},
	} {
		if _, err := marshalProto(v); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	var l line
	if err := unmarshalProto([]byte{0x0a, 0x05, 'a'}, &l); err == nil {
		t.Error("want an error for a truncated message")
	}
	if err := unmarshalProto([]byte{0x08, 0x01}, &l); err == nil {
		t.Error("want an error for a number where a string belongs")
	}
}

func TestProtoNames(t *testing.T) {
	for in, want := range map[string]string{"id": "id", "OrderID": "order_id", "createdAt": "created_at", "order-id": "order_id", "HTTPStatus": "http_status", "SKU": "sku"} {
		if got := protoName(in); got != want {
			t.Errorf("protoName(%q) = %q, want %q", in, got, want)
		}
	}
}
