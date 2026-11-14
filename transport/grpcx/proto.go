package grpcx

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Protobuf clients for an interface-first service.
//
// The Go side of a service speaks JSON over gRPC. A client in another
// language wants a .proto file and the protobuf encoding instead. Service.Proto
// writes that file from the registered methods, and EnableProtobuf makes the
// server accept and answer protobuf, both from the same description of the
// Go types:
//
//	func main() {
//		grpcx.EnableProtobuf()
//		svc := transport.NewOrderGRPC(eps, mws)
//		schema, err := svc.Proto() // write it to api/order.proto
//		...
//	}
//
// Field numbers follow the order of a struct's exported fields. Add new
// fields at the end, or pin a number with a `proto:"N"` tag, and keep the
// written file in the repository with a test that compares it to
// Service.Proto, so a change that would break clients shows up in review.

// rawMessage is what a server asks its codec for: the bytes as they came,
// and which encoding they are in. The handler decodes them itself, so a bad
// message is reported as CodeInvalid.
type rawMessage struct {
	bytes []byte
	proto bool
}

type protoCodec struct{}

func (protoCodec) Name() string { return "proto" }

func (protoCodec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case proto.Message:
		return proto.Marshal(m)
	case protoadapt.MessageV1:
		return proto.Marshal(protoadapt.MessageV2Of(m))
	}
	return marshalProto(v)
}

func (protoCodec) Unmarshal(b []byte, v any) error {
	switch m := v.(type) {
	case *rawMessage:
		m.bytes, m.proto = append(m.bytes[:0], b...), true
		return nil
	case proto.Message:
		return proto.Unmarshal(b, m)
	case protoadapt.MessageV1:
		return proto.Unmarshal(b, protoadapt.MessageV2Of(m))
	}
	return unmarshalProto(b, v)
}

var enableProtobuf sync.Once

// EnableProtobuf lets interface-first services answer clients that speak
// protobuf, the default encoding of gRPC, besides JSON. Call it once in
// main before serving.
//
// It replaces the process's "proto" gRPC codec with one that also encodes
// plain Go values. Generated protobuf messages are encoded exactly as
// before.
func EnableProtobuf() {
	enableProtobuf.Do(func() { encoding.RegisterCodec(protoCodec{}) })
}

// rpc records the types of one method, for the schema.
type rpc struct {
	name                       string
	in, out                    reflect.Type
	clientStream, serverStream bool
}

// Descriptor describes the service as a protobuf file, for tools that read
// descriptors rather than .proto text.
func (s *Service) Descriptor() (*descriptorpb.FileDescriptorProto, error) {
	pkg, name := "", s.name
	if i := strings.LastIndexByte(s.name, '.'); i >= 0 {
		pkg, name = s.name[:i], s.name[i+1:]
	}
	b := &schema{pkg: pkg, names: map[reflect.Type]string{}, taken: map[string]bool{}, imports: map[string]bool{}}
	svc := &descriptorpb.ServiceDescriptorProto{Name: proto.String(name)}
	for _, r := range s.rpcs {
		in, err := b.top(r.in, r.name+"Request")
		if err != nil {
			return nil, fmt.Errorf("grpcx: %s.%s request: %w", s.name, r.name, err)
		}
		out, err := b.top(r.out, r.name+"Response")
		if err != nil {
			return nil, fmt.Errorf("grpcx: %s.%s response: %w", s.name, r.name, err)
		}
		m := &descriptorpb.MethodDescriptorProto{Name: proto.String(r.name), InputType: proto.String(in), OutputType: proto.String(out)}
		if r.clientStream {
			m.ClientStreaming = proto.Bool(true)
		}
		if r.serverStream {
			m.ServerStreaming = proto.Bool(true)
		}
		svc.Method = append(svc.Method, m)
	}
	fd := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(strings.ReplaceAll(s.name, ".", "/") + ".proto"),
		Syntax:      proto.String("proto3"),
		MessageType: b.messages,
		Service:     []*descriptorpb.ServiceDescriptorProto{svc},
	}
	if pkg != "" {
		fd.Package = proto.String(pkg)
	}
	for _, imp := range []string{"google/protobuf/duration.proto", "google/protobuf/timestamp.proto"} {
		if b.imports[imp] {
			fd.Dependency = append(fd.Dependency, imp)
		}
	}
	return fd, nil
}

type schema struct {
	pkg      string
	names    map[reflect.Type]string // struct type -> message name
	taken    map[string]bool
	messages []*descriptorpb.DescriptorProto
	imports  map[string]bool
}

func (b *schema) qualify(name string) string {
	if b.pkg == "" {
		return "." + name
	}
	return "." + b.pkg + "." + name
}

// top returns the message type of a request or response. A value that is
// not a struct is wrapped in a message named hint with one field, value.
func (b *schema) top(t reflect.Type, hint string) (string, error) {
	s, err := describe(t)
	if err != nil {
		return "", err
	}
	if s.singular() && s.kind == kMessage {
		return b.message(s, hint)
	}
	if b.taken[hint] {
		return "", fmt.Errorf("message name %s is used twice", hint)
	}
	b.taken[hint] = true
	msg := &descriptorpb.DescriptorProto{Name: proto.String(hint)}
	b.messages = append(b.messages, msg)
	return b.qualify(hint), b.field(msg, &field{num: 1, name: "value", json: "value", shape: s}, hint)
}

var (
	typeArgs  = regexp.MustCompile(`[\[\],]+`)
	qualifier = regexp.MustCompile(`^.*[./]`)
	notIdent  = regexp.MustCompile(`[^A-Za-z0-9_]+`)
)

// messageName names a struct type's message: the Go type name with a
// capital first letter, and the type arguments of a generic type joined on
// (Page[domain.Order] is PageOrder), or hint for a struct that has no name.
func messageName(t reflect.Type, hint string) string {
	if t.Name() == "" {
		return hint
	}
	var b strings.Builder
	for _, part := range typeArgs.Split(t.Name(), -1) {
		part = notIdent.ReplaceAllString(qualifier.ReplaceAllString(part, ""), "")
		if part != "" {
			b.WriteString(strings.ToUpper(part[:1]) + part[1:])
		}
	}
	return b.String()
}

func (b *schema) message(s *shape, hint string) (string, error) {
	if name, ok := b.names[s.rt]; ok {
		return b.qualify(name), nil
	}
	name := messageName(s.rt, hint)
	if b.taken[name] {
		return "", fmt.Errorf("two types would both be message %s (one is %s): rename one", name, s.rt)
	}
	b.taken[name] = true
	if s.rt.Name() != "" { // a struct without a name is a message of its own at each use
		b.names[s.rt] = name
	}
	msg := &descriptorpb.DescriptorProto{Name: proto.String(name)}
	b.messages = append(b.messages, msg)
	for i := range s.fields {
		if err := b.field(msg, &s.fields[i], name); err != nil {
			return "", err
		}
	}
	return b.qualify(name), nil
}

func (b *schema) field(msg *descriptorpb.DescriptorProto, f *field, owner string) error {
	fd := &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(f.name),
		JsonName: proto.String(f.json),
		Number:   proto.Int32(int32(f.num)),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
	s := f.shape
	switch {
	case s.dict:
		entry := &descriptorpb.DescriptorProto{
			Name:    proto.String(camel(f.name) + "Entry"),
			Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
		}
		for _, part := range []field{{num: 1, name: "key", json: "key", shape: s.key}, {num: 2, name: "value", json: "value", shape: s.elem}} {
			if err := b.field(entry, &part, owner+camel(f.name)); err != nil {
				return err
			}
		}
		// An entry's fields have no presence of their own.
		entry.OneofDecl = nil
		for _, ef := range entry.Field {
			ef.Proto3Optional, ef.OneofIndex = nil, nil
		}
		msg.NestedType = append(msg.NestedType, entry)
		fd.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		fd.TypeName = proto.String(b.qualify(msg.GetName()) + "." + entry.GetName())
	default:
		one := s
		if s.list {
			one = s.elem
			fd.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		}
		typ, typeName, err := b.fieldType(one, owner+camel(f.name))
		if err != nil {
			return err
		}
		fd.Type = typ.Enum()
		if typeName != "" {
			fd.TypeName = proto.String(typeName)
		}
		// A pointer to a scalar is an optional field: the client can tell
		// "not sent" from the zero value.
		if !s.list && s.ptr && typ != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
			fd.Proto3Optional = proto.Bool(true)
			fd.OneofIndex = proto.Int32(int32(len(msg.OneofDecl)))
			msg.OneofDecl = append(msg.OneofDecl, &descriptorpb.OneofDescriptorProto{Name: proto.String("_" + f.name)})
		}
	}
	msg.Field = append(msg.Field, fd)
	return nil
}

func (b *schema) fieldType(s *shape, hint string) (descriptorpb.FieldDescriptorProto_Type, string, error) {
	switch s.kind {
	case kMessage:
		name, err := b.message(s, hint)
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, name, err
	case kTimestamp:
		b.imports["google/protobuf/timestamp.proto"] = true
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".google.protobuf.Timestamp", nil
	case kDuration:
		b.imports["google/protobuf/duration.proto"] = true
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".google.protobuf.Duration", nil
	case kBytes:
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES, "", nil
	case kBool:
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL, "", nil
	case kInt64:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64, "", nil
	case kInt32:
		return descriptorpb.FieldDescriptorProto_TYPE_INT32, "", nil
	case kUint64:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT64, "", nil
	case kUint32:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT32, "", nil
	case kDouble:
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, "", nil
	case kFloat:
		return descriptorpb.FieldDescriptorProto_TYPE_FLOAT, "", nil
	}
	return descriptorpb.FieldDescriptorProto_TYPE_STRING, "", nil // kString, kText
}

// camel is the name protoc derives a map entry's message from: order_lines
// becomes OrderLines.
func camel(s string) string {
	var b strings.Builder
	up := true
	for _, r := range s {
		switch {
		case r == '_':
			up = true
		case up && r >= 'a' && r <= 'z':
			b.WriteRune(r - 'a' + 'A')
			up = false
		default:
			b.WriteRune(r)
			up = false
		}
	}
	return b.String()
}

// Proto writes the service as a proto3 file: one rpc per registered method
// and one message per Go struct they use.
func (s *Service) Proto() (string, error) {
	fd, err := s.Descriptor()
	if err != nil {
		return "", err
	}
	var w strings.Builder
	w.WriteString("// Written by kakarot from the Go service " + s.name + ". Do not edit.\n")
	w.WriteString("// Field numbers follow the order of the Go struct fields: add new fields at\n")
	w.WriteString("// the end, or pin a number with a `proto:\"N\"` struct tag.\n\n")
	w.WriteString("syntax = \"proto3\";\n")
	if fd.Package != nil {
		w.WriteString("\npackage " + fd.GetPackage() + ";\n")
	}
	if len(fd.Dependency) > 0 {
		w.WriteString("\n")
		for _, d := range fd.Dependency {
			fmt.Fprintf(&w, "import %q;\n", d)
		}
	}
	local := func(name string) string {
		name = strings.TrimPrefix(name, ".")
		if fd.Package != nil {
			name = strings.TrimPrefix(name, fd.GetPackage()+".")
		}
		return name
	}
	for _, svc := range fd.Service {
		fmt.Fprintf(&w, "\nservice %s {\n", svc.GetName())
		for _, m := range svc.Method {
			in, out := local(m.GetInputType()), local(m.GetOutputType())
			if m.GetClientStreaming() {
				in = "stream " + in
			}
			if m.GetServerStreaming() {
				out = "stream " + out
			}
			fmt.Fprintf(&w, "  rpc %s(%s) returns (%s);\n", m.GetName(), in, out)
		}
		w.WriteString("}\n")
	}
	scalar := func(f *descriptorpb.FieldDescriptorProto) string {
		if f.TypeName != nil {
			return local(f.GetTypeName())
		}
		return strings.ToLower(strings.TrimPrefix(f.GetType().String(), "TYPE_"))
	}
	for _, msg := range fd.MessageType {
		if len(msg.Field) == 0 {
			fmt.Fprintf(&w, "\nmessage %s {}\n", msg.GetName())
			continue
		}
		fmt.Fprintf(&w, "\nmessage %s {\n", msg.GetName())
		entries := map[string]*descriptorpb.DescriptorProto{}
		for _, n := range msg.NestedType {
			entries[local(msg.GetName()+"."+n.GetName())] = n
		}
		for _, f := range msg.Field {
			typ := scalar(f)
			switch entry := entries[typ]; {
			case entry != nil:
				typ = fmt.Sprintf("map<%s, %s>", scalar(entry.Field[0]), scalar(entry.Field[1]))
			case f.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED:
				typ = "repeated " + typ
			case f.GetProto3Optional():
				typ = "optional " + typ
			}
			opts := ""
			if f.GetJsonName() != lowerCamel(f.GetName()) {
				opts = fmt.Sprintf(" [json_name = %q]", f.GetJsonName())
			}
			fmt.Fprintf(&w, "  %s %s = %d%s;\n", typ, f.GetName(), f.GetNumber(), opts)
		}
		w.WriteString("}\n")
	}
	return w.String(), nil
}

// lowerCamel is the JSON name protoc gives a field by default.
func lowerCamel(s string) string {
	c := camel(s)
	if c == "" {
		return c
	}
	return strings.ToLower(c[:1]) + c[1:]
}
