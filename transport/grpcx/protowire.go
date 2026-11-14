package grpcx

import (
	"encoding"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// The protobuf side of an interface-first service. Go types are described
// once by reflection (describe), and that one description drives both the
// wire encoding here and the .proto schema in proto.go, so the two cannot
// disagree.
//
// How a Go type maps to protobuf:
//
//	string, named strings        string
//	bool                         bool
//	int, int64                   int64      int8, int16, int32     int32
//	uint, uint64                 uint64     uint8, uint16, uint32  uint32
//	float64                      double     float32                float
//	[]byte, [N]byte              bytes
//	time.Time                    google.protobuf.Timestamp
//	time.Duration                google.protobuf.Duration
//	encoding.TextMarshaler       string (a struct or array that marshals to text)
//	struct                       message
//	*T                           optional T
//	[]T                          repeated T
//	map[K]V                      map<K, V>
//
// A field's number is its position among the struct's exported fields,
// counted from 1, or the N of a `proto:"N"` tag. Fields tagged `json:"-"` or
// `proto:"-"` are left out.

type kind int

const (
	kMessage kind = iota
	kString
	kBytes
	kBool
	kInt64
	kInt32
	kUint64
	kUint32
	kDouble
	kFloat
	kTimestamp
	kDuration
	kText
)

// shape describes how one Go type is written in protobuf.
type shape struct {
	kind kind
	rt   reflect.Type // the Go type, without the pointer when ptr is set
	ptr  bool         // a Go pointer: the field has presence
	list bool         // a slice; elem describes one element
	dict bool         // a map; key and elem describe an entry
	key  *shape
	elem *shape

	fields []field // kMessage
	byNum  map[protowire.Number]*field
}

type field struct {
	num   protowire.Number
	name  string // protobuf field name
	json  string // name in the JSON encoding
	index int
	shape *shape
}

func (s *shape) goType() reflect.Type {
	if s.ptr {
		return reflect.PointerTo(s.rt)
	}
	return s.rt
}

func (s *shape) singular() bool { return !s.list && !s.dict }

// packable reports whether a list of s is written packed.
func (s *shape) packable() bool {
	switch s.kind {
	case kBool, kInt64, kInt32, kUint64, kUint32, kDouble, kFloat:
		return !s.ptr
	}
	return false
}

func (s *shape) wireType() protowire.Type {
	switch s.kind {
	case kBool, kInt64, kInt32, kUint64, kUint32:
		return protowire.VarintType
	case kDouble:
		return protowire.Fixed64Type
	case kFloat:
		return protowire.Fixed32Type
	}
	return protowire.BytesType
}

var (
	timeType     = reflect.TypeFor[time.Time]()
	durationType = reflect.TypeFor[time.Duration]()
	textMarshal  = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarsh  = reflect.TypeFor[encoding.TextUnmarshaler]()

	shapes  sync.Map // reflect.Type -> *shape
	shapeMu sync.Mutex
)

// describe returns the shape of t, or an error naming what protobuf cannot
// carry.
func describe(t reflect.Type) (*shape, error) {
	if s, ok := shapes.Load(t); ok {
		return s.(*shape), nil
	}
	shapeMu.Lock()
	defer shapeMu.Unlock()
	building := map[reflect.Type]*shape{}
	s, err := build(t, building)
	if err != nil {
		return nil, err
	}
	shapes.Store(t, s)
	return s, nil
}

func build(t reflect.Type, building map[reflect.Type]*shape) (*shape, error) {
	if s, ok := building[t]; ok {
		return s, nil // a type that refers to itself
	}
	if s, ok := shapes.Load(t); ok {
		return s.(*shape), nil
	}
	switch {
	case t == timeType:
		return &shape{kind: kTimestamp, rt: t}, nil
	case t == durationType:
		return &shape{kind: kDuration, rt: t}, nil
	case (t.Kind() == reflect.Struct || t.Kind() == reflect.Array) && t.Implements(textMarshal) && reflect.PointerTo(t).Implements(textUnmarsh):
		return &shape{kind: kText, rt: t}, nil
	}
	switch t.Kind() {
	case reflect.String:
		return &shape{kind: kString, rt: t}, nil
	case reflect.Bool:
		return &shape{kind: kBool, rt: t}, nil
	case reflect.Int, reflect.Int64:
		return &shape{kind: kInt64, rt: t}, nil
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return &shape{kind: kInt32, rt: t}, nil
	case reflect.Uint, reflect.Uint64, reflect.Uintptr:
		return &shape{kind: kUint64, rt: t}, nil
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return &shape{kind: kUint32, rt: t}, nil
	case reflect.Float64:
		return &shape{kind: kDouble, rt: t}, nil
	case reflect.Float32:
		return &shape{kind: kFloat, rt: t}, nil
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return &shape{kind: kBytes, rt: t}, nil
		}
		return nil, fmt.Errorf("%s: protobuf has no fixed-size array, use a slice", t)
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return &shape{kind: kBytes, rt: t}, nil
		}
		elem, err := build(t.Elem(), building)
		if err != nil {
			return nil, err
		}
		if !elem.singular() {
			return nil, fmt.Errorf("%s: protobuf cannot nest a list or map in a list, wrap the element in a struct", t)
		}
		return &shape{rt: t, list: true, elem: elem}, nil
	case reflect.Map:
		key, err := build(t.Key(), building)
		if err != nil {
			return nil, err
		}
		switch key.kind {
		case kString, kBool, kInt64, kInt32, kUint64, kUint32:
		default:
			return nil, fmt.Errorf("%s: a protobuf map key is a string, an integer or a bool", t)
		}
		elem, err := build(t.Elem(), building)
		if err != nil {
			return nil, err
		}
		if !elem.singular() {
			return nil, fmt.Errorf("%s: protobuf cannot nest a list or map in a map, wrap the value in a struct", t)
		}
		return &shape{rt: t, dict: true, key: key, elem: elem}, nil
	case reflect.Pointer:
		elem, err := build(t.Elem(), building)
		if err != nil {
			return nil, err
		}
		if !elem.singular() || elem.ptr {
			return nil, fmt.Errorf("%s: protobuf cannot carry a pointer to a list, map or pointer", t)
		}
		p := *elem
		p.ptr = true
		return &p, nil
	case reflect.Struct:
		s := &shape{kind: kMessage, rt: t, byNum: map[protowire.Number]*field{}}
		building[t] = s
		names, nums := map[string]bool{}, map[int]bool{}
		pos := 0
		for i := range t.NumField() {
			sf := t.Field(i)
			tag := sf.Tag.Get("proto")
			jsonName, _, _ := strings.Cut(sf.Tag.Get("json"), ",")
			if !sf.IsExported() || tag == "-" || jsonName == "-" {
				continue
			}
			pos++
			num := pos
			if tag != "" {
				n, err := strconv.Atoi(tag)
				if err != nil {
					return nil, fmt.Errorf("%s.%s: proto tag %q is not a field number", t, sf.Name, tag)
				}
				num = n
			}
			if num < 1 || num > 1<<29-1 || (num >= 19000 && num <= 19999) {
				return nil, fmt.Errorf("%s.%s: %d is not a usable protobuf field number", t, sf.Name, num)
			}
			if jsonName == "" {
				jsonName = sf.Name
			}
			name := protoName(jsonName)
			fs, err := build(sf.Type, building)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", t, sf.Name, err)
			}
			if nums[num] {
				return nil, fmt.Errorf("%s.%s: field number %d is used twice", t, sf.Name, num)
			}
			if names[name] {
				return nil, fmt.Errorf("%s.%s: protobuf field name %q is used twice", t, sf.Name, name)
			}
			names[name], nums[num] = true, true
			s.fields = append(s.fields, field{num: protowire.Number(num), name: name, json: jsonName, index: i, shape: fs})
		}
		for i := range s.fields {
			s.byNum[s.fields[i].num] = &s.fields[i]
		}
		return s, nil
	}
	return nil, fmt.Errorf("%s: protobuf cannot carry a %s", t, t.Kind())
}

// protoName turns a JSON or Go field name into a protobuf one: lower snake
// case, as protobuf style asks.
func protoName(s string) string {
	var b strings.Builder
	for i, r := range s {
		switch {
		case r >= 'A' && r <= 'Z':
			if i > 0 {
				prev := rune(s[i-1])
				nextLower := i+1 < len(s) && s[i+1] >= 'a' && s[i+1] <= 'z'
				if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') || (prev >= 'A' && prev <= 'Z' && nextLower) {
					b.WriteByte('_')
				}
			}
			b.WriteRune(r + 'a' - 'A')
		case (r >= 'a' && r <= 'z') || r == '_' || (r >= '0' && r <= '9' && i > 0):
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// marshalProto encodes v as a protobuf message. A value that is not a
// struct travels as a message with the single field `value = 1`.
func marshalProto(v any) ([]byte, error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, errors.New("grpcx: cannot encode an untyped nil as protobuf")
	}
	s, err := describe(rv.Type())
	if err != nil {
		return nil, err
	}
	if !s.singular() || s.kind != kMessage {
		return s.appendField(nil, 1, rv, false)
	}
	if s.ptr {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
	}
	return s.appendMessage(nil, rv)
}

// unmarshalProto decodes a protobuf message into the value v points to.
func unmarshalProto(b []byte, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("grpcx: protobuf decodes into a non-nil pointer")
	}
	rv = rv.Elem()
	s, err := describe(rv.Type())
	if err != nil {
		return err
	}
	if !s.singular() || s.kind != kMessage {
		return eachField(b, func(num protowire.Number, wt protowire.Type, b []byte) (int, error) {
			if num != 1 {
				return skip(num, wt, b)
			}
			return s.consume(b, wt, rv)
		})
	}
	if s.ptr {
		if rv.IsNil() {
			rv.Set(reflect.New(s.rt))
		}
		rv = rv.Elem()
	}
	return s.readMessage(b, rv)
}

func (s *shape) appendMessage(b []byte, v reflect.Value) ([]byte, error) {
	var err error
	for i := range s.fields {
		f := &s.fields[i]
		if b, err = f.shape.appendField(b, f.num, v.Field(f.index), false); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// appendField writes v as field num. A zero value is left out, as proto3
// does, unless always is set or the Go value is a non-nil pointer.
func (s *shape) appendField(b []byte, num protowire.Number, v reflect.Value, always bool) ([]byte, error) {
	var err error
	switch {
	case s.dict:
		keys := v.MapKeys()
		slices.SortFunc(keys, compareKeys)
		for _, k := range keys {
			var entry []byte
			if entry, err = s.key.appendField(entry, 1, k, false); err != nil {
				return nil, err
			}
			if entry, err = s.elem.appendField(entry, 2, v.MapIndex(k), false); err != nil {
				return nil, err
			}
			b = protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), entry)
		}
		return b, nil
	case s.list:
		if v.Len() == 0 {
			return b, nil
		}
		if s.elem.packable() {
			var packed []byte
			for i := range v.Len() {
				packed = s.elem.appendScalar(packed, v.Index(i))
			}
			return protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), packed), nil
		}
		for i := range v.Len() {
			e := v.Index(i)
			if s.elem.ptr && e.IsNil() {
				e = reflect.New(s.elem.rt) // protobuf has no null element
			}
			if b, err = s.elem.appendField(b, num, e, true); err != nil {
				return nil, err
			}
		}
		return b, nil
	}
	if s.ptr {
		if v.IsNil() {
			return b, nil
		}
		v, always = v.Elem(), true
	}
	switch s.kind {
	case kMessage, kTimestamp, kDuration:
		if s.kind == kTimestamp && !always && v.Interface().(time.Time).IsZero() {
			return b, nil
		}
		body, err := s.body(v)
		if err != nil {
			return nil, err
		}
		if len(body) == 0 && !always {
			return b, nil
		}
		return protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), body), nil
	case kText:
		text, err := v.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return nil, err
		}
		if len(text) == 0 && !always {
			return b, nil
		}
		return protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), text), nil
	case kString:
		if v.Len() == 0 && !always {
			return b, nil
		}
		return protowire.AppendString(protowire.AppendTag(b, num, protowire.BytesType), v.String()), nil
	case kBytes:
		if !always && (v.IsZero() || v.Len() == 0) {
			return b, nil
		}
		raw := make([]byte, v.Len())
		reflect.Copy(reflect.ValueOf(raw), v)
		return protowire.AppendBytes(protowire.AppendTag(b, num, protowire.BytesType), raw), nil
	}
	if v.IsZero() && !always {
		return b, nil
	}
	return s.appendScalar(protowire.AppendTag(b, num, s.wireType()), v), nil
}

// body encodes a message, a timestamp or a duration without its tag.
func (s *shape) body(v reflect.Value) ([]byte, error) {
	var sec, nanos int64
	switch s.kind {
	case kMessage:
		return s.appendMessage(nil, v)
	case kTimestamp:
		t := v.Interface().(time.Time)
		sec, nanos = t.Unix(), int64(t.Nanosecond())
	case kDuration:
		d := v.Int()
		sec, nanos = d/1e9, d%1e9
	}
	var b []byte
	if sec != 0 {
		b = protowire.AppendVarint(protowire.AppendTag(b, 1, protowire.VarintType), uint64(sec))
	}
	if nanos != 0 {
		b = protowire.AppendVarint(protowire.AppendTag(b, 2, protowire.VarintType), uint64(nanos))
	}
	return b, nil
}

func (s *shape) appendScalar(b []byte, v reflect.Value) []byte {
	switch s.kind {
	case kBool:
		return protowire.AppendVarint(b, protowire.EncodeBool(v.Bool()))
	case kInt64, kInt32:
		return protowire.AppendVarint(b, uint64(v.Int()))
	case kUint64, kUint32:
		return protowire.AppendVarint(b, v.Uint())
	case kDouble:
		return protowire.AppendFixed64(b, math.Float64bits(v.Float()))
	default: // kFloat
		return protowire.AppendFixed32(b, math.Float32bits(float32(v.Float())))
	}
}

func compareKeys(a, b reflect.Value) int {
	switch a.Kind() {
	case reflect.String:
		return strings.Compare(a.String(), b.String())
	case reflect.Bool:
		return int(protowire.EncodeBool(a.Bool())) - int(protowire.EncodeBool(b.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return compare(a.Int(), b.Int())
	}
	return compare(a.Uint(), b.Uint())
}

func compare[T int64 | uint64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// eachField walks the fields of an encoded message.
func eachField(b []byte, f func(protowire.Number, protowire.Type, []byte) (int, error)) error {
	for len(b) > 0 {
		num, wt, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		n, err := f(num, wt, b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func skip(num protowire.Number, wt protowire.Type, b []byte) (int, error) {
	n := protowire.ConsumeFieldValue(num, wt, b)
	if n < 0 {
		return 0, protowire.ParseError(n)
	}
	return n, nil
}

// readMessage decodes b into the struct v. Unknown fields are skipped, so an
// older server accepts a newer client's message.
func (s *shape) readMessage(b []byte, v reflect.Value) error {
	return eachField(b, func(num protowire.Number, wt protowire.Type, b []byte) (int, error) {
		f := s.byNum[num]
		if f == nil {
			return skip(num, wt, b)
		}
		n, err := f.shape.consume(b, wt, v.Field(f.index))
		if err != nil {
			return 0, fmt.Errorf("%s: %w", f.name, err)
		}
		return n, nil
	})
}

// consume reads one occurrence of a field into v.
func (s *shape) consume(b []byte, wt protowire.Type, v reflect.Value) (int, error) {
	switch {
	case s.dict:
		entry, n, err := consumeBytes(b, wt)
		if err != nil {
			return 0, err
		}
		k, e := reflect.New(s.key.goType()).Elem(), reflect.New(s.elem.goType()).Elem()
		err = eachField(entry, func(num protowire.Number, wt protowire.Type, b []byte) (int, error) {
			switch num {
			case 1:
				return s.key.consume(b, wt, k)
			case 2:
				return s.elem.consume(b, wt, e)
			}
			return skip(num, wt, b)
		})
		if err != nil {
			return 0, err
		}
		if s.elem.ptr && e.IsNil() {
			e.Set(reflect.New(s.elem.rt))
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(s.rt))
		}
		v.SetMapIndex(k, e)
		return n, nil
	case s.list:
		if s.elem.packable() && wt == protowire.BytesType {
			packed, n, err := consumeBytes(b, wt)
			if err != nil {
				return 0, err
			}
			for len(packed) > 0 {
				e := reflect.New(s.elem.rt).Elem()
				m, err := s.elem.consume(packed, s.elem.wireType(), e)
				if err != nil {
					return 0, err
				}
				packed = packed[m:]
				v.Set(reflect.Append(v, e))
			}
			return n, nil
		}
		e := reflect.New(s.elem.goType()).Elem()
		n, err := s.elem.consume(b, wt, e)
		if err != nil {
			return 0, err
		}
		v.Set(reflect.Append(v, e))
		return n, nil
	}
	if s.ptr {
		if v.IsNil() {
			v.Set(reflect.New(s.rt))
		}
		v = v.Elem()
	}
	if wt != s.wireType() {
		return 0, fmt.Errorf("wire type %d does not fit a %s", wt, s.rt)
	}
	switch s.wireType() {
	case protowire.VarintType:
		x, n := protowire.ConsumeVarint(b)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		switch s.kind {
		case kBool:
			v.SetBool(protowire.DecodeBool(x))
		case kInt64, kInt32:
			v.SetInt(int64(x))
		default:
			v.SetUint(x)
		}
		return n, nil
	case protowire.Fixed64Type:
		x, n := protowire.ConsumeFixed64(b)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		v.SetFloat(math.Float64frombits(x))
		return n, nil
	case protowire.Fixed32Type:
		x, n := protowire.ConsumeFixed32(b)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		v.SetFloat(float64(math.Float32frombits(x)))
		return n, nil
	}
	body, n, err := consumeBytes(b, wt)
	if err != nil {
		return 0, err
	}
	switch s.kind {
	case kString:
		v.SetString(string(body))
	case kBytes:
		if s.rt.Kind() == reflect.Array {
			if len(body) != s.rt.Len() {
				return 0, fmt.Errorf("%d bytes do not fit a %s", len(body), s.rt)
			}
			reflect.Copy(v, reflect.ValueOf(body))
		} else {
			v.SetBytes(slices.Clone(body))
		}
	case kText:
		err = v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(body)
	case kMessage:
		err = s.readMessage(body, v)
	default: // kTimestamp, kDuration
		var sec, nanos int64
		err = eachField(body, func(num protowire.Number, wt protowire.Type, b []byte) (int, error) {
			if wt != protowire.VarintType || num > 2 {
				return skip(num, wt, b)
			}
			x, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			if num == 1 {
				sec = int64(x)
			} else {
				nanos = int64(int32(x))
			}
			return n, nil
		})
		if s.kind == kTimestamp {
			v.Set(reflect.ValueOf(time.Unix(sec, nanos).UTC()))
		} else {
			v.SetInt(sec*1e9 + nanos)
		}
	}
	return n, err
}

func consumeBytes(b []byte, wt protowire.Type) ([]byte, int, error) {
	if wt != protowire.BytesType {
		return nil, 0, fmt.Errorf("wire type %d where a length-delimited value belongs", wt)
	}
	body, n := protowire.ConsumeBytes(b)
	if n < 0 {
		return nil, 0, protowire.ParseError(n)
	}
	return body, n, nil
}
