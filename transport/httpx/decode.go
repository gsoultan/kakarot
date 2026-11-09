package httpx

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"

	"github.com/gsoultan/kakarot"
)

// DecodeFunc turns an HTTP request into the endpoint's request type.
type DecodeFunc[Req any] func(r *http.Request) (Req, error)

// MaxBodyBytes caps the JSON body the default decoder reads.
const MaxBodyBytes = 4 << 20

type source uint8

const (
	fromPath source = iota
	fromQuery
	fromHeader
)

type fieldPlan struct {
	index []int
	src   source
	name  string
	slice bool // query only: repeated values
}

// DefaultDecoder builds a decoder for Req from struct tags, once:
//
//	type GetOrder struct {
//	    ID     string `path:"id"`
//	    Expand bool   `query:"expand"`
//	    Tenant string `header:"X-Tenant"`
//	}
//
// A JSON body, when present, is decoded first; path, query and header values
// then override it. The struct is inspected with reflection once, when the
// route is registered, not per request.
func DefaultDecoder[Req any]() DecodeFunc[Req] {
	t := reflect.TypeFor[Req]()
	if t.Kind() != reflect.Struct {
		return func(r *http.Request) (Req, error) {
			var req Req
			return req, decodeJSON(r, &req)
		}
	}
	plan, err := buildPlan(t)
	if err != nil {
		panic(err) // a programming error, caught at route registration
	}
	return func(r *http.Request) (Req, error) {
		var req Req
		if err := decodeJSON(r, &req); err != nil {
			return req, err
		}
		v := reflect.ValueOf(&req).Elem()
		fields := map[string]string{}
		for _, p := range plan {
			f := v.FieldByIndex(p.index)
			var err error
			switch p.src {
			case fromPath:
				if s := r.PathValue(p.name); s != "" {
					err = setString(f, s)
				}
			case fromHeader:
				if s := r.Header.Get(p.name); s != "" {
					err = setString(f, s)
				}
			case fromQuery:
				vals, ok := r.URL.Query()[p.name]
				if !ok {
					continue
				}
				if p.slice {
					err = setSlice(f, vals)
				} else if len(vals) > 0 {
					err = setString(f, vals[0])
				}
			}
			if err != nil {
				fields[p.name] = err.Error()
			}
		}
		if len(fields) > 0 {
			return req, kakarot.Invalid("invalid request parameters", fields)
		}
		return req, nil
	}
}

func decodeJSON(r *http.Request, dst any) error {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return nil
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, MaxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil && !errors.Is(err, io.EOF) {
		return kakarot.E(kakarot.CodeInvalid, "malformed JSON body", err)
	}
	return nil
}

var textUnmarshaler = reflect.TypeFor[encoding.TextUnmarshaler]()

func buildPlan(t reflect.Type) ([]fieldPlan, error) {
	var plan []fieldPlan
	for _, sf := range reflect.VisibleFields(t) {
		if !sf.IsExported() {
			continue
		}
		for _, tag := range []struct {
			key string
			src source
		}{{"path", fromPath}, {"query", fromQuery}, {"header", fromHeader}} {
			name, ok := sf.Tag.Lookup(tag.key)
			if !ok || name == "-" {
				continue
			}
			p := fieldPlan{index: sf.Index, src: tag.src, name: name}
			ft := sf.Type
			if tag.src == fromQuery && ft.Kind() == reflect.Slice && !reflect.PointerTo(ft).Implements(textUnmarshaler) {
				p.slice = true
				ft = ft.Elem()
			}
			if !settable(ft) {
				return nil, fmt.Errorf("httpx: %s.%s: unsupported type %s for %s tag", t, sf.Name, sf.Type, tag.key)
			}
			plan = append(plan, p)
		}
	}
	return plan, nil
}

func settable(t reflect.Type) bool {
	if reflect.PointerTo(t).Implements(textUnmarshaler) {
		return true
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func setSlice(f reflect.Value, vals []string) error {
	s := reflect.MakeSlice(f.Type(), len(vals), len(vals))
	for i, v := range vals {
		if err := setString(s.Index(i), v); err != nil {
			return err
		}
	}
	f.Set(s)
	return nil
}

func setString(f reflect.Value, s string) error {
	if u, ok := f.Addr().Interface().(encoding.TextUnmarshaler); ok {
		return u.UnmarshalText([]byte(s))
	}
	switch f.Kind() {
	case reflect.String:
		f.SetString(s)
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return errors.New("must be true or false")
		}
		f.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, f.Type().Bits())
		if err != nil {
			return errors.New("must be an integer")
		}
		f.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, f.Type().Bits())
		if err != nil {
			return errors.New("must be a non-negative integer")
		}
		f.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(s, f.Type().Bits())
		if err != nil {
			return errors.New("must be a number")
		}
		f.SetFloat(n)
	}
	return nil
}
