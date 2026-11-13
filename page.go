package kakarot

import (
	"encoding/base64"
	"encoding/json"
)

// Page is one page of a keyset-paginated list. Next is empty on the last
// page; pass it back as the cursor to read the following page.
type Page[T any] struct {
	Items []T    `json:"items"`
	Next  string `json:"next,omitempty"`
}

// PageRequest is embedded in list requests:
//
//	type ListOrders struct {
//	    CustomerID string `query:"customer_id"`
//	    kakarot.PageRequest
//	}
type PageRequest struct {
	Cursor string `query:"cursor"`
	Limit  int    `query:"limit"`
}

// Size returns the page size clamped to [1, max], defaulting to def.
func (p PageRequest) Size(def, max int) int {
	switch {
	case p.Limit <= 0:
		return def
	case p.Limit > max:
		return max
	}
	return p.Limit
}

// EncodeCursor turns a typed position (usually the last row's sort key) into
// an opaque cursor string. Cursors are encoded, not signed or encrypted: a
// client can read and forge one. Use a cursor only as a position inside a
// query that is already filtered to what the caller may see, never as proof
// of access.
func EncodeCursor[C any](c C) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// DecodeCursor reads a cursor made by EncodeCursor. An empty cursor returns
// ok=false (first page); a malformed one is CodeInvalid.
func DecodeCursor[C any](s string) (c C, ok bool, err error) {
	if s == "" {
		return c, false, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	if err != nil {
		return c, false, Invalid("malformed cursor", map[string]string{"cursor": "malformed"})
	}
	return c, true, nil
}

// NextPage builds a Page from rows fetched with limit+1: the extra row only
// signals that another page exists. cursorOf extracts the position of the
// last item on this page.
func NextPage[Row, T, C any](rows []Row, limit int, toItem func(Row) T, cursorOf func(Row) C) (Page[T], error) {
	more := len(rows) > limit
	if more {
		rows = rows[:limit]
	}
	p := Page[T]{Items: make([]T, 0, len(rows))}
	for _, r := range rows {
		p.Items = append(p.Items, toItem(r))
	}
	if more && len(rows) > 0 {
		next, err := EncodeCursor(cursorOf(rows[len(rows)-1]))
		if err != nil {
			return p, err
		}
		p.Next = next
	}
	return p, nil
}
