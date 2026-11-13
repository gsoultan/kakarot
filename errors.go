package kakarot

import (
	"errors"
	"fmt"
	"sync"
)

// Code classifies an error independently of any transport. Each transport
// maps codes to its own status values exactly once.
type Code uint8

const (
	CodeInternal Code = iota
	CodeInvalid
	CodeNotFound
	CodeConflict
	CodeUnauthorized
	CodeForbidden
	CodeUnavailable
	CodeTimeout
)

var codeNames = []string{
	CodeInternal:     "internal",
	CodeInvalid:      "invalid",
	CodeNotFound:     "not_found",
	CodeConflict:     "conflict",
	CodeUnauthorized: "unauthorized",
	CodeForbidden:    "forbidden",
	CodeUnavailable:  "unavailable",
	CodeTimeout:      "timeout",
}

// NewCode registers an application-specific code, such as
// "quota_exceeded". Call it from a package-level var so codes are fixed at
// startup; transports map it with their own registry (httpx.RegisterStatus).
//
//	var CodeQuotaExceeded = kakarot.NewCode("quota_exceeded")
func NewCode(name string) Code {
	codeMu.Lock()
	defer codeMu.Unlock()
	if len(codeNames) > 255 {
		panic("kakarot: too many error codes")
	}
	codeNames = append(codeNames, name)
	return Code(len(codeNames) - 1)
}

var codeMu sync.Mutex

// LookupCode finds a code by its name, built in or registered with NewCode.
// Transports use it to restore a code that crossed the wire as text.
func LookupCode(name string) (Code, bool) {
	codeMu.Lock()
	defer codeMu.Unlock()
	for i, n := range codeNames {
		if n == name {
			return Code(i), true
		}
	}
	return CodeInternal, false
}

func (c Code) String() string {
	codeMu.Lock()
	defer codeMu.Unlock()
	if int(c) < len(codeNames) {
		return codeNames[c]
	}
	return fmt.Sprintf("code(%d)", c)
}

// Error is a domain error: a code, a message safe to show callers, and an
// optional cause kept for logs.
type Error struct {
	Code    Code
	Message string
	Fields  map[string]string // per-field problems, for CodeInvalid
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return e.Code.String() + ": " + e.Message + ": " + e.Cause.Error()
	}
	return e.Code.String() + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Cause }

// E builds an Error.
func E(code Code, msg string, cause error) *Error {
	return &Error{Code: code, Message: msg, Cause: cause}
}

// NotFound reports that the named thing does not exist.
func NotFound(kind, id string) *Error {
	return &Error{Code: CodeNotFound, Message: kind + " " + id + " not found"}
}

// Invalid reports a bad input, optionally per field.
func Invalid(msg string, fields map[string]string) *Error {
	return &Error{Code: CodeInvalid, Message: msg, Fields: fields}
}

// Conflict reports a clash with existing state (duplicate key, stale version).
func Conflict(msg string, cause error) *Error {
	return &Error{Code: CodeConflict, Message: msg, Cause: cause}
}

// AsError returns err as an *Error. Errors that are not domain errors become
// CodeInternal with a generic message, so internals never leak to callers.
func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*Error](err); ok {
		return e
	}
	return &Error{Code: CodeInternal, Message: "internal error", Cause: err}
}

// CodeOf returns the code of err, CodeInternal for non-domain errors. There
// is no code for success: check err != nil first, or use Is.
func CodeOf(err error) Code {
	if err == nil {
		return CodeInternal
	}
	return AsError(err).Code
}

// Is reports whether err is an error with code c. It is false for nil, so
// it is safe to call on a result that may have succeeded:
//
//	id, err := gateway.Charge(ctx, order)
//	declined := kakarot.Is(err, CodeDeclined)
func Is(err error, c Code) bool { return err != nil && AsError(err).Code == c }
