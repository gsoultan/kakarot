package kakarot

import (
	"errors"
	"fmt"
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

var codeNames = [...]string{
	CodeInternal:     "internal",
	CodeInvalid:      "invalid",
	CodeNotFound:     "not_found",
	CodeConflict:     "conflict",
	CodeUnauthorized: "unauthorized",
	CodeForbidden:    "forbidden",
	CodeUnavailable:  "unavailable",
	CodeTimeout:      "timeout",
}

func (c Code) String() string {
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

// CodeOf returns the code of err, CodeInternal for non-domain errors.
func CodeOf(err error) Code { return AsError(err).Code }
