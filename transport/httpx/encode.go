package httpx

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/gsoultan/kakarot"
)

// EncodeFunc writes the endpoint's response.
type EncodeFunc[Resp any] func(ctx context.Context, w http.ResponseWriter, status int, resp Resp) error

// JSONEncoder writes resp as JSON with the given status. A 204 writes no body.
func JSONEncoder[Resp any](_ context.Context, w http.ResponseWriter, status int, resp Resp) error {
	if status == http.StatusNoContent {
		w.WriteHeader(status)
		return nil
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(resp)
}

// ErrorEncoder writes an error. The default is WriteError.
type ErrorEncoder func(ctx context.Context, w http.ResponseWriter, err error)

// StatusOf maps a kakarot error code to an HTTP status. It is the only place
// HTTP status codes meet domain errors.
func StatusOf(c kakarot.Code) int {
	switch c {
	case kakarot.CodeInvalid:
		return http.StatusBadRequest
	case kakarot.CodeNotFound:
		return http.StatusNotFound
	case kakarot.CodeConflict:
		return http.StatusConflict
	case kakarot.CodeUnauthorized:
		return http.StatusUnauthorized
	case kakarot.CodeForbidden:
		return http.StatusForbidden
	case kakarot.CodeUnavailable:
		return http.StatusServiceUnavailable
	case kakarot.CodeTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

type errorBody struct {
	Error struct {
		Code    string            `json:"code"`
		Message string            `json:"message"`
		Fields  map[string]string `json:"fields,omitempty"`
	} `json:"error"`
}

// WriteError writes err as {"error":{"code","message","fields"}}. Internal
// errors are logged with their cause and shown to the caller only as
// "internal error".
func WriteError(ctx context.Context, w http.ResponseWriter, err error) {
	e := kakarot.AsError(err)
	if e.Code == kakarot.CodeInternal {
		slog.ErrorContext(ctx, "internal error", "err", err)
	}
	var body errorBody
	body.Error.Code = e.Code.String()
	body.Error.Message = e.Message
	body.Error.Fields = e.Fields
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(StatusOf(e.Code))
	_ = json.NewEncoder(w).Encode(body)
}
