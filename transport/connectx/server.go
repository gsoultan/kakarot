package connectx

import (
	"net/http"

	"github.com/gsoultan/kakarot/transport/httpx"
)

// NewServer returns an httpx.Server for h on addr that accepts HTTP/1.1,
// HTTP/2 over TLS and HTTP/2 without TLS (h2c). gRPC clients and
// bidirectional streams need HTTP/2; behind a TLS-terminating proxy, h2c is
// how they reach the service.
func NewServer(addr string, h http.Handler) *httpx.Server {
	s := httpx.NewServer(addr, h)
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetHTTP2(true)
	p.SetUnencryptedHTTP2(true)
	s.HTTP.Protocols = p
	return s
}
