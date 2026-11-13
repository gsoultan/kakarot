package kakarot_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/kakarot/transport/httpx"
)

type Greet struct {
	Name string `path:"name"`
}

func (g Greet) Validate() error {
	if len(g.Name) > 20 {
		return kakarot.Invalid("invalid greeting", map[string]string{"name": "at most 20 letters"})
	}
	return nil
}

type Greeting struct {
	Text string `json:"text"`
}

// A service method already has the endpoint shape, so it can be bound to a
// transport directly.
func Example() {
	hello := func(_ context.Context, in Greet) (Greeting, error) {
		return Greeting{Text: "hello " + in.Name}, nil
	}

	r := httpx.NewRouter()
	r.Handle("GET /hello/{name}", middleware.Validated(hello))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/hello/ana", nil))
	fmt.Println(w.Code, strings.TrimSpace(w.Body.String()))

	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/hello/"+strings.Repeat("a", 21), nil))
	fmt.Println(w.Code, strings.TrimSpace(w.Body.String()))
	// Output:
	// 200 {"text":"hello ana"}
	// 400 {"error":{"code":"invalid","message":"invalid greeting","fields":{"name":"at most 20 letters"}}}
}

// Shared middleware wraps any endpoint without seeing its request type.
func ExampleWrap() {
	trace := func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		fmt.Println("before", op.Service+"."+op.Method)
		err := next(ctx)
		fmt.Println("after, failed:", err != nil)
		return err
	}
	hello := func(_ context.Context, in Greet) (Greeting, error) {
		return Greeting{Text: "hello " + in.Name}, nil
	}
	e := kakarot.Wrap(kakarot.Op{Service: "demo", Method: "Hello"}, hello, trace)

	out, _ := e(context.Background(), Greet{Name: "ana"})
	fmt.Println(out.Text)
	// Output:
	// before demo.Hello
	// after, failed: false
	// hello ana
}

// Errors carry a code; each transport maps the code to its own status.
func ExampleIs() {
	var CodeQuotaExceeded = kakarot.NewCode("example_quota_exceeded")
	httpx.RegisterStatus(CodeQuotaExceeded, http.StatusTooManyRequests)

	err := fmt.Errorf("upload: %w", kakarot.E(CodeQuotaExceeded, "monthly quota used up", nil))
	fmt.Println(kakarot.Is(err, CodeQuotaExceeded), kakarot.Is(nil, CodeQuotaExceeded))
	fmt.Println(kakarot.CodeOf(err), httpx.StatusOf(kakarot.CodeOf(err)))
	// Output:
	// true false
	// example_quota_exceeded 429
}
