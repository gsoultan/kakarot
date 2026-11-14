package otelx_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/kakarottest"
	"github.com/gsoultan/kakarot/otelx"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/transport/httpx"
)

func setup(t *testing.T) (kakarot.Middleware, *tracetest.SpanRecorder, *sdkmetric.ManualReader, *sdktrace.TracerProvider) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return otelx.Middleware(otelx.WithTracerProvider(tp), otelx.WithMeterProvider(mp)), rec, reader, tp
}

func attr(kvs []attribute.KeyValue, key string) string {
	for _, kv := range kvs {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

func TestSpanAndMetricPerCall(t *testing.T) {
	mw, rec, reader, _ := setup(t)
	op := kakarot.Op{Service: "order", Method: "Get", Transport: "http"}
	results := []error{nil, kakarot.NotFound("order", "1"), errors.New("db down")}
	i := 0
	e := kakarot.Wrap(op, func(context.Context, int) (int, error) { i++; return 0, results[i-1] }, mw)
	for range results {
		_, _ = e(t.Context(), 0)
	}

	spans := rec.Ended()
	if len(spans) != 3 {
		t.Fatalf("spans = %d", len(spans))
	}
	if spans[0].Name() != "order.Get" || spans[0].SpanKind() != trace.SpanKindServer || attr(spans[0].Attributes(), "kakarot.transport") != "http" {
		t.Fatalf("span 0 = %s %v %v", spans[0].Name(), spans[0].SpanKind(), spans[0].Attributes())
	}
	if spans[0].Status().Code != codes.Unset {
		t.Fatal("success marked as error")
	}
	if spans[1].Status().Code == codes.Error || attr(spans[1].Attributes(), "kakarot.code") != "not_found" {
		t.Fatalf("a not_found answer is not a server fault: %v %v", spans[1].Status(), spans[1].Attributes())
	}
	if spans[2].Status().Code != codes.Error || spans[2].Status().Description != "internal error" || len(spans[2].Events()) != 1 {
		t.Fatalf("internal error: %v events=%d", spans[2].Status(), len(spans[2].Events()))
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	h := rm.ScopeMetrics[0].Metrics[0]
	hist, ok := h.Data.(metricdata.Histogram[float64])
	if h.Name != "kakarot.call.duration" || h.Unit != "s" || !ok || len(hist.DataPoints) != 3 {
		t.Fatalf("metric = %+v", h)
	}
	codesSeen := map[string]uint64{}
	for _, dp := range hist.DataPoints {
		v, _ := dp.Attributes.Value("kakarot.code")
		codesSeen[v.AsString()] += dp.Count
	}
	if codesSeen["ok"] != 1 || codesSeen["not_found"] != 1 || codesSeen["internal"] != 1 {
		t.Fatalf("codes = %v", codesSeen)
	}
}

func TestQueueHandlersAreConsumerSpans(t *testing.T) {
	mw, rec, _, _ := setup(t)
	e := kakarot.Wrap(kakarot.Op{Service: "billing", Method: "Charge", Transport: "queue"},
		func(context.Context, int) (int, error) { return 0, nil }, mw)
	_, _ = e(t.Context(), 0)
	if k := rec.Ended()[0].SpanKind(); k != trace.SpanKindConsumer {
		t.Fatalf("kind = %v", k)
	}
}

func TestHTTPContinuesCallerTrace(t *testing.T) {
	mw, rec, _, tp := setup(t)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	r := httpx.NewRouter()
	r.Use(otelx.HTTP())
	r.Group("order", mw).Handle("GET /orders", func(context.Context, struct{}) (string, error) { return "ok", nil })

	// The caller's span, sent as a traceparent header.
	ctx, parent := tp.Tracer("caller").Start(t.Context(), "caller")
	req := httptest.NewRequest("GET", "/orders", nil)
	otelx.Inject(ctx, req.Header)
	parent.End()
	if req.Header.Get("traceparent") == "" {
		t.Fatal("Inject wrote no traceparent")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body)
	}
	var server sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "order.GET /orders" {
			server = s
		}
	}
	if server == nil {
		t.Fatalf("no server span among %d", len(rec.Ended()))
	}
	if server.Parent().SpanID() != parent.SpanContext().SpanID() || server.SpanContext().TraceID() != parent.SpanContext().TraceID() {
		t.Fatal("server span is not a child of the caller's span")
	}
}

// A trace crosses any transport as plain headers: gRPC metadata or the
// headers of a queue message.
func TestHeadersCarryTheTraceToAQueueHandler(t *testing.T) {
	mw, rec, _, tp := setup(t)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	const topic queue.Topic[string] = "greeted"
	broker := &queue.Memory{}
	c := &queue.Consumer{Service: "mail", Source: broker, Extract: otelx.WithHeaders, Middleware: []kakarot.Middleware{mw}}
	queue.Handle(c, topic, "Send", func(context.Context, string) (string, error) { return "ok", nil })
	kakarottest.Serve(t, c)
	kakarottest.Eventually(t, time.Second, "subscribed", func() bool { return broker.Subscribers("greeted") > 0 })

	// The publisher's span travels in the message headers.
	ctx, parent := tp.Tracer("publisher").Start(t.Context(), "publish")
	m, err := topic.Encode("k", "hello")
	if err != nil {
		t.Fatal(err)
	}
	m.Headers = otelx.Headers(ctx)
	parent.End()
	if m.Headers["traceparent"] == "" {
		t.Fatal("Headers wrote no traceparent")
	}
	if err := broker.Publish(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	var handler sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "mail.Send" {
			handler = s
		}
	}
	if handler == nil {
		t.Fatalf("no handler span among %d", len(rec.Ended()))
	}
	if handler.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatal("the handler's span is not a child of the publisher's span")
	}
}
