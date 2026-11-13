// Package otelx adds OpenTelemetry tracing and metrics to kakarot.
//
//	mws := kakarot.Chain(otelx.Middleware(), middleware.Default(log, 5*time.Second))
//	r.Use(otelx.HTTP()) // continue traces started by callers
//
// Middleware works for every transport: HTTP, gRPC, WebSocket and queue
// consumers all produce the same spans and metrics, named by operation.
// It uses the global providers unless WithTracerProvider or
// WithMeterProvider is given; with no SDK installed it costs almost nothing.
package otelx

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/gsoultan/kakarot"
)

const scope = "github.com/gsoultan/kakarot/otelx"

type config struct {
	tp trace.TracerProvider
	mp metric.MeterProvider
}

// Option configures Middleware.
type Option func(*config)

// WithTracerProvider uses tp instead of the global tracer provider.
func WithTracerProvider(tp trace.TracerProvider) Option { return func(c *config) { c.tp = tp } }

// WithMeterProvider uses mp instead of the global meter provider.
func WithMeterProvider(mp metric.MeterProvider) Option { return func(c *config) { c.mp = mp } }

// Middleware records one span and one duration measurement per call.
//
// Span: named "<service>.<method>", kind server (consumer for queue
// handlers), with attributes rpc.service, rpc.method, kakarot.transport and,
// on failure, kakarot.code. Only server-side faults (internal, unavailable,
// timeout) mark the span as an error; a not_found or invalid answer is a
// correct response to a bad request.
//
// Metric: histogram kakarot.call.duration in seconds, with the same
// attributes plus kakarot.code ("ok" on success).
//
// Request and response values are never recorded.
func Middleware(opts ...Option) kakarot.Middleware {
	var c config
	for _, o := range opts {
		o(&c)
	}
	if c.tp == nil {
		c.tp = otel.GetTracerProvider()
	}
	if c.mp == nil {
		c.mp = otel.GetMeterProvider()
	}
	tracer := c.tp.Tracer(scope)
	// An instrument that cannot be created is replaced by a no-op by the
	// API, so the error needs no handling beyond OpenTelemetry's own.
	duration, _ := c.mp.Meter(scope).Float64Histogram("kakarot.call.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of kakarot operations."))

	return func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		attrs := []attribute.KeyValue{
			attribute.String("rpc.service", op.Service),
			attribute.String("rpc.method", op.Method),
			attribute.String("kakarot.transport", op.Transport),
		}
		kind := trace.SpanKindServer
		if op.Transport == "queue" {
			kind = trace.SpanKindConsumer
		}
		ctx, span := tracer.Start(ctx, op.String(), trace.WithSpanKind(kind), trace.WithAttributes(attrs...))
		start := time.Now()
		err := next(ctx)

		code := "ok"
		if err != nil {
			e := kakarot.AsError(err)
			code = e.Code.String()
			span.SetAttributes(attribute.String("kakarot.code", code))
			switch e.Code {
			case kakarot.CodeInternal, kakarot.CodeUnavailable, kakarot.CodeTimeout:
				span.RecordError(err)
				span.SetStatus(codes.Error, e.Message)
			}
		}
		span.End()
		duration.Record(ctx, time.Since(start).Seconds(),
			metric.WithAttributes(append(attrs, attribute.String("kakarot.code", code))...))
		return err
	}
}

// HTTP is transport middleware that continues a trace started by the caller
// (W3C traceparent by default), using the global propagator. Put it before
// routes are served: r.Use(otelx.HTTP()).
func HTTP() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// Inject writes the trace context of ctx into outgoing HTTP headers, for
// calls this service makes to others.
func Inject(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}
