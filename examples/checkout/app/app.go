// Package app wires the checkout: it is the only place that knows every
// layer. main calls Build with a real database; tests call it the same way.
package app

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gsoultan/kakarot"
	kdata "github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/kakarot/data/idempotency"
	idemmodel "github.com/gsoultan/kakarot/data/idempotency/model"
	"github.com/gsoultan/kakarot/data/outbox"
	outboxmodel "github.com/gsoultan/kakarot/data/outbox/model"
	"github.com/gsoultan/kakarot/examples/checkout/data"
	"github.com/gsoultan/kakarot/examples/checkout/data/model"
	"github.com/gsoultan/kakarot/examples/checkout/domain"
	"github.com/gsoultan/kakarot/examples/checkout/endpoint"
	"github.com/gsoultan/kakarot/examples/checkout/service"
	"github.com/gsoultan/kakarot/examples/checkout/transport"
	"github.com/gsoultan/kakarot/examples/checkout/usecase"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/transport/httpx"
	"github.com/gsoultan/storm"
)

// Models lists every table the checkout needs, for storm.Build / storm diff.
func Models() []any {
	return []any{
		&model.Product{}, &model.Order{}, &model.OrderLine{}, &model.OrderStep{},
		&outboxmodel.OutboxEvent{}, &idemmodel.IdempotencyRecord{},
	}
}

// Config is what the checkout needs from its environment.
type Config struct {
	DB        storm.DB
	Logger    *slog.Logger
	Payments  usecase.Payments
	Warehouse usecase.Warehouse
	Notifier  usecase.Notifier
	// PaymentWindow is how long an order may stay pending before the expiry
	// job cancels it. Default 15 minutes.
	PaymentWindow time.Duration
	// RelayInterval is how often the outbox is polled. Default 200ms.
	RelayInterval time.Duration
}

// Checkout is the assembled service.
type Checkout struct {
	HTTP     *httpx.Router
	Consumer *queue.Consumer
	Relay    *outbox.Relay
	Expiry   *Job
}

// Servers returns everything kakarot.App should run: the HTTP server for the
// synchronous side, and the consumer, relay and expiry job for the
// asynchronous side.
func (c *Checkout) Servers(httpAddr string) []kakarot.Server {
	return []kakarot.Server{httpx.NewServer(httpAddr, c.HTTP), c.Consumer, c.Relay, c.Expiry}
}

func init() {
	httpx.RegisterStatus(domain.CodeOutOfStock, http.StatusConflict)
	httpx.RegisterStatus(domain.CodeDeclined, http.StatusPaymentRequired)
}

// Build wires repositories, usecases, the service and both transports.
func Build(c Config) *Checkout {
	log := c.Logger
	if c.PaymentWindow <= 0 {
		c.PaymentWindow = 15 * time.Minute
	}
	if c.RelayInterval <= 0 {
		c.RelayInterval = 200 * time.Millisecond
	}
	tx := kdata.NewTxManager(c.DB)
	catalog := &data.CatalogRepo{DB: c.DB}
	orders := &data.OrderRepo{DB: c.DB}
	events := &data.Events{DB: c.DB}

	svc := service.New(service.Usecases{
		AddProduct:  &usecase.AddProduct{Catalog: catalog},
		GetProduct:  &usecase.GetProduct{Catalog: catalog},
		PlaceOrder:  &usecase.PlaceOrder{Catalog: catalog, Orders: orders, Events: events, Tx: tx},
		GetOrder:    &usecase.GetOrder{Orders: orders},
		CancelOrder: &usecase.CancelOrder{Catalog: catalog, Orders: orders, Events: events, Tx: tx},
		Charge:      &usecase.ChargePayment{Catalog: catalog, Orders: orders, Events: events, Payments: c.Payments, Tx: tx},
		Ship:        &usecase.ShipOrder{Orders: orders, Events: events, Warehouse: c.Warehouse, Tx: tx},
		Refund:      &usecase.RefundPayment{Orders: orders, Events: events, Payments: c.Payments, Tx: tx},
		Notify:      &usecase.NotifyCustomer{Notifier: c.Notifier},
	})
	eps := endpoint.MakeCheckoutEndpoints(svc)

	// Synchronous side. A retried POST /orders with the same Idempotency-Key
	// returns the first order instead of reserving stock twice.
	eps.PlaceOrder = kakarot.Decorate(kakarot.Op{Service: "checkout", Method: "PlaceOrder"}, eps.PlaceOrder,
		middleware.Idempotent[domain.PlaceOrder](&idempotency.Store[domain.Order]{DB: c.DB}))

	r := httpx.NewRouter()
	r.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	transport.RegisterCheckoutHTTP(r, eps, middleware.LoadShed(256), middleware.Default(log, 5*time.Second))
	r.ServeOpenAPI("GET /openapi.json", httpx.Info{Title: "checkout", Version: "0.1.0"})

	// Asynchronous side: outbox row -> relay -> broker -> handler.
	//
	// A short provider blip is retried inside the handler; a longer outage
	// fails the message, and the relay delivers it again on a later poll.
	blips := middleware.Retry(middleware.RetryPolicy{Attempts: 3, BaseDelay: 50 * time.Millisecond})
	eps.ChargePayment = kakarot.Wrap(kakarot.Op{Service: "checkout", Method: "ChargePayment"}, eps.ChargePayment, blips)
	eps.RefundPayment = kakarot.Wrap(kakarot.Op{Service: "checkout", Method: "RefundPayment"}, eps.RefundPayment, blips)

	// queue.Memory keeps everything in one process; swap in a broker client
	// (anything with Subscribe and Publish) to run consumers elsewhere.
	broker := &queue.Memory{}
	consumer := &queue.Consumer{Service: "checkout", Source: broker, Logger: log}
	transport.RegisterCheckoutQueue(consumer, eps, middleware.Default(log, 30*time.Second))
	relay := &outbox.Relay{DB: c.DB, Logger: log, Publisher: outbox.ToQueue(broker), Interval: c.RelayInterval}

	expire := &usecase.ExpireOrders{Catalog: catalog, Orders: orders, Events: events, Tx: tx, After: c.PaymentWindow}
	job := &Job{
		Name: "expire-orders", Every: min(c.PaymentWindow/4, time.Minute), Logger: log,
		Run: func(ctx context.Context) error {
			n, err := expire.Run(ctx)
			if n > 0 {
				log.InfoContext(ctx, "expired orders", "count", n)
			}
			return err
		},
	}
	return &Checkout{HTTP: r, Consumer: consumer, Relay: relay, Expiry: job}
}

// Job runs a function on a fixed interval. It is a kakarot.Server, so it
// starts and stops with the application.
type Job struct {
	Name   string
	Every  time.Duration
	Run    func(ctx context.Context) error
	Logger *slog.Logger

	mu   sync.Mutex
	stop chan struct{}
	done chan struct{}
}

func (j *Job) channels() (stop, done chan struct{}) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.stop == nil {
		j.stop, j.done = make(chan struct{}), make(chan struct{})
	}
	return j.stop, j.done
}

func (j *Job) Serve(ctx context.Context) error {
	stop, done := j.channels()
	defer close(done)
	t := time.NewTicker(j.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stop:
			return nil
		case <-t.C:
			if err := j.Run(ctx); err != nil {
				j.Logger.ErrorContext(ctx, "job failed", "job", j.Name, "err", err)
			}
		}
	}
}

// Shutdown stops the job after its current run. Call it once.
func (j *Job) Shutdown(ctx context.Context) error {
	stop, done := j.channels()
	close(stop)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
