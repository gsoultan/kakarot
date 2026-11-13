// Package app wires the wallet: it is the only place that knows every layer.
// main calls Build with a real database; tests call it the same way.
package app

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/auth"
	kdata "github.com/gsoultan/kakarot/data"
	"github.com/gsoultan/kakarot/data/idempotency"
	idemmodel "github.com/gsoultan/kakarot/data/idempotency/model"
	"github.com/gsoultan/kakarot/data/outbox"
	outboxmodel "github.com/gsoultan/kakarot/data/outbox/model"
	"github.com/gsoultan/kakarot/examples/wallet/data"
	"github.com/gsoultan/kakarot/examples/wallet/data/model"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
	"github.com/gsoultan/kakarot/examples/wallet/endpoint"
	"github.com/gsoultan/kakarot/examples/wallet/service"
	"github.com/gsoultan/kakarot/examples/wallet/transport"
	"github.com/gsoultan/kakarot/examples/wallet/usecase"
	"github.com/gsoultan/kakarot/middleware"
	"github.com/gsoultan/kakarot/queue"
	"github.com/gsoultan/kakarot/transport/grpcx"
	"github.com/gsoultan/kakarot/transport/httpx"
	"github.com/gsoultan/storm"
)

// Models lists every table the wallet needs, for storm.Build / storm diff.
func Models() []any {
	return []any{&model.Account{}, &model.Entry{}, &outboxmodel.OutboxEvent{}, &idemmodel.IdempotencyRecord{}}
}

// Config is what the wallet needs from its environment.
type Config struct {
	DB     storm.DB
	Logger *slog.Logger
	// Tokens maps bearer tokens to user ids. It stands in for a real
	// identity provider: replace Verifier with JWT or session validation.
	Tokens map[string]string
	Audit  usecase.Audit
}

// Wallet is the assembled service.
type Wallet struct {
	HTTP     *httpx.Router
	GRPC     *grpc.Server
	Consumer *queue.Consumer
	Relay    *outbox.Relay
}

// Servers returns everything kakarot.App should run.
func (w *Wallet) Servers(httpAddr, grpcAddr string) []kakarot.Server {
	return []kakarot.Server{
		httpx.NewServer(httpAddr, w.HTTP),
		&grpcx.Server{GRPC: w.GRPC, Addr: grpcAddr},
		w.Consumer,
		w.Relay,
	}
}

func init() {
	// The wallet's own error code gets its own status on each transport.
	httpx.RegisterStatus(domain.CodeInsufficientFunds, http.StatusUnprocessableEntity)
	grpcx.RegisterCode(domain.CodeInsufficientFunds, codes.FailedPrecondition)
}

// Build wires repositories, usecases, the service, and all three transports.
func Build(c Config) *Wallet {
	log := c.Logger
	tx := kdata.NewTxManager(c.DB)
	accounts := &data.AccountRepo{DB: c.DB}
	deposit := &usecase.Deposit{Accounts: accounts, Tx: tx}
	svc := service.New(service.Usecases{
		Open:    &usecase.OpenAccount{Accounts: accounts},
		Get:     &usecase.GetAccount{Accounts: accounts},
		Deposit: deposit,
		Move:    &usecase.Transfer{Accounts: accounts, Events: &data.Events{DB: c.DB}, Tx: tx},
		Entries: &usecase.ListEntries{Accounts: accounts},
		Import:  (&usecase.ImportDeposits{Deposit: deposit}).Stream,
		Audit:   &usecase.AuditTransfer{Audit: c.Audit},
	})

	// One set of typed endpoints for every transport.
	eps := endpoint.New(svc)
	// A retried transfer with the same Idempotency-Key must not move money
	// twice, whichever instance or transport receives the retry. Keys are
	// scoped to the caller, so a guessed key replays nothing.
	eps.Transfer = kakarot.Decorate(kakarot.Op{Service: "wallet", Method: "Transfer"}, eps.Transfer,
		middleware.IdempotentFor[domain.Transfer](&idempotency.Store[domain.TransferDone]{DB: c.DB},
			func(ctx context.Context) string {
				u, _ := auth.From[domain.User](ctx)
				return u.ID
			}))

	verify := Verifier(c.Tokens)
	// Shared by HTTP and gRPC: shed load, then recover/log/time out, then
	// demand an authenticated user before any usecase runs.
	api := kakarot.Chain(
		middleware.LoadShed(256),
		middleware.Default(log, 5*time.Second),
		auth.Require[domain.User](nil),
	)

	r := httpx.NewRouter()
	r.Use(auth.Bearer(verify))
	r.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	transport.HTTP(r, eps, api)
	r.ServeOpenAPI("GET /openapi.json", httpx.Info{Title: "wallet", Version: "0.1.0"})

	unary, stream := grpcx.Bearer(verify)
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
	transport.GRPC(gs, eps, api)

	// transfer.completed: outbox row -> relay -> broker -> AuditTransfer.
	// queue.Memory keeps it in one process; swap in a broker client later.
	broker := &queue.Memory{}
	consumer := &queue.Consumer{Service: "wallet", Source: broker, Logger: log}
	transport.Queue(consumer, eps, middleware.Default(log, 30*time.Second))
	relay := &outbox.Relay{DB: c.DB, Logger: log, Publisher: outbox.ToQueue(broker), Interval: 200 * time.Millisecond}

	return &Wallet{HTTP: r, GRPC: gs, Consumer: consumer, Relay: relay}
}

// Verifier checks a bearer token against the configured ones in constant
// time and returns the typed principal.
func Verifier(tokens map[string]string) auth.Verifier[domain.User] {
	return func(_ context.Context, token string) (domain.User, error) {
		var user string
		for known, id := range tokens {
			if subtle.ConstantTimeCompare([]byte(known), []byte(token)) == 1 {
				user = id
			}
		}
		if user == "" {
			return domain.User{}, kakarot.E(kakarot.CodeUnauthorized, "invalid token", nil)
		}
		return domain.User{ID: user}, nil
	}
}

// ParseTokens reads "token=user,token=user".
func ParseTokens(s string) map[string]string {
	out := map[string]string{}
	for pair := range strings.SplitSeq(s, ",") {
		if token, user, ok := strings.Cut(strings.TrimSpace(pair), "="); ok && token != "" && user != "" {
			out[token] = user
		}
	}
	return out
}

// LogAudit is an Audit that writes to the log.
type LogAudit struct{ Log *slog.Logger }

func (a LogAudit) Record(ctx context.Context, t domain.TransferDone) error {
	a.Log.InfoContext(ctx, "audit transfer", "id", t.ID, "from", t.From, "to", t.To, "amount", t.Amount)
	return nil
}
