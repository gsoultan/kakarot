package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/wallet/app"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
	"github.com/gsoultan/kakarot/examples/wallet/transport"
	"github.com/gsoultan/kakarot/transport/grpcx"
	"github.com/gsoultan/storm"
	"github.com/gsoultan/storm/migrate"
	"github.com/gsoultan/storm/runtime/pgxdrv"
)

type audit struct {
	mu   sync.Mutex
	seen []string
}

func (a *audit) Record(_ context.Context, t domain.TransferDone) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = append(a.seen, t.ID)
	return nil
}

func (a *audit) ids() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string{}, a.seen...)
}

// stack is the real wallet against a real PostgreSQL, every layer included.
// It runs when KAKAROT_TEST_DSN is set.
type stack struct {
	t     *testing.T
	http  http.Handler
	grpc  *grpc.ClientConn
	audit *audit
}

func start(t *testing.T) *stack {
	t.Helper()
	dsn := os.Getenv("KAKAROT_TEST_DSN")
	if dsn == "" {
		t.Skip("KAKAROT_TEST_DSN not set")
	}
	ctx := t.Context()
	pool, err := pgxdrv.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := storm.Build(app.Models()...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.AutoPool(ctx, pool, schema, migrate.AutoOptions{}); err != nil {
		t.Fatal(err)
	}
	db := pgxdrv.Pool{P: pool}
	if _, err := db.Exec(ctx, "TRUNCATE entries, accounts, outbox_events, idempotency_records", nil); err != nil {
		t.Fatal(err)
	}

	au := &audit{}
	w := app.Build(app.Config{
		DB:     db,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Tokens: map[string]string{"alice-secret": "alice", "bob-secret": "bob"},
		Audit:  au,
	})
	lis := bufconn.Listen(1 << 20)
	servers := []kakarot.Server{&grpcx.Server{GRPC: w.GRPC, Listener: lis}, w.Consumer, w.Relay}
	for _, s := range servers {
		go s.Serve(context.Background())
	}
	t.Cleanup(func() {
		for _, s := range servers {
			_ = s.Shutdown(context.Background())
		}
	})
	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &stack{t: t, http: w.HTTP, grpc: conn, audit: au}
}

// call performs an HTTP request as token and decodes the JSON answer.
func call[T any](s *stack, token, method, path string, body any, hdr ...string) (int, T) {
	s.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	s.http.ServeHTTP(w, req)
	var out T
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil && w.Body.Len() > 0 {
		s.t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body)
	}
	return w.Code, out
}

type errBody struct {
	Error struct {
		Code   string            `json:"code"`
		Fields map[string]string `json:"fields"`
	} `json:"error"`
}

func (s *stack) open(token string, balance int64) domain.Account {
	s.t.Helper()
	code, a := call[domain.Account](s, token, "POST", "/accounts", nil)
	if code != http.StatusCreated || a.ID == "" {
		s.t.Fatalf("open: %d %+v", code, a)
	}
	if balance > 0 {
		code, a = call[domain.Account](s, token, "POST", "/accounts/"+a.ID+"/deposits", map[string]int64{"amount": balance})
		if code != http.StatusCreated || a.Balance != balance {
			s.t.Fatalf("deposit: %d %+v", code, a)
		}
	}
	return a
}

func (s *stack) balance(token, id string) int64 {
	s.t.Helper()
	code, a := call[domain.Account](s, token, "GET", "/accounts/"+id, nil)
	if code != http.StatusOK {
		s.t.Fatalf("get %s: %d", id, code)
	}
	return a.Balance
}

func TestLiveTransferOverHTTP(t *testing.T) {
	s := start(t)
	alice, bob := s.open("alice-secret", 100), s.open("bob-secret", 0)

	code, done := call[domain.TransferDone](s, "alice-secret", "POST", "/transfers",
		domain.Transfer{From: alice.ID, To: bob.ID, Amount: 30})
	if code != http.StatusCreated || done.FromBalance != 70 {
		t.Fatalf("transfer: %d %+v", code, done)
	}
	if a, b := s.balance("alice-secret", alice.ID), s.balance("bob-secret", bob.ID); a != 70 || b != 30 {
		t.Fatalf("balances alice=%d bob=%d", a, b)
	}

	// The ledger explains the balance.
	code, page := call[kakarot.Page[domain.Entry]](s, "alice-secret", "GET", "/accounts/"+alice.ID+"/entries?limit=1", nil)
	if code != http.StatusOK || len(page.Items) != 1 || page.Items[0].Kind != domain.KindDeposit || page.Next == "" {
		t.Fatalf("entries page 1: %d %+v", code, page)
	}
	_, page = call[kakarot.Page[domain.Entry]](s, "alice-secret", "GET", "/accounts/"+alice.ID+"/entries?limit=1&cursor="+page.Next, nil)
	if len(page.Items) != 1 || page.Items[0].Kind != domain.KindTransferOut || page.Items[0].Amount != -30 || page.Items[0].Ref != done.ID {
		t.Fatalf("entries page 2: %+v", page)
	}

	// The event reaches the audit handler through outbox, relay and queue.
	deadline := time.Now().Add(5 * time.Second)
	for len(s.audit.ids()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if ids := s.audit.ids(); len(ids) != 1 || ids[0] != done.ID {
		t.Fatalf("audited = %v", ids)
	}
}

func TestLiveRefusals(t *testing.T) {
	s := start(t)
	alice, bob := s.open("alice-secret", 50), s.open("bob-secret", 0)

	if code, _ := call[errBody](s, "", "GET", "/accounts/"+alice.ID, nil); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := call[errBody](s, "wrong", "GET", "/accounts/"+alice.ID, nil); code != http.StatusUnauthorized {
		t.Fatalf("bad token: %d", code)
	}
	// Bob cannot see or spend from Alice's account, and cannot tell it exists.
	if code, e := call[errBody](s, "bob-secret", "GET", "/accounts/"+alice.ID, nil); code != http.StatusNotFound {
		t.Fatalf("foreign read: %d %+v", code, e)
	}
	if code, _ := call[errBody](s, "bob-secret", "POST", "/transfers", domain.Transfer{From: alice.ID, To: bob.ID, Amount: 1}); code != http.StatusNotFound {
		t.Fatalf("foreign spend: %d", code)
	}
	if code, _ := call[errBody](s, "bob-secret", "GET", "/accounts/"+alice.ID+"/entries", nil); code != http.StatusNotFound {
		t.Fatalf("foreign ledger: %d", code)
	}
	// The wallet's own error code, with its own status.
	code, e := call[errBody](s, "alice-secret", "POST", "/transfers", domain.Transfer{From: alice.ID, To: bob.ID, Amount: 51})
	if code != http.StatusUnprocessableEntity || e.Error.Code != "insufficient_funds" {
		t.Fatalf("overdraft: %d %+v", code, e)
	}
	code, e = call[errBody](s, "alice-secret", "POST", "/transfers", domain.Transfer{From: alice.ID, To: alice.ID, Amount: 0})
	if code != http.StatusBadRequest || e.Error.Fields["amount"] == "" || e.Error.Fields["to"] == "" {
		t.Fatalf("validation: %d %+v", code, e)
	}
	if a, b := s.balance("alice-secret", alice.ID), s.balance("bob-secret", bob.ID); a != 50 || b != 0 {
		t.Fatalf("a refused request moved money: alice=%d bob=%d", a, b)
	}
}

func TestLiveIdempotentTransfer(t *testing.T) {
	s := start(t)
	alice, bob := s.open("alice-secret", 100), s.open("bob-secret", 0)
	in := domain.Transfer{From: alice.ID, To: bob.ID, Amount: 40}

	_, first := call[domain.TransferDone](s, "alice-secret", "POST", "/transfers", in, "Idempotency-Key", "pay-1")
	_, again := call[domain.TransferDone](s, "alice-secret", "POST", "/transfers", in, "Idempotency-Key", "pay-1")
	if first.ID == "" || again.ID != first.ID {
		t.Fatalf("first=%+v again=%+v", first, again)
	}
	if a := s.balance("alice-secret", alice.ID); a != 60 {
		t.Fatalf("a retried transfer moved money twice: alice=%d", a)
	}
	// Bob guessing Alice's key gets nothing of hers: it is his own key.
	code, _ := call[errBody](s, "bob-secret", "POST", "/transfers", in, "Idempotency-Key", "pay-1")
	if code != http.StatusNotFound {
		t.Fatalf("another caller replayed the transfer: %d", code)
	}
}

// Opposite transfers at once: no deadlock, no lost update, money conserved.
func TestLiveConcurrentTransfersConserveMoney(t *testing.T) {
	s := start(t)
	a, b := s.open("alice-secret", 1000), s.open("alice-secret", 1000)
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Go(func() {
			in := domain.Transfer{From: a.ID, To: b.ID, Amount: 7}
			if i%2 == 1 {
				in = domain.Transfer{From: b.ID, To: a.ID, Amount: 3}
			}
			if code, _ := call[domain.TransferDone](s, "alice-secret", "POST", "/transfers", in); code != http.StatusCreated {
				t.Errorf("transfer %d: %d", i, code)
			}
		})
	}
	wg.Wait()
	ba, bb := s.balance("alice-secret", a.ID), s.balance("alice-secret", b.ID)
	if ba != 1000-20*7+20*3 || bb != 1000+20*7-20*3 || ba+bb != 2000 {
		t.Fatalf("a=%d b=%d", ba, bb)
	}
}

func TestLiveImportOverHTTP(t *testing.T) {
	s := start(t)
	a := s.open("alice-secret", 0)
	lines := `{"account_id":"` + a.ID + `","amount":10}` + "\n" + `{"account_id":"` + a.ID + `","amount":15}` + "\n"
	code, res := call[domain.ImportResult](s, "alice-secret", "POST", "/deposits/import", lines)
	if code != http.StatusCreated || res.Deposits != 2 || res.Total != 25 || s.balance("alice-secret", a.ID) != 25 {
		t.Fatalf("import: %d %+v", code, res)
	}
}

// The same service over gRPC, through the generated client.
func TestLiveGRPC(t *testing.T) {
	s := start(t)
	client := transport.NewWalletGRPCClient(s.grpc)
	alice := grpcx.WithBearer(t.Context(), "alice-secret")
	bob := grpcx.WithBearer(t.Context(), "bob-secret")

	a, err := client.OpenAccount(alice, domain.OpenAccount{})
	if err != nil || a.Owner != "alice" {
		t.Fatalf("open: %+v %v", a, err)
	}
	b, err := client.OpenAccount(bob, domain.OpenAccount{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.ImportDeposits(alice, kakarot.SliceStream([]domain.Deposit{{AccountID: a.ID, Amount: 60}, {AccountID: a.ID, Amount: 40}}))
	if err != nil || res.Total != 100 {
		t.Fatalf("import: %+v %v", res, err)
	}
	done, err := client.Transfer(alice, domain.Transfer{From: a.ID, To: b.ID, Amount: 25, Key: "grpc-1"})
	if err != nil || done.FromBalance != 75 {
		t.Fatalf("transfer: %+v %v", done, err)
	}
	if again, err := client.Transfer(alice, domain.Transfer{From: a.ID, To: b.ID, Amount: 25, Key: "grpc-1"}); err != nil || again.ID != done.ID {
		t.Fatalf("replay: %+v %v", again, err)
	}
	if got, err := client.GetAccount(bob, domain.GetAccount{ID: b.ID}); err != nil || got.Balance != 25 {
		t.Fatalf("bob: %+v %v", got, err)
	}
	_, err = client.Transfer(alice, domain.Transfer{From: a.ID, To: b.ID, Amount: 1000})
	if kakarot.CodeOf(err) != domain.CodeInsufficientFunds {
		t.Fatalf("the wallet's own code must survive gRPC: %v", err)
	}
	_, err = client.Transfer(alice, domain.Transfer{From: a.ID, To: a.ID})
	if e := kakarot.AsError(err); e.Code != kakarot.CodeInvalid || e.Fields["amount"] == "" {
		t.Fatalf("validation fields must survive gRPC: %+v", e)
	}
	if _, err := client.GetAccount(t.Context(), domain.GetAccount{ID: a.ID}); kakarot.CodeOf(err) != kakarot.CodeUnauthorized {
		t.Fatalf("no token: %v", err)
	}
	if _, err := client.GetAccount(bob, domain.GetAccount{ID: a.ID}); kakarot.CodeOf(err) != kakarot.CodeNotFound {
		t.Fatalf("foreign read: %v", err)
	}
	page, err := client.ListEntries(alice, domain.ListEntries{AccountID: a.ID})
	if err != nil || len(page.Items) != 3 {
		t.Fatalf("entries: %+v %v", page, err)
	}
}
