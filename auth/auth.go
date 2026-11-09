// Package auth carries a typed principal (the authenticated caller) through
// context.Context. P is your own type, so reading it never needs a cast.
//
//	type User struct{ ID, Tenant string; Roles []string }
//
//	r.Use(auth.Bearer(verifyJWT))                                  // transport level
//	g := r.Group("admin", auth.Require(func(u User) error { ... })) // endpoint level
//	u, ok := auth.From[User](ctx)                                   // anywhere below
package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/transport/httpx"
)

type key[P any] struct{}

// With returns ctx carrying principal p.
func With[P any](ctx context.Context, p P) context.Context {
	return context.WithValue(ctx, key[P]{}, p)
}

// From returns the principal of type P carried by ctx.
func From[P any](ctx context.Context) (P, bool) {
	p, ok := ctx.Value(key[P]{}).(P)
	return p, ok
}

// Verifier turns a credential into a principal, or fails with
// CodeUnauthorized.
type Verifier[P any] func(ctx context.Context, token string) (P, error)

// Bearer is HTTP transport middleware: it verifies "Authorization: Bearer
// <token>" when present and stores the principal in the request context.
// Requests without a token pass through; use Require to demand one.
func Bearer[P any](verify Verifier[P]) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			token, ok := strings.CutPrefix(h, "Bearer ")
			if !ok || token == "" {
				next.ServeHTTP(w, r)
				return
			}
			p, err := verify(r.Context(), token)
			if err != nil {
				if kakarot.CodeOf(err) == kakarot.CodeInternal {
					err = kakarot.E(kakarot.CodeUnauthorized, "invalid credentials", err)
				}
				httpx.WriteError(r.Context(), w, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(With(r.Context(), p)))
		})
	}
}

// Require is shared endpoint middleware: the call needs a principal of type
// P, and check (optional) decides whether that principal may proceed.
// It works for every transport that put the principal in ctx.
func Require[P any](check func(p P) error) kakarot.Middleware {
	return func(ctx context.Context, op kakarot.Op, next kakarot.Next) error {
		p, ok := From[P](ctx)
		if !ok {
			return kakarot.E(kakarot.CodeUnauthorized, "authentication required", nil)
		}
		if check != nil {
			if err := check(p); err != nil {
				if kakarot.CodeOf(err) == kakarot.CodeInternal {
					return kakarot.E(kakarot.CodeForbidden, "not allowed", err)
				}
				return err
			}
		}
		return next(ctx)
	}
}
