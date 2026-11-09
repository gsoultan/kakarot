package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/auth"
	"github.com/gsoultan/kakarot/transport/httpx"
)

type user struct {
	ID    string
	Roles []string
}

func verify(_ context.Context, token string) (user, error) {
	switch token {
	case "admin-token":
		return user{ID: "u1", Roles: []string{"admin"}}, nil
	case "user-token":
		return user{ID: "u2"}, nil
	}
	return user{}, kakarot.E(kakarot.CodeUnauthorized, "bad token", nil)
}

func admin(u user) error {
	if !slices.Contains(u.Roles, "admin") {
		return kakarot.E(kakarot.CodeForbidden, "admins only", nil)
	}
	return nil
}

func TestBearerAndRequire(t *testing.T) {
	r := httpx.NewRouter()
	r.Use(auth.Bearer(verify))
	g := r.Group("admin", auth.Require(admin))
	g.Handle("GET /whoami", func(ctx context.Context, _ struct{}) (string, error) {
		u, ok := auth.From[user](ctx) // typed, no cast
		if !ok {
			return "", errors.New("no user")
		}
		return u.ID, nil
	})

	cases := []struct {
		token string
		want  int
	}{{"", 401}, {"junk", 401}, {"user-token", 403}, {"admin-token", 200}}
	for _, c := range cases {
		req := httptest.NewRequest("GET", "/whoami", nil)
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != c.want {
			t.Errorf("token %q: status %d, want %d (%s)", c.token, w.Code, c.want, w.Body)
		}
	}
	_ = http.StatusOK
}
