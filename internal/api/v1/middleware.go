package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/83codes/octar/internal/auth"
	"github.com/83codes/octar/internal/auth/identity"
)

type contextKey string

const identityCtxKey contextKey = "identity"

func needsAuth(path string) bool {
	return strings.HasPrefix(path, "/namespaces") ||
		strings.HasPrefix(path, "/users") ||
		strings.HasPrefix(path, "/queues") ||
		path == "/internal/metrics"
}

func authMiddleware(authSvc *auth.Service) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !needsAuth(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				writeJSONError(w, http.StatusUnauthorized, "missing authorization header")
				return
			}

			token := strings.TrimPrefix(authHeader, "Bearer ")
			id, err := authSvc.VerifyToken(token)
			if err != nil {
				writeJSONError(w, http.StatusUnauthorized, "invalid or expired token: "+err.Error())
				return
			}

			ctx := context.WithValue(r.Context(), identityCtxKey, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func identityFrom(ctx context.Context) *identity.Identity {
	id, _ := ctx.Value(identityCtxKey).(*identity.Identity)
	return id
}

func requireAuth(ctx context.Context) (*identity.Identity, error) {
	id := identityFrom(ctx)
	if id == nil {
		return nil, huma.Error401Unauthorized("unauthorized")
	}
	return id, nil
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"title": msg, "status": status})
}
