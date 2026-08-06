package middleware

import (
	"encoding/json"
	"net/http"

	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

type AuthMiddleware struct {
	Token string
}

func NewAuth(token string) *AuthMiddleware {
	return &AuthMiddleware{Token: token}
}

func (m *AuthMiddleware) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}

		token := r.Header.Get("Authorization")
		if token == "" {
			writeJSON(w, http.StatusUnauthorized, mcp.NewError(mcp.ErrorCodeUnauthorized, "missing authorization header", false))
			return
		}

		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}

		if token != m.Token {
			writeJSON(w, http.StatusUnauthorized, mcp.NewError(mcp.ErrorCodeUnauthorized, "invalid token", false))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
