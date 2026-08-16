package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

func TestAuthenticate(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	cases := []struct {
		name         string
		token        string
		auth         string
		wantCode     int
		wantEnvelope bool
	}{
		{"valid bearer", "secret", "Bearer secret", http.StatusOK, false},
		{"valid bare", "secret", "secret", http.StatusOK, false},
		{"missing header", "secret", "", http.StatusUnauthorized, true},
		{"wrong token", "secret", "Bearer wrong", http.StatusUnauthorized, true},
		{"malformed prefix", "secret", "Basic secret", http.StatusUnauthorized, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			NewAuth(tc.token).Authenticate(ok).ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantCode)
			}
			if !tc.wantEnvelope {
				return
			}
			var env mcp.ErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			if env.Error.Code != mcp.ErrorCodeUnauthorized {
				t.Fatalf("envelope code = %q, want UNAUTHORIZED", env.Error.Code)
			}
		})
	}
}
