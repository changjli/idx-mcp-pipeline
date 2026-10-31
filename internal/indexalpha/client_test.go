package indexalpha

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
)

func testLogger() *logrus.Logger {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	return log
}

// TestFetch_PinsOneDayAndRegularMarket is the "rejected by construction"
// guarantee: Fetch takes one date, sends from == to, pins market RG and states
// the all-investor view, and authenticates with the configured Bearer token.
func TestFetch_PinsOneDayAndRegularMarket(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("testdata", "bbri-2026-10-22.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var gotQuery map[string]string
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stocks/broker-summary" {
			t.Errorf("path = %q, want /stocks/broker-summary", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		gotQuery = map[string]string{
			"ticker":   r.URL.Query().Get("ticker"),
			"from":     r.URL.Query().Get("from"),
			"to":       r.URL.Query().Get("to"),
			"investor": r.URL.Query().Get("investor"),
			"market":   r.URL.Query().Get("market"),
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Monthly-Remaining", "149")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, APIKey: "test-key", Timeout: 5 * time.Second}, testLogger())
	day := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)

	res, err := c.Fetch(context.Background(), "BBRI", day)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(res.Buyers) == 0 || len(res.Sellers) == 0 {
		t.Fatalf("Fetch returned no rows: %+v", res)
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("Authorization = %q, want Bearer test-key", gotAuth)
	}
	want := map[string]string{
		"ticker":   "BBRI",
		"from":     "2026-10-22",
		"to":       "2026-10-22",
		"investor": "all",
		"market":   "RG",
	}
	for k, v := range want {
		if gotQuery[k] != v {
			t.Errorf("query %s = %q, want %q", k, gotQuery[k], v)
		}
	}
}

func TestFetch_StructuredErrors(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		headers map[string]string
		body    string
		want    error
	}{
		{
			name:   "missing token",
			status: http.StatusUnauthorized,
			body:   `{"detail":"Not authenticated"}`,
			want:   ErrUnauthorized,
		},
		{
			name:   "inactive key",
			status: http.StatusForbidden,
			body:   `{"detail":"API key inactive"}`,
			want:   ErrUnauthorized,
		},
		{
			name:    "monthly quota exhausted",
			status:  http.StatusForbidden,
			headers: map[string]string{"X-Monthly-Remaining": "0", "X-Monthly-Reset": "2026-11-29T00:00:00"},
			body:    `{"detail":"Account monthly API limit exceeded"}`,
			want:    broker.ErrQuotaExhausted,
		},
		{
			name:   "free plan daily quota exhausted",
			status: http.StatusForbidden,
			body:   `{"detail":"Daily API limit exceeded"}`,
			want:   broker.ErrQuotaExhausted,
		},
		{
			name:   "rate limited",
			status: http.StatusTooManyRequests,
			body:   `{"detail":"Too many requests"}`,
			want:   broker.ErrRateLimited,
		},
		{
			name:   "malformed body",
			status: http.StatusOK,
			body:   `{"success":true,"data":{}}`,
			want:   ErrMalformedPayload,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			c := NewClient(Config{BaseURL: srv.URL, APIKey: "test-key", Timeout: 5 * time.Second}, testLogger())
			_, err := c.Fetch(context.Background(), "BBRI", time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestFetch_NoKeyIsAnAuthError keeps a keyless deployment usable: the server
// still starts, and only the broker tool reports the missing credential.
func TestFetch_NoKeyIsAnAuthError(t *testing.T) {
	c := NewClient(Config{BaseURL: "http://127.0.0.1:0"}, testLogger())
	_, err := c.Fetch(context.Background(), "BBRI", time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
}
