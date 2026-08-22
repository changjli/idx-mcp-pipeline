package client

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// roundTripFunc adapts a func to http.RoundTripper for hermetic client tests.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// stubFlare is a hermetic flareFetcher for the IDX client's flaresolverr mode.
type stubFlare struct {
	body       []byte
	status     int
	err        error
	gotURL     string
	gotHeaders map[string]string
}

func (s *stubFlare) Fetch(url string, headers map[string]string) ([]byte, int, error) {
	s.gotURL = url
	s.gotHeaders = headers
	return s.body, s.status, s.err
}

func (s *stubFlare) Close() {}

// TestClient_GetWithHeaders_FlareSolverrMode verifies that in flaresolverr mode
// GetWithHeaders delegates to the FlareSolverr path and surfaces the extracted
// JSON as the response body with the expected status.
func TestClient_GetWithHeaders_FlareSolverrMode(t *testing.T) {
	stub := &stubFlare{
		body:   []byte(`{"data":[{"StockCode":"BBCA"}]}`),
		status: http.StatusOK,
	}
	c := &Client{
		config: Config{BaseURL: "https://idx.example", FetchMode: "flaresolverr"},
		flare:  stub,
		log:    logrus.New(),
		stopCh: make(chan struct{}),
	}

	headers := map[string]string{"Referer": "https://idx.example/en/market-data/"}
	resp, err := c.GetWithHeaders("/primary/TradingSummary/GetStockSummary", headers)
	if err != nil {
		t.Fatalf("GetWithHeaders: %v", err)
	}
	defer resp.Body.Close()

	if stub.gotURL != "https://idx.example/primary/TradingSummary/GetStockSummary" {
		t.Errorf("expected resolved URL, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != headers["Referer"] {
		t.Errorf("expected Referer passthrough, got %v", stub.gotHeaders)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != string(stub.body) {
		t.Errorf("expected body %q, got %q", stub.body, body)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

// TestClient_GetStream_BypassesCache verifies GetStream skips the stale cache
// (the same URL may hold a fresh JSON entry) and streams the live body rather
// than buffering or caching it — the two properties disclosure PDF extraction
// relies on (10MB binaries must not land in the in-memory cache).
func TestClient_GetStream_BypassesCache(t *testing.T) {
	var hits int
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		hits++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("live-pdf-body")),
			Request:    r,
		}, nil
	})

	cm, err := NewCookieManager("https://idx.example", time.Hour, "test-agent")
	if err != nil {
		t.Fatalf("cookie manager: %v", err)
	}
	cm.lastRefresh = time.Now() // fresh cookie: skip the init fetch

	c := &Client{
		config:  Config{BaseURL: "https://idx.example", Timeout: time.Second},
		http:    &http.Client{Transport: transport},
		limiter: NewRateLimiter(1000),
		cache:   NewStaleCache(time.Hour),
		cookies: cm,
		log:     logrus.New(),
		stopCh:  make(chan struct{}),
	}

	// Seed a fresh cache entry for the URL GetStream will request: a cache-read
	// would serve this instead of hitting upstream.
	c.cache.Set("https://idx.example/pdf", http.StatusOK, make(http.Header), []byte("stale"))

	resp, err := c.GetStream("/pdf", nil)
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	defer resp.Body.Close()

	if hits != 1 {
		t.Errorf("expected 1 upstream hit (cache bypass), got %d", hits)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "live-pdf-body" {
		t.Errorf("expected live body %q, got %q", "live-pdf-body", body)
	}
	// GetStream must not write the response to the cache either.
	if got, _, _, _ := c.cache.Get("https://idx.example/pdf"); string(got) != "stale" {
		t.Errorf("GetStream must not overwrite the cache, got %q", got)
	}
}
