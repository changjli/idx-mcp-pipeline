package indexalpha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
			name:   "daily request cap",
			status: http.StatusForbidden,
			body:   `{"detail":"Daily API limit exceeded"}`,
			want:   broker.ErrDailyLimit,
		},
		{
			name:    "daily cap with monthly units left",
			status:  http.StatusForbidden,
			headers: map[string]string{"X-Monthly-Remaining": "124", "X-Monthly-Reset": "1795784282"},
			body:    `{"detail":"API limit exceeded"}`,
			want:    broker.ErrDailyLimit,
		},
		{
			name:   "daily request wording",
			status: http.StatusForbidden,
			body:   `{"detail":"Daily request limit reached, try again tomorrow"}`,
			want:   broker.ErrDailyLimit,
		},
		{
			name:   "per day wording",
			status: http.StatusForbidden,
			body:   `{"detail":"Too many requests per day"}`,
			want:   broker.ErrDailyLimit,
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

// TestFetchRange_PinsWindowAndForeignLane is the 10a client change: a range is
// one request with from/to, the f lane pinned, market RG, and the f payload
// parsed without the all-investor reconciliation.
func TestFetchRange_PinsWindowAndForeignLane(t *testing.T) {
	respBody := []byte(`{"success":true,"data":[
		{"code":"HD","buy_freq":2,"buy_volume":100,"buy_value":1000.0,
		 "sell_freq":1,"sell_volume":100,"sell_value":900.0,
		 "buy_avg":10.0,"sell_avg":9.0}],"error":null}`)

	var gotQuery map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{
			"ticker":   r.URL.Query().Get("ticker"),
			"from":     r.URL.Query().Get("from"),
			"to":       r.URL.Query().Get("to"),
			"investor": r.URL.Query().Get("investor"),
			"market":   r.URL.Query().Get("market"),
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Monthly-Remaining", "99")
		_, _ = w.Write(respBody)
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, APIKey: "test-key", Timeout: 5 * time.Second}, testLogger())
	from := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

	res, _, err := c.FetchRangeWithKey(context.Background(), "test-key", "fire", from, to, broker.InvestorForeign)
	if err != nil {
		t.Fatalf("FetchRangeWithKey: %v", err)
	}
	want := map[string]string{
		"ticker":   "FIRE",
		"from":     "2026-10-27",
		"to":       "2026-11-02",
		"investor": "f",
		"market":   "RG",
	}
	for k, v := range want {
		if gotQuery[k] != v {
			t.Errorf("query %s = %q, want %q", k, gotQuery[k], v)
		}
	}
	if res.Investor != broker.InvestorForeign {
		t.Errorf("Investor = %q, want f", res.Investor)
	}
	if res.Totals.FNVal == nil || *res.Totals.FNVal != 100 {
		t.Errorf("FNVal = %v, want 100 (1000 buy − 900 sell)", res.Totals.FNVal)
	}
}

func TestFetchRange_RejectsInvertedWindowAndUnsupportedInvestor(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"success":true,"data":[],"error":null}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, APIKey: "test-key", Timeout: 5 * time.Second}, testLogger())
	from := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)

	if _, _, err := c.FetchRangeWithKey(context.Background(), "test-key", "FIRE", from, to, broker.InvestorForeign); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("inverted window err = %v, want ErrInvalidRequest", err)
	}
	if _, _, err := c.FetchRangeWithKey(context.Background(), "test-key", "FIRE", to, from, "d"); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("unsupported investor err = %v, want ErrInvalidRequest", err)
	}
	if calls != 0 {
		t.Errorf("server calls = %d, want 0 (rejected before the request)", calls)
	}
}

// TestFetchBatchRange_PostsWindowAndForeignLane pins the batch range body: one
// window, one investor lane, market RG.
func TestFetchBatchRange_PostsWindowAndForeignLane(t *testing.T) {
	respBody := []byte(`{"success":true,"data":{
		"FIRE":[{"code":"HD","buy_freq":2,"buy_volume":100,"buy_value":1000.0,
		         "sell_freq":1,"sell_volume":100,"sell_value":900.0,
		         "buy_avg":10.0,"sell_avg":9.0}],
		"GRPH":[{"code":"AK","buy_freq":1,"buy_volume":200,"buy_value":2000.0,
		         "sell_freq":2,"sell_volume":200,"sell_value":2100.0,
		         "buy_avg":10.0,"sell_avg":10.5}]
	},"error":null}`)

	var gotBody batchRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		_, _ = w.Write(respBody)
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, APIKey: "test-key", Timeout: 5 * time.Second}, testLogger())
	from := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

	batch, _, err := c.FetchBatchRangeWithKey(context.Background(), "test-key",
		[]string{"FIRE", "GRPH"}, from, to, broker.InvestorForeign)
	if err != nil {
		t.Fatalf("FetchBatchRangeWithKey: %v", err)
	}
	if gotBody.From != "2026-10-27" || gotBody.To != "2026-11-02" {
		t.Errorf("from/to = %q/%q, want the window", gotBody.From, gotBody.To)
	}
	if gotBody.Investor != "f" || gotBody.Market != "RG" {
		t.Errorf("investor/market = %q/%q, want f/RG", gotBody.Investor, gotBody.Market)
	}
	if len(batch.Results) != 2 {
		t.Fatalf("results = %d, want 2", len(batch.Results))
	}
	if got := batch.Results["FIRE"].Totals.FNVal; got == nil || *got != 100 {
		t.Errorf("FIRE FNVal = %v, want 100", got)
	}
}

// TestFetchBatch_PostsOneDayBatch pins the batch contract: POST JSON, one date
// (from == to), regular market, all investors, deduplicated tickers, and the
// per-ticker response shape parsed into broker.Batch.
func TestFetchBatch_PostsOneDayBatch(t *testing.T) {
	respBody := []byte(`{"success":true,"data":{
		"BBCA":[{"code":"AF","buy_freq":1,"buy_volume":100,"buy_value":1000000.0,
		         "sell_freq":1,"sell_volume":100,"sell_value":1000000.0,
		         "buy_avg":100.0,"sell_avg":100.0}],
		"TLKM":[{"code":"AK","buy_freq":1,"buy_volume":200,"buy_value":2000000.0,
		         "sell_freq":2,"sell_volume":200,"sell_value":2000000.0,
		         "buy_avg":100.0,"sell_avg":100.0}]
	},"error":null}`)

	var gotMethod, gotPath, gotAuth, gotContentType string
	var gotBody batchRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Monthly-Remaining", "148")
		_, _ = w.Write(respBody)
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, APIKey: "test-key", Timeout: 5 * time.Second}, testLogger())
	day := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)

	batch, err := c.FetchBatch(context.Background(), []string{" bbca ", "BBCA", "tlkm", ""}, day)
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/stocks/broker-summary/batch" {
		t.Errorf("%s %s, want POST /stocks/broker-summary/batch", gotMethod, gotPath)
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("Authorization = %q, want Bearer test-key", gotAuth)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	if len(gotBody.Tickers) != 2 || gotBody.Tickers[0] != "BBCA" || gotBody.Tickers[1] != "TLKM" {
		t.Errorf("tickers = %v, want deduplicated [BBCA TLKM]", gotBody.Tickers)
	}
	if gotBody.From != "2026-10-22" || gotBody.To != "2026-10-22" {
		t.Errorf("from/to = %q/%q, want the same single day", gotBody.From, gotBody.To)
	}
	if gotBody.Investor != "all" || gotBody.Market != "RG" {
		t.Errorf("investor/market = %q/%q, want all/RG", gotBody.Investor, gotBody.Market)
	}
	if len(batch.Results) != 2 || len(batch.Invalid) != 0 {
		t.Errorf("batch = %+v, want 2 valid tickers", batch)
	}
}

// TestFetchBatch_RejectsOversizedAndEmptyBeforeSpending asserts the 50-ticker
// limit is enforced before any request is made: a 51-ticker batch must not cost
// a daily request or quota.
func TestFetchBatch_RejectsOversizedAndEmptyBeforeSpending(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"success":true,"data":{},"error":null}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, APIKey: "test-key", Timeout: 5 * time.Second}, testLogger())

	tickers := make([]string, broker.MaxBatchTickers+1)
	for i := range tickers {
		tickers[i] = fmt.Sprintf("T%03d", i)
	}
	if _, err := c.FetchBatch(context.Background(), tickers, time.Now()); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("oversized batch err = %v, want ErrInvalidRequest", err)
	}
	if _, err := c.FetchBatch(context.Background(), []string{"", "  "}, time.Now()); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("empty batch err = %v, want ErrInvalidRequest", err)
	}
	if calls != 0 {
		t.Errorf("server calls = %d, want 0 (rejected before the request)", calls)
	}
}
