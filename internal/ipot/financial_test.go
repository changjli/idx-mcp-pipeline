package ipot

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// newFinancialTestClient builds a Client pointed at an httptest server serving
// the captured fixture, with fast pacing for tests.
func newFinancialTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.MinDelay = 0
	cfg.CacheTTL = time.Minute
	return NewClient(cfg, logrus.New())
}

// TestFetchFinancialQuarterly fetches through a fake upstream built from the
// fixture and checks the request URL and the filtered result.
func TestFetchFinancialQuarterly(t *testing.T) {
	var gotPath, gotQuery string
	c := newFinancialTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		w.Write(loadFixture(t, "fundamental-tlkm-q1.html"))
	})

	fin, err := c.FetchFinancial(context.Background(), "tlkm", ViewQuarterly)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !strings.HasSuffix(gotPath, "/module/saham/include/fundamental.php") {
		t.Errorf("path = %q", gotPath)
	}
	if !strings.Contains(gotQuery, "code=TLKM") || !strings.Contains(gotQuery, "quarter=1") {
		t.Errorf("query = %q, want code=TLKM&quarter=1", gotQuery)
	}
	if len(fin.Periods) == 0 {
		t.Fatal("no periods returned")
	}
	for _, p := range fin.Periods {
		if p.IsForecast || p.IsInterim || p.DurationMonths != 3 {
			t.Errorf("period %q should be a reported 3M column", p.Label)
		}
	}
}

// TestFetchFinancialRecent checks the recent view maps to quarter=5 and keeps
// every column, forecast and interim included.
func TestFetchFinancialRecent(t *testing.T) {
	var gotQuery string
	c := newFinancialTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Write(loadFixture(t, "fundamental-tlkm-q5.html"))
	})

	fin, err := c.FetchFinancial(context.Background(), "TLKM", ViewRecent)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !strings.Contains(gotQuery, "quarter=5") {
		t.Errorf("query = %q, want quarter=5", gotQuery)
	}
	if len(fin.Periods) != 8 {
		t.Errorf("periods = %d, want 8 (nothing filtered in recent view)", len(fin.Periods))
	}
	if !fin.Periods[0].IsForecast || !fin.Periods[1].IsInterim {
		t.Errorf("first periods = %q, %q; want forecast then interim", fin.Periods[0].Label, fin.Periods[1].Label)
	}
}

// TestFetchFinancialAnnualQuarterParam checks the annual view maps to
// quarter=4.
func TestFetchFinancialAnnualQuarterParam(t *testing.T) {
	var gotQuery string
	c := newFinancialTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Write(loadFixture(t, "fundamental-tlkm-q4.html"))
	})
	if _, err := c.FetchFinancial(context.Background(), "TLKM", ViewAnnual); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !strings.Contains(gotQuery, "quarter=4") {
		t.Errorf("query = %q, want quarter=4", gotQuery)
	}
}

// TestFetchFinancialCaches checks a second call within the TTL does not hit
// the upstream again, and that the returned value is a deep copy.
func TestFetchFinancialCaches(t *testing.T) {
	calls := 0
	c := newFinancialTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Write(loadFixture(t, "fundamental-tlkm-q5.html"))
	})

	first, err := c.FetchFinancial(context.Background(), "TLKM", ViewQuarterly)
	if err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	second, err := c.FetchFinancial(context.Background(), "TLKM", ViewQuarterly)
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if calls != 1 {
		t.Errorf("upstream calls = %d, want 1 (cache hit expected)", calls)
	}

	// Mutating the second result must not corrupt the cache: a third call
	// (after TTL expiry) would re-parse; here just check slice independence.
	second.Periods[0].Label = "tampered"
	if first.Periods[0].Label == "tampered" {
		t.Error("clone shares period slice with caller")
	}

	// Different view = different cache key = its own upstream call.
	if _, err := c.FetchFinancial(context.Background(), "TLKM", ViewAnnual); err != nil {
		t.Fatalf("annual fetch: %v", err)
	}
	if calls != 2 {
		t.Errorf("upstream calls after annual = %d, want 2", calls)
	}
}

// TestFetchFinancialInvalidView checks an unknown view is rejected before any
// upstream call.
func TestFetchFinancialInvalidView(t *testing.T) {
	c := newFinancialTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream should not be called for invalid view")
	})
	if _, err := c.FetchFinancial(context.Background(), "TLKM", "monthly"); err == nil {
		t.Fatal("expected error for invalid view")
	}
}

// TestFetchFinancial429 checks the rate-limit sentinel surfaces for backoff.
func TestFetchFinancial429(t *testing.T) {
	c := newFinancialTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	_, err := c.FetchFinancial(context.Background(), "TLKM", ViewQuarterly)
	if err == nil || !strings.Contains(err.Error(), ErrUpstream429.Error()) {
		t.Errorf("err = %v, want ErrUpstream429", err)
	}
}
