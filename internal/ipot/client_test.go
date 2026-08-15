package ipot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(os.Stderr)
	l.SetLevel(logrus.PanicLevel)
	return l
}

// newTestClient builds a client pointed at the given httptest server with a
// tiny pacing delay so tests don't sleep for the production 2s.
func newTestClient(ts *httptest.Server) *Client {
	return NewClient(Config{
		BaseURL:  ts.URL,
		Timeout:  5 * time.Second,
		MinDelay: 10 * time.Millisecond,
		CacheTTL: time.Hour,
	}, testLogger())
}

func TestFetch_URLShape(t *testing.T) {
	var gotURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		body, _ := os.ReadFile("testdata/raja.html")
		w.Write(body)
	}))
	defer ts.Close()

	c := newTestClient(ts)
	res, err := c.Fetch(context.Background(), "RAJA", time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	want := "/module/saham/include/data-brokersummary.php?code=RAJA&start=08/12/2026&end=08/12/2026&fd=all&board=RG"
	if gotURL != want {
		t.Errorf("request URL = %q, want %q", gotURL, want)
	}
	if len(res.Buyers) != 10 {
		t.Errorf("expected 10 buyers, got %d", len(res.Buyers))
	}
}

func TestFetch_CacheHit(t *testing.T) {
	var hits int
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		body, _ := os.ReadFile("testdata/raja.html")
		w.Write(body)
	}))
	defer ts.Close()

	c := newTestClient(ts)
	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	if _, err := c.Fetch(context.Background(), "RAJA", date); err != nil {
		t.Fatalf("first Fetch error: %v", err)
	}
	if _, err := c.Fetch(context.Background(), "RAJA", date); err != nil {
		t.Fatalf("second Fetch error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Errorf("expected 1 upstream hit within cache TTL, got %d", hits)
	}
}

func TestFetch_CacheKeyedByTickerAndDate(t *testing.T) {
	var hits int
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		body, _ := os.ReadFile("testdata/raja.html")
		w.Write(body)
	}))
	defer ts.Close()

	c := newTestClient(ts)
	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	// Same ticker, different date → cache miss.
	if _, err := c.Fetch(context.Background(), "RAJA", date); err != nil {
		t.Fatalf("Fetch RAJA error: %v", err)
	}
	if _, err := c.Fetch(context.Background(), "RAJA", date.AddDate(0, 0, 1)); err != nil {
		t.Fatalf("Fetch RAJA+1d error: %v", err)
	}
	// Different ticker, same date → cache miss.
	if _, err := c.Fetch(context.Background(), "BBCA", date); err != nil {
		t.Fatalf("Fetch BBCA error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if hits != 3 {
		t.Errorf("expected 3 upstream hits (distinct ticker+date keys), got %d", hits)
	}
}

func TestFetch_Pacing(t *testing.T) {
	var mu sync.Mutex
	var reqTimes []time.Time
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqTimes = append(reqTimes, time.Now())
		mu.Unlock()
		body, _ := os.ReadFile("testdata/raja.html")
		w.Write(body)
	}))
	defer ts.Close()

	c := NewClient(Config{
		BaseURL:  ts.URL,
		Timeout:  5 * time.Second,
		MinDelay: 100 * time.Millisecond,
		CacheTTL: time.Hour,
	}, testLogger())

	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	if _, err := c.Fetch(context.Background(), "RAJA", date); err != nil {
		t.Fatalf("Fetch RAJA error: %v", err)
	}
	if _, err := c.Fetch(context.Background(), "BBCA", date); err != nil {
		t.Fatalf("Fetch BBCA error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reqTimes) != 2 {
		t.Fatalf("expected 2 upstream requests, got %d", len(reqTimes))
	}
	elapsed := reqTimes[1].Sub(reqTimes[0])
	if elapsed < 100*time.Millisecond {
		t.Errorf("requests spaced %v apart, want >= 100ms (MinDelay)", elapsed)
	}
}

func TestFetch_PacingUnderConcurrency(t *testing.T) {
	var mu sync.Mutex
	var reqTimes []time.Time
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqTimes = append(reqTimes, time.Now())
		mu.Unlock()
		body, _ := os.ReadFile("testdata/raja.html")
		w.Write(body)
	}))
	defer ts.Close()

	c := NewClient(Config{
		BaseURL:  ts.URL,
		Timeout:  5 * time.Second,
		MinDelay: 100 * time.Millisecond,
		CacheTTL: time.Hour,
	}, testLogger())

	// 5 concurrent fetches of distinct tickers must still be paced ≥ MinDelay
	// apart — releasing the pacing mutex during the sleep would let them all
	// fire at once.
	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ticker := fmt.Sprintf("T%03d", i)
			if _, err := c.Fetch(context.Background(), ticker, date); err != nil {
				t.Errorf("Fetch %s: %v", ticker, err)
			}
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(reqTimes) != 5 {
		t.Fatalf("expected 5 upstream requests, got %d", len(reqTimes))
	}
	// 90ms threshold (not 100ms) tolerates clock/scheduler jitter while still
	// catching the race that fires requests ~µs apart.
	const minSpacing = 90 * time.Millisecond
	for i := 1; i < len(reqTimes); i++ {
		elapsed := reqTimes[i].Sub(reqTimes[i-1])
		if elapsed < minSpacing {
			t.Errorf("requests %d-%d spaced %v apart, want >= %v (MinDelay under concurrency)", i-1, i, elapsed, minSpacing)
		}
	}
}

func TestFetch_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := newTestClient(ts)
	_, err := c.Fetch(context.Background(), "RAJA", time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
}

func TestFetch_EmptyResult(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := os.ReadFile("testdata/empty.html")
		w.Write(body)
	}))
	defer ts.Close()

	c := newTestClient(ts)
	res, err := c.Fetch(context.Background(), "RAJA", time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Fetch on empty table should not error, got: %v", err)
	}
	if len(res.Buyers) != 0 || len(res.Sellers) != 0 {
		t.Errorf("expected empty result, got %d buyers %d sellers", len(res.Buyers), len(res.Sellers))
	}
}

func TestFetch_EmptyResultNotCached(t *testing.T) {
	var hits int
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		body, _ := os.ReadFile("testdata/empty.html")
		w.Write(body)
	}))
	defer ts.Close()

	c := newTestClient(ts)
	date := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)

	// Two fetches of the same empty ticker+date must both hit upstream —
	// caching the empty table would blind the tool to just-published data.
	if _, err := c.Fetch(context.Background(), "RAJA", date); err != nil {
		t.Fatalf("first Fetch error: %v", err)
	}
	if _, err := c.Fetch(context.Background(), "RAJA", date); err != nil {
		t.Fatalf("second Fetch error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if hits != 2 {
		t.Errorf("expected 2 upstream hits for empty result (not cached), got %d", hits)
	}
}

func TestFetch_429ReturnsSentinel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer ts.Close()

	c := newTestClient(ts)
	_, err := c.Fetch(context.Background(), "RAJA", time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, ErrUpstream429) {
		t.Errorf("expected ErrUpstream429, got %v", err)
	}
}
