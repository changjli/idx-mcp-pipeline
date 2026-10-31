package indexalpha

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
)

// fakeClock is a mutex-guarded clock so cooldown and reset tests are exact.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

// callLog records which key hit an endpoint, across concurrent handlers.
type callLog struct {
	mu    sync.Mutex
	byKey map[string]int
	order []string
}

func newCallLog() *callLog { return &callLog{byKey: make(map[string]int)} }

func (l *callLog) add(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.byKey[key]++
	l.order = append(l.order, key)
	return l.byKey[key]
}

func (l *callLog) count(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.byKey[key]
}

func (l *callLog) total() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.order)
}

func (l *callLog) ordered() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.order...)
}

func bearerKey(r *http.Request) string {
	const prefix = "Bearer "
	v := r.Header.Get("Authorization")
	if len(v) > len(prefix) && v[:len(prefix)] == prefix {
		return v[len(prefix):]
	}
	return ""
}

func usageJSON(remaining int, periodEnd string, active bool) string {
	return fmt.Sprintf(`{"success":true,"data":{
		"api_key":"masked",
		"key":{"key_id":"id","key_name":"Default","current_usage":0,"monthly_limit":150,"remaining":%d},
		"account":{"plan":"Free","current_usage":0,"monthly_limit":150,"remaining":%d,"period_end":%q,"is_active":%t}
	},"error":null}`, remaining, remaining, periodEnd, active)
}

func brokerFixture(t *testing.T) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "bbri-2026-10-22.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return body
}

func writeBrokerOK(t *testing.T, w http.ResponseWriter, remaining int) {
	t.Helper()
	if remaining >= 0 {
		w.Header().Set("X-Monthly-Remaining", strconv.Itoa(remaining))
	}
	w.Header().Set("X-Monthly-Reset", "2026-11-30T00:00:00")
	_, _ = w.Write(brokerFixture(t))
}

func newTestPool(t *testing.T, baseURL string, keys []string, reserve int, clock *fakeClock) *Pool {
	t.Helper()
	p := NewPool(Config{
		BaseURL:    baseURL,
		Keys:       keys,
		Timeout:    5 * time.Second,
		Reserve:    reserve,
		Cooldown:   30 * time.Second,
		DailyLimit: 1000, // most tests are about monthly quota; daily tests override
	}, testLogger())
	p.now = clock.Now
	return p
}

// TestPool_SingleKeyIsUnchanged pins the one-key request to exactly what the
// client sent before the pool existed: same path, same query, same auth, same
// parsed result. A one-key deployment must not notice the pool.
func TestPool_SingleKeyIsUnchanged(t *testing.T) {
	fixture := brokerFixture(t)
	var gotAuth string
	var gotQuery map[string]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/usage":
			_, _ = w.Write([]byte(usageJSON(149, "2026-11-30T00:00:00", true)))
		case "/stocks/broker-summary":
			gotAuth = r.Header.Get("Authorization")
			gotQuery = map[string]string{
				"ticker":   r.URL.Query().Get("ticker"),
				"from":     r.URL.Query().Get("from"),
				"to":       r.URL.Query().Get("to"),
				"investor": r.URL.Query().Get("investor"),
				"market":   r.URL.Query().Get("market"),
			}
			w.Header().Set("X-Monthly-Remaining", "148")
			_, _ = w.Write(fixture)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	clock := newFakeClock()
	p := newTestPool(t, srv.URL, []string{"only-key"}, 0, clock)
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	res, err := p.Fetch(context.Background(), "BBRI", time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(res.Buyers) == 0 || len(res.Sellers) == 0 {
		t.Fatalf("Fetch returned no rows: %+v", res)
	}
	if gotAuth != "Bearer only-key" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	want := map[string]string{"ticker": "BBRI", "from": "2026-10-22", "to": "2026-10-22", "investor": "all", "market": "RG"}
	for k, v := range want {
		if gotQuery[k] != v {
			t.Errorf("query %s = %q, want %q", k, gotQuery[k], v)
		}
	}
	st := p.Status()
	if st.Configured != 1 || st.Usable != 1 || st.TotalRemaining != 148 {
		t.Errorf("status = %+v", st)
	}
	if st.Keys[0].Remaining != 148 || st.Keys[0].Unknown {
		t.Errorf("key status = %+v", st.Keys[0])
	}
}

// TestPool_RoundRobinsOnSuccess proves rotation is per successful request: the
// pointer moves only after a 200, so three calls use key1, key2, key1.
func TestPool_RoundRobinsOnSuccess(t *testing.T) {
	summary := newCallLog()
	var mu sync.Mutex
	counts := map[string]int{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := bearerKey(r)
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(10, "2026-11-30T00:00:00", true)))
			return
		}
		summary.add(key)
		mu.Lock()
		counts[key]++
		n := counts[key]
		mu.Unlock()
		writeBrokerOK(t, w, 10-n)
	}))
	defer srv.Close()

	clock := newFakeClock()
	p := newTestPool(t, srv.URL, []string{"key1", "key2"}, 0, clock)
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
			t.Fatalf("Fetch %d: %v", i, err)
		}
	}

	want := []string{"key1", "key2", "key1"}
	got := summary.ordered()
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

// TestPool_QuotaExhaustionRotatesAndStops drives key1 to exhaustion, shows the
// call fails over to key2, then shows the pool stops making requests once every
// key is out, reporting ErrQuotaExhausted and firing the event once.
func TestPool_QuotaExhaustionRotatesAndStops(t *testing.T) {
	summary := newCallLog()
	periodEnd := "2026-11-30T00:00:00"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := bearerKey(r)
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(5, periodEnd, true)))
			return
		}
		summary.add(key)
		if key == "key1" {
			w.Header().Set("X-Monthly-Remaining", "0")
			w.Header().Set("X-Monthly-Reset", periodEnd)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"detail":"Account monthly API limit exceeded"}`))
			return
		}
		// key2's last unit: the 200 consumes it, so the header reports zero.
		writeBrokerOK(t, w, 0)
	}))
	defer srv.Close()

	clock := newFakeClock()
	p := newTestPool(t, srv.URL, []string{"key1", "key2"}, 0, clock)

	var events []QuotaExhaustedEvent
	p.SetOnQuotaExhausted(func(ev QuotaExhaustedEvent) { events = append(events, ev) })

	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	res, err := p.Fetch(context.Background(), "BBRI", time.Now())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if res == nil {
		t.Fatal("Fetch returned nil result")
	}
	if got := summary.count("key1"); got != 1 {
		t.Errorf("key1 calls = %d, want 1", got)
	}
	if got := summary.count("key2"); got != 1 {
		t.Errorf("key2 calls = %d, want 1", got)
	}

	st := p.Status()
	if st.Usable != 0 || !st.QuotaStopped {
		t.Errorf("status = %+v, want no usable keys and quota stopped", st)
	}
	if !st.Keys[0].Exhausted || !st.Keys[1].Exhausted {
		t.Errorf("keys = %+v, want both exhausted", st.Keys)
	}
	if len(events) != 1 {
		t.Fatalf("quota events = %d, want 1", len(events))
	}
	if events[0].ResetAt == nil {
		t.Errorf("event reset = nil, want the period end")
	}

	// A second call must not spend a request on either key.
	_, err = p.Fetch(context.Background(), "BBRI", time.Now())
	if !errors.Is(err, broker.ErrQuotaExhausted) {
		t.Fatalf("err = %v, want ErrQuotaExhausted", err)
	}
	if got := summary.total(); got != 2 {
		t.Errorf("summary calls after stop = %d, want 2", got)
	}
	if len(events) != 1 {
		t.Errorf("quota events = %d after repeated calls, want 1", len(events))
	}
}

// TestPool_RateLimitCoolsDownAndRotates: a 429 parks only that key, the same
// call still succeeds on the next key, and the parked key returns after the
// cooldown without sleeping in the process.
func TestPool_RateLimitCoolsDownAndRotates(t *testing.T) {
	summary := newCallLog()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := bearerKey(r)
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(10, "2026-11-30T00:00:00", true)))
			return
		}
		summary.add(key)
		if key == "key1" {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"detail":"Too many requests"}`))
			return
		}
		writeBrokerOK(t, w, 9)
	}))
	defer srv.Close()

	clock := newFakeClock()
	p := newTestPool(t, srv.URL, []string{"key1", "key2"}, 0, clock)
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	st := p.Status()
	if st.Keys[0].CooldownUntil == nil {
		t.Fatalf("key1 cooldown = nil, want parked")
	}
	if st.Keys[0].Usable {
		t.Errorf("key1 usable during cooldown")
	}

	// Second call must skip key1.
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got := summary.count("key1"); got != 1 {
		t.Errorf("key1 calls during cooldown = %d, want 1", got)
	}

	clock.Advance(31 * time.Second)
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch after cooldown: %v", err)
	}
	if got := summary.count("key1"); got != 2 {
		t.Errorf("key1 calls after cooldown = %d, want 2", got)
	}
}

func TestPool_AllRateLimitedReturnsRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(10, "2026-11-30T00:00:00", true)))
			return
		}
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"detail":"Too many requests"}`))
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"k1", "k2"}, 0, newFakeClock())
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); !errors.Is(err, broker.ErrRateLimited) {
		t.Fatalf("err = %v, want ErrRateLimited", err)
	}
}

// TestPool_UnauthorizedKeyIsDisabled: a 401 condemns one key, failover keeps the
// source working, and the dead key is not retried on the next call.
func TestPool_UnauthorizedKeyIsDisabled(t *testing.T) {
	summary := newCallLog()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := bearerKey(r)
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(10, "2026-11-30T00:00:00", true)))
			return
		}
		summary.add(key)
		if key == "dead" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"detail":"Not authenticated"}`))
			return
		}
		writeBrokerOK(t, w, 9)
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"dead", "live"}, 0, newFakeClock())
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got := summary.count("dead"); got != 1 {
		t.Errorf("dead key calls = %d, want 1", got)
	}
	st := p.Status()
	if st.Keys[0].DisabledReason == "" || st.Keys[0].Usable {
		t.Errorf("dead key status = %+v", st.Keys[0])
	}
}

func TestPool_AllUnauthorizedReturnsUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(10, "2026-11-30T00:00:00", true)))
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"detail":"Not authenticated"}`))
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"k1", "k2"}, 0, newFakeClock())
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
}

// TestPool_RequestScopedErrorDoesNotRotate: a 400 is the request's fault, not
// the key's. It must not burn another key or park the first.
func TestPool_RequestScopedErrorDoesNotRotate(t *testing.T) {
	summary := newCallLog()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(10, "2026-11-30T00:00:00", true)))
			return
		}
		summary.add(bearerKey(r))
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":"invalid ticker"}`))
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"key1", "key2"}, 0, newFakeClock())
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	_, err := p.Fetch(context.Background(), "BBRI", time.Now())
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("err = %v, want ErrUpstream", err)
	}
	if got := summary.count("key1"); got != 1 {
		t.Errorf("key1 calls = %d, want 1", got)
	}
	if got := summary.count("key2"); got != 0 {
		t.Errorf("key2 calls = %d, want 0 (no failover on request error)", got)
	}
	st := p.Status()
	if !st.Keys[0].Usable || st.Keys[0].Exhausted {
		t.Errorf("key1 status = %+v, want usable and unpenalised", st.Keys[0])
	}
}

// TestPool_DeductsCostWhenHeaderMissing: the header is authoritative, but a
// response without it must still move the counter by the call's cost.
func TestPool_DeductsCostWhenHeaderMissing(t *testing.T) {
	fixture := brokerFixture(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(2, "2026-11-30T00:00:00", true)))
			return
		}
		// Deliberately no X-Monthly-Remaining.
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"only"}, 0, newFakeClock())
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}
	if got := p.Status().Keys[0].Remaining; got != 1 {
		t.Errorf("remaining after one call = %d, want 1", got)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); !errors.Is(err, broker.ErrQuotaExhausted) {
		t.Fatalf("err = %v, want ErrQuotaExhausted", err)
	}
}

// TestPool_ReseedsAfterRollingPeriod: an exhausted key stays parked until the
// period end, then reads /usage again instead of assuming a calendar month.
func TestPool_ReseedsAfterRollingPeriod(t *testing.T) {
	var usageCalls int32
	var available atomic.Bool
	summary := newCallLog()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			atomic.AddInt32(&usageCalls, 1)
			if available.Load() {
				_, _ = w.Write([]byte(usageJSON(50, "2026-12-30T00:00:00", true)))
			} else {
				_, _ = w.Write([]byte(usageJSON(0, "2026-11-01T00:00:00", true)))
			}
			return
		}
		summary.add(bearerKey(r))
		writeBrokerOK(t, w, 49)
	}))
	defer srv.Close()

	clock := newFakeClock()
	p := newTestPool(t, srv.URL, []string{"only"}, 0, clock)
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	// period_end 2026-11-01 is inside the future relative to the fake clock.
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); !errors.Is(err, broker.ErrQuotaExhausted) {
		t.Fatalf("err before reset = %v, want ErrQuotaExhausted", err)
	}
	if summary.total() != 0 {
		t.Fatalf("summary calls before reset = %d, want 0", summary.total())
	}

	clock.Advance(48 * time.Hour)
	available.Store(true)
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch after reset: %v", err)
	}
	if got := atomic.LoadInt32(&usageCalls); got != 2 {
		t.Errorf("usage calls = %d, want 2 (boot seed + reset reseed)", got)
	}
	if got := summary.total(); got != 1 {
		t.Errorf("summary calls after reset = %d, want 1", got)
	}
}

// TestPool_SeedFailureIsNonFatal: a /usage outage must not disable a working
// key; the first metered response fills the counter instead.
func TestPool_SeedFailureIsNonFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"detail":"boom"}`))
			return
		}
		writeBrokerOK(t, w, 9)
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"only"}, 0, newFakeClock())
	if err := p.Seed(context.Background()); err == nil {
		t.Fatal("Seed error = nil, want the seed failure surfaced")
	}
	if st := p.Status(); st.Keys[0].Usable != true || !st.Keys[0].Unknown {
		t.Fatalf("key after failed seed = %+v, want usable and unknown", st.Keys[0])
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got := p.Status().Keys[0].Remaining; got != 9 {
		t.Errorf("remaining = %d, want 9 from the header", got)
	}
}

func TestPool_NoKeysIsUnauthorized(t *testing.T) {
	p := NewPool(Config{}, testLogger())
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed with no keys: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	if p.Configured() != 0 {
		t.Errorf("configured = %d, want 0", p.Configured())
	}
}

// TestPool_ReserveIsCallerBudget: available is remaining minus reserve, but an
// on-demand Fetch may still use the reserved units.
func TestPool_ReserveIsCallerBudget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(5, "2026-11-30T00:00:00", true)))
			return
		}
		writeBrokerOK(t, w, 4)
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"only"}, 10, newFakeClock())
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	st := p.Status()
	if st.TotalRemaining != 5 || st.Available != 0 {
		t.Errorf("status = %+v, want remaining 5 and available 0", st)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch into the reserve: %v", err)
	}
}

// TestPool_ConcurrentFetches exercises the mutex and key rotation under the race
// detector; every key must stay reachable.
func TestPool_ConcurrentFetches(t *testing.T) {
	summary := newCallLog()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(100, "2026-11-30T00:00:00", true)))
			return
		}
		summary.add(bearerKey(r))
		writeBrokerOK(t, w, 100)
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"k1", "k2", "k3"}, 0, newFakeClock())
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent fetch: %v", err)
	}
	if got := summary.total(); got != 30 {
		t.Errorf("summary calls = %d, want 30", got)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if summary.count(k) == 0 {
			t.Errorf("key %s was never used", k)
		}
	}
}

// TestPool_DailyCapCountsAndSkips: the daily cap is per HTTP call, not per
// ticker. One request caps a key for the day; the pool then uses the other key,
// never oversending the capped one, and fails with ErrDailyLimit once both are
// capped.
func TestPool_DailyCapCountsAndSkips(t *testing.T) {
	summary := newCallLog()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(20, "2026-11-30T00:00:00", true)))
			return
		}
		summary.add(bearerKey(r))
		writeBrokerOK(t, w, 19)
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"key1", "key2"}, 0, newFakeClock())
	p.dailyLimit = 1
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); !errors.Is(err, broker.ErrDailyLimit) {
		t.Fatalf("Fetch 3 err = %v, want ErrDailyLimit", err)
	}
	if got := summary.total(); got != 2 {
		t.Errorf("summary calls = %d, want 2 (one per key, then stop)", got)
	}
	if got := summary.count("key1"); got != 1 {
		t.Errorf("key1 calls = %d, want 1", got)
	}
	if got := summary.count("key2"); got != 1 {
		t.Errorf("key2 calls = %d, want 1", got)
	}

	st := p.Status()
	if st.DailyRequests != 2 {
		t.Errorf("daily requests = %d, want 2", st.DailyRequests)
	}
	for _, k := range st.Keys {
		if !k.DailyCapped || k.DailyUsed != 1 || k.DailyRemaining != 0 {
			t.Errorf("key %s daily state = %+v, want capped 1/1", k.Label, k)
		}
		// The daily cap must not touch the monthly counter.
		if k.Remaining != 19 {
			t.Errorf("key %s monthly remaining = %d, want 19 untouched", k.Label, k.Remaining)
		}
	}
}

// TestPool_DailyCapResetsAtLocalMidnight: the cap parks until 00:00
// Asia/Jakarta, then the key is usable again without re-reading /usage.
func TestPool_DailyCapResetsAtLocalMidnight(t *testing.T) {
	var usageCalls int32
	summary := newCallLog()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			atomic.AddInt32(&usageCalls, 1)
			_, _ = w.Write([]byte(usageJSON(20, "2026-11-30T00:00:00", true)))
			return
		}
		summary.add(bearerKey(r))
		writeBrokerOK(t, w, 19)
	}))
	defer srv.Close()

	clock := newFakeClock()
	clock.Set(time.Date(2026, 10, 31, 23, 59, 0, 0, jakartaLocation()))
	p := newTestPool(t, srv.URL, []string{"only"}, 0, clock)
	p.dailyLimit = 1
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch before midnight: %v", err)
	}
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); !errors.Is(err, broker.ErrDailyLimit) {
		t.Fatalf("err before midnight = %v, want ErrDailyLimit", err)
	}

	clock.Advance(2 * time.Minute) // crosses 00:00 WIB
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch after midnight: %v", err)
	}
	if got := summary.total(); got != 2 {
		t.Errorf("summary calls = %d, want 2", got)
	}
	if got := atomic.LoadInt32(&usageCalls); got != 1 {
		t.Errorf("usage calls = %d, want 1 (midnight reset must not reseed)", got)
	}
	if got := p.Status().Keys[0].DailyUsed; got != 1 {
		t.Errorf("daily used after midnight = %d, want 1 (fresh day)", got)
	}
}

// TestPool_Daily403ParksUntilMidnightNotPeriodEnd: a daily rejection must not
// zero the monthly counter or park until period_end.
func TestPool_Daily403ParksUntilMidnightNotPeriodEnd(t *testing.T) {
	clock := newFakeClock()
	clock.Set(time.Date(2026, 10, 31, 12, 0, 0, 0, jakartaLocation()))

	summary := newCallLog()
	var dailyHit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := bearerKey(r)
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(124, "2026-11-27T12:58:02.465000", true)))
			return
		}
		summary.add(key)
		if key == "key1" && !dailyHit.Load() {
			dailyHit.Store(true)
			w.Header().Set("X-Monthly-Remaining", "124")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"detail":"Daily API limit exceeded"}`))
			return
		}
		writeBrokerOK(t, w, 123)
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"key1", "key2"}, 0, clock)
	p.dailyLimit = 5
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch (failover): %v", err)
	}
	st := p.Status()
	k1 := st.Keys[0]
	if !k1.DailyCapped {
		t.Errorf("key1 daily capped = false, want true: %+v", k1)
	}
	if k1.Exhausted {
		t.Errorf("key1 marked monthly-exhausted; daily cap must not do that: %+v", k1)
	}
	if k1.Remaining != 124 {
		t.Errorf("key1 remaining = %d, want 124 untouched", k1.Remaining)
	}

	// Still capped later the same day: key2 serves.
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	if got := summary.count("key1"); got != 1 {
		t.Errorf("key1 calls = %d, want 1", got)
	}

	// Next local day: key1 is usable again (server now returns 200).
	clock.Advance(24 * time.Hour)
	if _, err := p.Fetch(context.Background(), "BBRI", time.Now()); err != nil {
		t.Fatalf("Fetch next day: %v", err)
	}
	if got := summary.count("key1"); got != 2 {
		t.Errorf("key1 calls next day = %d, want 2", got)
	}
	if st := p.Status(); st.Keys[0].DailyCapped {
		t.Errorf("key1 still capped after midnight")
	}
}

// TestPool_DailyCapConcurrentNeverOversends is the concurrency guarantee: with
// two keys and a one-request daily cap, ten racing fetches must send exactly
// two HTTP calls and fail the rest with ErrDailyLimit.
func TestPool_DailyCapConcurrentNeverOversends(t *testing.T) {
	summary := newCallLog()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usage" {
			_, _ = w.Write([]byte(usageJSON(20, "2026-11-30T00:00:00", true)))
			return
		}
		summary.add(bearerKey(r))
		writeBrokerOK(t, w, 19)
	}))
	defer srv.Close()

	p := newTestPool(t, srv.URL, []string{"key1", "key2"}, 0, newFakeClock())
	p.dailyLimit = 1
	if err := p.Seed(context.Background()); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	var wg sync.WaitGroup
	var dailyErrs int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Fetch(context.Background(), "BBRI", time.Now())
			if errors.Is(err, broker.ErrDailyLimit) {
				atomic.AddInt32(&dailyErrs, 1)
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := summary.total(); got != 2 {
		t.Errorf("summary calls = %d, want 2 (one per key, cap respected)", got)
	}
	if got := atomic.LoadInt32(&dailyErrs); got != 8 {
		t.Errorf("daily errors = %d, want 8", got)
	}
}
