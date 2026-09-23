package client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// nodriverStub mimics the nodriver sidecar /fetch + /health API for hermetic tests.
type nodriverStub struct {
	mu          sync.Mutex
	authToken   string
	healthReady bool
	requests    []nodriverRequest
	// per-proxy /fetch behavior
	proxyStatus map[string]int
	proxyBody   map[string]string
	proxyError  map[string]string
	// proxySeq drives per-proxy sequential replies, consumed in order; the
	// last entry repeats. A zero status maps to 200. Falls back to the static
	// maps when absent.
	proxySeq map[string][]stubReply
}

// stubReply is one scripted /fetch response for proxySeq.
type stubReply struct {
	status int
	body   string
	errMsg string
}

func newNodriverStub() *nodriverStub {
	return &nodriverStub{
		healthReady: true,
		proxyStatus: map[string]int{},
		proxyBody:   map[string]string{},
		proxyError:  map[string]string{},
		proxySeq:    map[string][]stubReply{},
	}
}

func (s *nodriverStub) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			s.mu.Lock()
			ready := s.healthReady
			s.mu.Unlock()
			if ready {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path != "/fetch" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if s.authToken != "" && r.Header.Get("Authorization") != "Bearer "+s.authToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var req nodriverRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.requests = append(s.requests, req)
		status := s.proxyStatus[req.Proxy]
		body := s.proxyBody[req.Proxy]
		errMsg := s.proxyError[req.Proxy]
		if seq := s.proxySeq[req.Proxy]; len(seq) > 0 {
			reply := seq[0]
			if len(seq) > 1 {
				s.proxySeq[req.Proxy] = seq[1:]
			}
			status, body, errMsg = reply.status, reply.body, reply.errMsg
		}
		s.mu.Unlock()
		if status == 0 {
			status = http.StatusOK
		}
		if req.Binary {
			// Mirror the sidecar: a binary fetch returns the body base64-encoded
			// with an explicit encoding marker.
			enc := base64.StdEncoding.EncodeToString([]byte(body))
			json.NewEncoder(w).Encode(nodriverResponse{Status: status, Body: enc, Error: errMsg, Encoding: "base64"})
			return
		}
		json.NewEncoder(w).Encode(nodriverResponse{Status: status, Body: body, Error: errMsg})
	})
}

func newTestNodriver(t *testing.T, stub *nodriverStub, proxies []string) *NodriverClient {
	t.Helper()
	ts := httptest.NewServer(stub.handler())
	t.Cleanup(ts.Close)
	pool := newProxyPool(writeProxyFile(t, proxies), time.Hour, time.Hour, &http.Client{Timeout: time.Second}, logrus.New())
	cfg := NodriverConfig{
		BaseURL:     ts.URL,
		Timeout:     time.Second,
		WakeTimeout: 2 * time.Second,
	}
	nc, err := NewNodriverClient(cfg, pool, logrus.New())
	if err != nil {
		t.Fatalf("NewNodriverClient: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestNodriver_Fetch_ExtractsBody(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyBody["http://a:1"] = `{"draw":1,"data":[{"StockCode":"BBCA"}]}`
	nc := newTestNodriver(t, stub, []string{"http://a:1"})

	body, status, err := nc.Fetch("https://idx.example/api", map[string]string{"Referer": "https://idx.example/x"})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("expected status 200, got %d", status)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("body not JSON: %v (%s)", err, body)
	}
	if got["draw"].(float64) != 1 {
		t.Errorf("expected draw=1, got %v", got["draw"])
	}
	// proxy + referer forwarded to sidecar
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) != 1 {
		t.Fatalf("expected 1 sidecar request, got %d", len(stub.requests))
	}
	if stub.requests[0].Proxy != "http://a:1" {
		t.Errorf("expected proxy forwarded, got %q", stub.requests[0].Proxy)
	}
	if stub.requests[0].Referer != "https://idx.example/x" {
		t.Errorf("expected referer forwarded, got %q", stub.requests[0].Referer)
	}
}

func TestNodriver_Fetch_502Rotates(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyStatus["http://a:1"] = http.StatusBadGateway // proxy_dead
	stub.proxyError["http://a:1"] = "proxy_dead"
	stub.proxyBody["http://b:2"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	body, status, err := nc.Fetch("https://idx.example/api", nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("expected 200 after rotation, got %d", status)
	}
	if string(body) != `{"ok":true}` {
		t.Errorf("expected b's body, got %s", body)
	}
	// a marked dead; next() skips it.
	proxy, err := nc.pool.next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if proxy != "http://b:2" {
		t.Errorf("expected b after a dead, got %s", proxy)
	}
}

func TestNodriver_Fetch_503ChallengeRotates(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyStatus["http://a:1"] = http.StatusServiceUnavailable // challenge_not_cleared
	stub.proxyError["http://a:1"] = "challenge_not_cleared"
	stub.proxyBody["http://b:2"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("expected 200 after rotating off challenge-blocked proxy, got status=%d err=%v", status, err)
	}
}

// TestNodriver_Fetch_TransientRetriesInPlace verifies the new rotation
// semantics: a 503 challenge flake is retried on the SAME proxy up to
// MaxProxyAttempts times before rotating, and a flake that clears on a
// re-attempt succeeds without touching other proxies.
func TestNodriver_Fetch_TransientRetriesInPlace(t *testing.T) {
	stub := newNodriverStub()
	stub.proxySeq["http://a:1"] = []stubReply{
		{status: http.StatusServiceUnavailable, errMsg: "challenge_not_cleared"},
		{status: http.StatusServiceUnavailable, errMsg: "challenge_not_cleared"},
		{status: http.StatusOK, body: `{"ok":true}`},
	}
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("expected 200 after in-place retry, got status=%d err=%v", status, err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) != 3 {
		t.Fatalf("expected 3 sidecar requests (2 flakes + 1 success), got %d", len(stub.requests))
	}
	for i, req := range stub.requests {
		if req.Proxy != "http://a:1" {
			t.Errorf("request %d should retry proxy a in place, got %s", i, req.Proxy)
		}
	}
}

// TestNodriver_Fetch_TransientRotatesWithoutBan verifies that a proxy whose
// transient attempts are spent rotates on WITHOUT being marked dead —
// Cloudflare flakiness is not a proxy property, so the proxy stays in
// rotation for later fetches.
func TestNodriver_Fetch_TransientRotatesWithoutBan(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyStatus["http://a:1"] = http.StatusServiceUnavailable
	stub.proxyError["http://a:1"] = "challenge_not_cleared"
	stub.proxyBody["http://b:2"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("expected 200 after rotating off spent transient proxy, got status=%d err=%v", status, err)
	}
	if got := nc.pool.live(); got != 2 {
		t.Errorf("expected both proxies still live (no ban on transient), got %d", got)
	}
}

// TestNodriver_Fetch_TargetStatusSurfaces verifies that a deterministic
// passthrough failure (target 404) surfaces to the caller immediately —
// one request, no rotation, no ban.
func TestNodriver_Fetch_TargetStatusSurfaces(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyStatus["http://a:1"] = http.StatusNotFound // target passthrough, not a sidecar class
	stub.proxyBody["http://b:2"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	_, _, err := nc.Fetch("https://idx.example/api", nil)
	if err == nil {
		t.Fatal("expected error on target 404")
	}
	if !strings.Contains(err.Error(), "status 404") {
		t.Errorf("expected surfaced 404, got %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) != 1 {
		t.Fatalf("expected 1 sidecar request (no rotation on deterministic failure), got %d", len(stub.requests))
	}
	if got := nc.pool.live(); got != 2 {
		t.Errorf("expected both proxies still live (no ban on target status), got %d", got)
	}
}

// TestNodriver_Fetch_StickyReusesProxy verifies the core sticky contract: once
// a proxy succeeds, later fetches reuse it without advancing the round-robin
// cursor, so the sidecar's warm Chrome keeps one Cloudflare clearance instead of
// paying a fresh challenge solve per request.
func TestNodriver_Fetch_StickyReusesProxy(t *testing.T) {
	stub := newNodriverStub()
	for _, p := range []string{"http://a:1", "http://b:2", "http://c:3"} {
		stub.proxyBody[p] = `{"ok":true}`
	}
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2", "http://c:3"})

	for i := 0; i < 3; i++ {
		if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
			t.Fatalf("fetch %d: status=%d err=%v", i, status, err)
		}
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) != 3 {
		t.Fatalf("expected 3 sidecar requests (one per fetch), got %d", len(stub.requests))
	}
	for i, req := range stub.requests {
		if req.Proxy != "http://a:1" {
			t.Errorf("fetch %d should reuse the sticky proxy a, got %s", i, req.Proxy)
		}
	}
}

// TestNodriver_Fetch_StickyRotatesAfterMaxSuccesses verifies the deliberate
// rotation: after maxStickySuccesses consecutive successes the proxy is
// released and the next fetch moves to the next live proxy, so one IP never
// carries a whole backfill.
func TestNodriver_Fetch_StickyRotatesAfterMaxSuccesses(t *testing.T) {
	stub := newNodriverStub()
	for _, p := range []string{"http://a:1", "http://b:2", "http://c:3"} {
		stub.proxyBody[p] = `{"ok":true}`
	}
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2", "http://c:3"})
	nc.maxStickySuccesses = 2

	for i := 0; i < 5; i++ {
		if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
			t.Fatalf("fetch %d: status=%d err=%v", i, status, err)
		}
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	want := []string{"http://a:1", "http://a:1", "http://b:2", "http://b:2", "http://c:3"}
	if len(stub.requests) != len(want) {
		t.Fatalf("expected %d sidecar requests, got %d", len(want), len(stub.requests))
	}
	for i, req := range stub.requests {
		if req.Proxy != want[i] {
			t.Errorf("fetch %d used %s, want %s", i, req.Proxy, want[i])
		}
	}
}

// TestNodriver_Fetch_StickyDroppedOnDeadProxy verifies a dead sticky proxy is
// banned and rotated off exactly as before stickiness existed: the next fetch
// tries it once, gets 502 proxy_dead, and lands on the next live proxy, which
// then becomes sticky itself.
func TestNodriver_Fetch_StickyDroppedOnDeadProxy(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyBody["http://a:1"] = `{"ok":true}`
	stub.proxyBody["http://b:2"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	if _, _, err := nc.Fetch("https://idx.example/api", nil); err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	stub.mu.Lock()
	stub.proxyStatus["http://a:1"] = http.StatusBadGateway
	stub.proxyError["http://a:1"] = "proxy_dead"
	stub.mu.Unlock()

	// Second fetch: sticky a is live, so it is retried — and dies.
	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("second fetch: status=%d err=%v", status, err)
	}
	// Third fetch: b is now sticky.
	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("third fetch: status=%d err=%v", status, err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	want := []string{"http://a:1", "http://a:1", "http://b:2", "http://b:2"}
	if len(stub.requests) != len(want) {
		t.Fatalf("expected %d sidecar requests, got %d", len(want), len(stub.requests))
	}
	for i, req := range stub.requests {
		if req.Proxy != want[i] {
			t.Errorf("request %d used %s, want %s", i, req.Proxy, want[i])
		}
	}
	if got := nc.pool.live(); got != 1 {
		t.Errorf("expected a banned (1 live proxy), got %d", got)
	}
}

// TestNodriver_Fetch_StickyDroppedOnTransientRotation verifies that a sticky
// proxy whose transient attempts are spent stops being reused: the rotation
// moves on without a ban, and the newly succeeded proxy becomes sticky.
func TestNodriver_Fetch_StickyDroppedOnTransientRotation(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyStatus["http://a:1"] = http.StatusServiceUnavailable
	stub.proxyError["http://a:1"] = "challenge_not_cleared"
	stub.proxyBody["http://b:2"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	// Fetch 1: a flakes 3x (MaxProxyAttempts), rotates to b, b succeeds.
	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("first fetch: status=%d err=%v", status, err)
	}
	// Fetch 2: b is sticky — one request, no a retries.
	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("second fetch: status=%d err=%v", status, err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	want := []string{"http://a:1", "http://a:1", "http://a:1", "http://b:2", "http://b:2"}
	if len(stub.requests) != len(want) {
		t.Fatalf("expected %d sidecar requests, got %d", len(want), len(stub.requests))
	}
	for i, req := range stub.requests {
		if req.Proxy != want[i] {
			t.Errorf("request %d used %s, want %s", i, req.Proxy, want[i])
		}
	}
	if got := nc.pool.live(); got != 2 {
		t.Errorf("expected both proxies live (no ban on transient), got %d", got)
	}
}

// TestNodriver_Fetch_StickyPoolExhaustionUnchanged verifies the all-dead path
// still ends in the same exhausted error — stickiness must not extend the
// attempt budget.
func TestNodriver_Fetch_StickyPoolExhaustionUnchanged(t *testing.T) {
	stub := newNodriverStub()
	for _, p := range []string{"http://a:1", "http://b:2"} {
		stub.proxyStatus[p] = http.StatusBadGateway
		stub.proxyError[p] = "proxy_dead"
	}
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	if _, _, err := nc.Fetch("https://idx.example/api", nil); err == nil {
		t.Fatal("expected error when every proxy is dead")
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) != 2 {
		t.Fatalf("expected 2 sidecar attempts (one per dead proxy), got %d", len(stub.requests))
	}
}

// TestNodriver_DefaultMaxStickySuccesses verifies the config default applies
// when the knob is unset.
func TestNodriver_DefaultMaxStickySuccesses(t *testing.T) {
	nc := newTestNodriver(t, newNodriverStub(), []string{"http://a:1"})
	if nc.maxStickySuccesses != defaultMaxStickySuccesses {
		t.Errorf("expected default max sticky successes %d, got %d", defaultMaxStickySuccesses, nc.maxStickySuccesses)
	}
	if defaultMaxStickySuccesses != 10 {
		t.Errorf("expected shipped default 10, got %d", defaultMaxStickySuccesses)
	}
}

// TestConfigFromViper_MaxStickySuccesses verifies the viper wiring for the
// deliberate-rotation knob.
func TestConfigFromViper_MaxStickySuccesses(t *testing.T) {
	vip := viper.New()
	if got := ConfigFromViper(vip).Nodriver.MaxStickySuccesses; got != 10 {
		t.Errorf("expected default 10, got %d", got)
	}
	vip.Set("nodriver.max_sticky_successes", 4)
	if got := ConfigFromViper(vip).Nodriver.MaxStickySuccesses; got != 4 {
		t.Errorf("expected 4 from viper, got %d", got)
	}
}

// TestNodriver_Fetch_StickyRotationWrapsRoundRobin pins what happens after a
// proxy's success cap: the released proxy is not banned, it re-enters only
// after the cursor wraps past every live proxy — one full cycle later.
func TestNodriver_Fetch_StickyRotationWrapsRoundRobin(t *testing.T) {
	stub := newNodriverStub()
	for _, p := range []string{"http://a:1", "http://b:2", "http://c:3"} {
		stub.proxyBody[p] = `{"ok":true}`
	}
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2", "http://c:3"})
	nc.maxStickySuccesses = 2

	for i := 0; i < 7; i++ {
		if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
			t.Fatalf("fetch %d: status=%d err=%v", i, status, err)
		}
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	want := []string{
		"http://a:1", "http://a:1",
		"http://b:2", "http://b:2",
		"http://c:3", "http://c:3",
		"http://a:1", // wrapped: a is back, still live (release is not a ban)
	}
	if len(stub.requests) != len(want) {
		t.Fatalf("expected %d sidecar requests, got %d", len(want), len(stub.requests))
	}
	for i, req := range stub.requests {
		if req.Proxy != want[i] {
			t.Errorf("fetch %d used %s, want %s", i, req.Proxy, want[i])
		}
	}
	if got := nc.pool.live(); got != 3 {
		t.Errorf("expected all 3 proxies still live (cap release is not a ban), got %d", got)
	}
}

// TestNodriver_Fetch_StickyRotationWrapsToSoleSurvivor pins the one case where
// the released proxy comes straight back: every other proxy is quarantined, so
// the dead-skip walk lands on it again. Reusing the only usable IP beats
// failing the fetch.
func TestNodriver_Fetch_StickyRotationWrapsToSoleSurvivor(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyBody["http://a:1"] = `{"ok":true}`
	stub.proxyStatus["http://b:2"] = http.StatusBadGateway
	stub.proxyError["http://b:2"] = "proxy_dead"
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})
	nc.maxStickySuccesses = 1

	// Fetch 1: a succeeds, hits the cap of 1 and is released.
	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("first fetch: status=%d err=%v", status, err)
	}
	// Fetch 2: rotation walks to b, which is dead-marked by then.
	nc.pool.markDead("http://b:2")
	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("second fetch: status=%d err=%v", status, err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	for i, req := range stub.requests {
		if req.Proxy != "http://a:1" {
			t.Errorf("request %d used %s, want the sole surviving proxy a", i, req.Proxy)
		}
	}
}

// TestNodriver_Fetch_AttemptBudgetExhausts verifies the loop terminates when
// every proxy keeps failing transiently: 2 proxies x MaxProxyAttempts (3)
// = 6 requests, then an exhausted error.
func TestNodriver_Fetch_AttemptBudgetExhausts(t *testing.T) {
	stub := newNodriverStub()
	for _, p := range []string{"http://a:1", "http://b:2"} {
		stub.proxyStatus[p] = http.StatusServiceUnavailable
		stub.proxyError[p] = "challenge_not_cleared"
	}
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	_, _, err := nc.Fetch("https://idx.example/api", nil)
	if err == nil {
		t.Fatal("expected exhausted error when every attempt fails transiently")
	}
	if !strings.Contains(err.Error(), "all proxies exhausted after 6 attempts") {
		t.Errorf("expected budget-exhausted error, got %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) != 6 {
		t.Fatalf("expected 6 sidecar attempts (2 proxies x 3), got %d", len(stub.requests))
	}
}

func TestNodriver_Fetch_AllDeadExhausts(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyStatus["http://a:1"] = http.StatusBadGateway
	stub.proxyError["http://a:1"] = "proxy_dead"
	stub.proxyStatus["http://b:2"] = http.StatusBadGateway
	stub.proxyError["http://b:2"] = "proxy_dead"
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	if _, _, err := nc.Fetch("https://idx.example/api", nil); err == nil {
		t.Fatal("expected error when every proxy is dead")
	}
}

func TestNodriver_Fetch_AuthHeader(t *testing.T) {
	stub := newNodriverStub()
	stub.authToken = "secret"
	stub.proxyBody["http://a:1"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1"})
	nc.authToken = "secret"

	if _, _, err := nc.Fetch("https://idx.example/api", nil); err != nil {
		t.Fatalf("Fetch with auth: %v", err)
	}
}

func TestNodriver_Wake(t *testing.T) {
	stub := newNodriverStub()
	stub.healthReady = false
	stub.proxyBody["http://a:1"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1"})

	go func() {
		time.Sleep(400 * time.Millisecond)
		stub.mu.Lock()
		stub.healthReady = true
		stub.mu.Unlock()
	}()

	if _, _, err := nc.Fetch("https://idx.example/api", nil); err != nil {
		t.Fatalf("Fetch after wake: %v", err)
	}
}

// TestNodriver_Fetch_EmptyBodyRetriesThenRotates verifies an empty body is
// treated as transient: retried in place, then rotated off to a healthy proxy.
func TestNodriver_Fetch_EmptyBodyRetriesThenRotates(t *testing.T) {
	stub := newNodriverStub()
	stub.proxyStatus["http://a:1"] = http.StatusOK
	stub.proxyBody["http://a:1"] = "" // empty body => treat as failure, rotate
	stub.proxyBody["http://b:2"] = `{"ok":true}`
	nc := newTestNodriver(t, stub, []string{"http://a:1", "http://b:2"})

	if _, status, err := nc.Fetch("https://idx.example/api", nil); err != nil || status != http.StatusOK {
		t.Fatalf("expected 200 after rotating off empty-body proxy, got status=%d err=%v", status, err)
	}
}
func TestNodriver_FetchBinary_DecodesBase64(t *testing.T) {
	stub := newNodriverStub()
	pdf := []byte("%PDF-1.6 fake disclosure \xff\xfe binary bytes")
	stub.proxyBody["http://a:1"] = string(pdf)
	nc := newTestNodriver(t, stub, []string{"http://a:1"})

	body, status, err := nc.FetchBinary(
		"https://idx.example/StaticData/x.pdf",
		map[string]string{"Referer": "https://idx.example/", "Range": "bytes=0-0"},
	)
	if err != nil {
		t.Fatalf("FetchBinary: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("expected status 200, got %d", status)
	}
	if !bytes.Equal(body, pdf) {
		t.Errorf("expected decoded PDF bytes %q, got %q", pdf, body)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) != 1 {
		t.Fatalf("expected 1 sidecar request, got %d", len(stub.requests))
	}
	if !stub.requests[0].Binary {
		t.Error("expected binary flag forwarded to sidecar")
	}
	if stub.requests[0].Referer != "https://idx.example/" {
		t.Errorf("expected referer forwarded, got %q", stub.requests[0].Referer)
	}
	if stub.requests[0].Headers["Range"] != "bytes=0-0" {
		t.Errorf("expected Range header forwarded, got %q", stub.requests[0].Headers["Range"])
	}
}

func TestNodriver_FetchBinary_AcceptsPartialContent(t *testing.T) {
	// A ranged size probe returns 206; that is a success for binary fetches,
	// unlike the text path which requires exactly 200.
	stub := newNodriverStub()
	stub.proxyStatus["http://a:1"] = http.StatusPartialContent
	stub.proxyBody["http://a:1"] = "\x25"
	nc := newTestNodriver(t, stub, []string{"http://a:1"})

	body, status, err := nc.FetchBinary("https://idx.example/StaticData/x.pdf", nil)
	if err != nil {
		t.Fatalf("FetchBinary: %v", err)
	}
	if status != http.StatusPartialContent {
		t.Errorf("expected status 206, got %d", status)
	}
	if !bytes.Equal(body, []byte("\x25")) {
		t.Errorf("expected partial body, got %q", body)
	}
}
