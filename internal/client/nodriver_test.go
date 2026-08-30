package client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
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
}

func newNodriverStub() *nodriverStub {
	return &nodriverStub{
		healthReady: true,
		proxyStatus: map[string]int{},
		proxyBody:   map[string]string{},
		proxyError:  map[string]string{},
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

func TestNodriver_Fetch_EmptyBodyExhausts(t *testing.T) {
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
