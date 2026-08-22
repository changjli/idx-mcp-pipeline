package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// flareStub mimics the FlareSolverr /v1 API for hermetic tests.
type flareStub struct {
	mu            sync.Mutex
	requests      []map[string]any
	sessions      map[string]string // sessionID -> proxy
	authToken     string
	healthReady   bool
	envelopeError bool
	errorMessage  string
	// per-proxy request.get behavior
	proxyStatus   map[string]int
	proxyResponse map[string]string
}

func newFlareStub() *flareStub {
	return &flareStub{
		sessions:      map[string]string{},
		healthReady:   true,
		proxyStatus:   map[string]int{},
		proxyResponse: map[string]string{},
	}
}

func (s *flareStub) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			s.mu.Lock()
			ready := s.healthReady
			s.mu.Unlock()
			if ready {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{"msg": "FlareSolverr is ready!"})
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		if s.authToken != "" && r.Header.Get("Authorization") != "Bearer "+s.authToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.requests = append(s.requests, req)
		s.mu.Unlock()

		switch req["cmd"] {
		case "sessions.create":
			proxy := ""
			if p, ok := req["proxy"].(map[string]any); ok {
				proxy, _ = p["url"].(string)
			}
			id := "sess-" + proxy
			s.sessions[id] = proxy
			json.NewEncoder(w).Encode(map[string]any{"status": "ok", "session": id})
		case "sessions.destroy":
			json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case "request.get":
			s.mu.Lock()
			envErr := s.envelopeError
			msg := s.errorMessage
			sess, _ := req["session"].(string)
			proxy := s.sessions[sess]
			status := s.proxyStatus[proxy]
			response := s.proxyResponse[proxy]
			s.mu.Unlock()
			if status == 0 {
				status = http.StatusOK
			}
			if envErr {
				json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": msg})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"status": "ok",
				"solution": map[string]any{
					"status":   status,
					"response": response,
				},
			})
		default:
			json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": "unknown cmd"})
		}
	})
}

func newTestFlare(t *testing.T, stub *flareStub, proxies []string) *FlareSolverrClient {
	t.Helper()
	ts := httptest.NewServer(stub.handler())
	t.Cleanup(ts.Close)
	dir := t.TempDir()
	f := filepath.Join(dir, "proxies.json")
	raw, _ := json.Marshal(proxies)
	if err := os.WriteFile(f, raw, 0o644); err != nil {
		t.Fatalf("write proxy list: %v", err)
	}
	cfg := FlareSolverrConfig{
		BaseURL:           ts.URL,
		Timeout:           time.Second,
		Proxies:           f,
		ProxiesTTL:        time.Hour,
		DeadRetryAfter:    time.Hour,
		WakeTimeout:       2 * time.Second,
		SessionTTLMinutes: 30,
	}
	fc, err := NewFlareSolverrClient(cfg, logrus.New())
	if err != nil {
		t.Fatalf("NewFlareSolverrClient: %v", err)
	}
	t.Cleanup(fc.Close)
	return fc
}

func TestFlareSolverr_Fetch_ExtractsJSONFromPre(t *testing.T) {
	stub := newFlareStub()
	stub.proxyResponse["http://a:1"] = `<html><body><pre>{"draw":1,"data":[{"StockCode":"BBCA"}]}</pre></body></html>`
	fc := newTestFlare(t, stub, []string{"http://a:1"})

	body, status, err := fc.Fetch("https://idx.example/api", map[string]string{"Referer": "https://idx.example/x"})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("expected status 200, got %d", status)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("extracted body not JSON: %v (%s)", err, body)
	}
	if got["draw"].(float64) != 1 {
		t.Errorf("expected draw=1, got %v", got["draw"])
	}
}

func TestFlareSolverr_Fetch_SendsSessionAndHeaders(t *testing.T) {
	stub := newFlareStub()
	stub.proxyResponse["http://a:1"] = `{"ok":true}`
	fc := newTestFlare(t, stub, []string{"http://a:1"})

	if _, _, err := fc.Fetch("https://idx.example/api", map[string]string{"Referer": "https://idx.example/x"}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	var get map[string]any
	for _, req := range stub.requests {
		if req["cmd"] == "request.get" {
			get = req
		}
	}
	if get == nil {
		t.Fatal("no request.get sent")
	}
	if get["session"] == "" {
		t.Error("request.get missing session")
	}
	if get["session_ttl_minutes"].(float64) != 30 {
		t.Errorf("expected session_ttl_minutes=30, got %v", get["session_ttl_minutes"])
	}
	headers, _ := get["headers"].(map[string]any)
	if headers["Referer"] != "https://idx.example/x" {
		t.Errorf("expected Referer header, got %v", headers)
	}
}

func TestFlareSolverr_Fetch_403Rotates(t *testing.T) {
	stub := newFlareStub()
	stub.proxyStatus["http://a:1"] = http.StatusForbidden
	stub.proxyResponse["http://a:1"] = "<html>Just a moment...</html>"
	stub.proxyResponse["http://b:2"] = `{"ok":true}`
	fc := newTestFlare(t, stub, []string{"http://a:1", "http://b:2"})

	body, status, err := fc.Fetch("https://idx.example/api", nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("expected 200 after rotation, got %d", status)
	}
	if string(body) != `{"ok":true}` {
		t.Errorf("expected b's body, got %s", body)
	}
	// a must be marked dead; next() skips it.
	proxy, err := fc.pool.next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if proxy != "http://b:2" {
		t.Errorf("expected b after a marked dead, got %s", proxy)
	}
}

func TestFlareSolverr_Fetch_EnvelopeErrorExhausts(t *testing.T) {
	stub := newFlareStub()
	stub.envelopeError = true
	stub.errorMessage = "Error solving the challenge. Timeout."
	fc := newTestFlare(t, stub, []string{"http://a:1", "http://b:2"})

	if _, _, err := fc.Fetch("https://idx.example/api", nil); err == nil {
		t.Fatal("expected error when FlareSolverr errors on every proxy")
	}
}

func TestFlareSolverr_Fetch_AuthHeader(t *testing.T) {
	stub := newFlareStub()
	stub.authToken = "secret"
	stub.proxyResponse["http://a:1"] = `{"ok":true}`
	fc := newTestFlare(t, stub, []string{"http://a:1"})
	fc.authToken = "secret"

	if _, _, err := fc.Fetch("https://idx.example/api", nil); err != nil {
		t.Fatalf("Fetch with auth: %v", err)
	}
}

func TestFlareSolverr_Wake(t *testing.T) {
	stub := newFlareStub()
	stub.healthReady = false
	stub.proxyResponse["http://a:1"] = `{"ok":true}`
	fc := newTestFlare(t, stub, []string{"http://a:1"})

	// Simulate the container waking up shortly after the first probe.
	go func() {
		time.Sleep(500 * time.Millisecond)
		stub.mu.Lock()
		stub.healthReady = true
		stub.mu.Unlock()
	}()

	if _, _, err := fc.Fetch("https://idx.example/api", nil); err != nil {
		t.Fatalf("Fetch after wake: %v", err)
	}
}

func TestExtractJSON(t *testing.T) {
	cases := []struct {
		name     string
		response string
		want     string
		wantErr  bool
	}{
		{"pre-wrapped", `<html><body><pre>{"a":1}</pre></body></html>`, `{"a":1}`, false},
		{"direct", `{"a":1}`, `{"a":1}`, false},
		{"slicing fallback", `prefix {"a":1} suffix`, `{"a":1}`, false},
		{"challenge page with js", `<html>Just a moment...<script>var x={y:1};</script></html>`, "", true},
		{"challenge page plain", `<html>Just a moment...</html>`, "", true},
		{"empty", ``, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractJSON(tc.response)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("extractJSON: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("expected %q, got %q", tc.want, got)
			}
		})
	}
}
