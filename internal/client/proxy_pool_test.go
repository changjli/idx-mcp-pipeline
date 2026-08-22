package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func testPool(source string, ttl, dead time.Duration) *proxyPool {
	return newProxyPool(source, ttl, dead, &http.Client{Timeout: time.Second}, logrus.New())
}

func writeProxyFile(t *testing.T, proxies []string) string {
	t.Helper()
	dir := t.TempDir()
	f := filepath.Join(dir, "proxies.json")
	raw, _ := json.Marshal(proxies)
	if err := os.WriteFile(f, raw, 0o644); err != nil {
		t.Fatalf("write proxy list: %v", err)
	}
	return f
}

func TestProxyPool_RoundRobin(t *testing.T) {
	p := testPool(writeProxyFile(t, []string{"http://a:1", "http://b:2", "http://c:3"}), time.Hour, time.Hour)

	got := map[string]int{}
	for i := 0; i < 3; i++ {
		proxy, err := p.next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		got[proxy]++
	}
	if len(got) != 3 {
		t.Errorf("expected 3 distinct proxies, got %d", len(got))
	}
	// Round-robin: next after 3 wraps to the first.
	first, err := p.next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if first != "http://a:1" {
		t.Errorf("expected wrap to first proxy, got %s", first)
	}
}

func TestProxyPool_DeadSkipped(t *testing.T) {
	p := testPool(writeProxyFile(t, []string{"http://a:1", "http://b:2"}), time.Hour, time.Hour)

	p.markDead("http://a:1")
	proxy, err := p.next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if proxy != "http://b:2" {
		t.Errorf("expected b after a dead, got %s", proxy)
	}
}

func TestProxyPool_AllDead(t *testing.T) {
	p := testPool(writeProxyFile(t, []string{"http://a:1"}), time.Hour, time.Hour)

	p.markDead("http://a:1")
	if _, err := p.next(); err == nil {
		t.Fatal("expected error when all proxies dead")
	}
}

func TestProxyPool_DeadRevivesAfterRetryAfter(t *testing.T) {
	p := testPool(writeProxyFile(t, []string{"http://a:1"}), time.Hour, 50*time.Millisecond)

	p.markDead("http://a:1")
	if _, err := p.next(); err == nil {
		t.Fatal("expected dead proxy to be skipped")
	}
	time.Sleep(60 * time.Millisecond)
	proxy, err := p.next()
	if err != nil {
		t.Fatalf("next after retry-after: %v", err)
	}
	if proxy != "http://a:1" {
		t.Errorf("expected revived proxy, got %s", proxy)
	}
}

func TestProxyPool_LoadFromURL(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]string{"http://a:1", "http://b:2"})
	}))
	defer ts.Close()

	p := testPool(ts.URL, time.Hour, time.Hour)
	proxy, err := p.next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if proxy != "http://a:1" {
		t.Errorf("expected a, got %s", proxy)
	}
}

func TestProxyPool_URLCachedByTTL(t *testing.T) {
	var hits int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		json.NewEncoder(w).Encode([]string{"http://a:1"})
	}))
	defer ts.Close()

	p := testPool(ts.URL, time.Hour, time.Hour)
	if _, err := p.next(); err != nil {
		t.Fatalf("next: %v", err)
	}
	if _, err := p.next(); err != nil {
		t.Fatalf("next: %v", err)
	}
	if hits != 1 {
		t.Errorf("expected 1 fetch within TTL, got %d", hits)
	}
}
