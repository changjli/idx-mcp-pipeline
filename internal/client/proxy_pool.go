package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// proxyPool is a rotating pool of egress proxies shared by the browser fetch
// modes (flaresolverr and nodriver). The list is loaded from an HTTPS URL or a
// local file path (JSON array of proxy URL strings), cached in-memory for ttl,
// and refreshed lazily. Dead proxies are skipped until deadRetryAfter elapses.
type proxyPool struct {
	mu             sync.Mutex
	source         string
	ttl            time.Duration
	deadRetryAfter time.Duration
	http           *http.Client
	log            *logrus.Logger

	proxies   []string
	dead      map[string]time.Time
	cursor    int
	lastFetch time.Time
}

func newProxyPool(source string, ttl, deadRetryAfter time.Duration, httpClient *http.Client, log *logrus.Logger) *proxyPool {
	if deadRetryAfter <= 0 {
		deadRetryAfter = time.Minute
	}
	return &proxyPool{
		source:         source,
		ttl:            ttl,
		deadRetryAfter: deadRetryAfter,
		http:           httpClient,
		log:            log,
		dead:           make(map[string]time.Time),
	}
}

// next returns the next live proxy, round-robin, skipping dead ones. Returns an
// error when the pool is empty or every proxy is dead.
func (p *proxyPool) next() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.refreshLocked(); err != nil {
		return "", err
	}
	if len(p.proxies) == 0 {
		return "", fmt.Errorf("proxy pool is empty (source=%s)", p.source)
	}

	now := time.Now()
	for i := 0; i < len(p.proxies); i++ {
		idx := (p.cursor + i) % len(p.proxies)
		proxy := p.proxies[idx]
		if until, dead := p.dead[proxy]; dead && until.After(now) {
			continue
		}
		p.cursor = (idx + 1) % len(p.proxies)
		return proxy, nil
	}
	return "", fmt.Errorf("all %d proxies are dead", len(p.proxies))
}

// markDead records a proxy as unusable until now+deadRetryAfter.
func (p *proxyPool) markDead(proxy string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dead[proxy] = time.Now().Add(p.deadRetryAfter)
	p.log.Warnf("proxy marked dead until %s: %s", p.dead[proxy].Format(time.RFC3339), proxy)
}

// refreshLocked reloads the proxy list if the cache is stale. Caller holds mu.
func (p *proxyPool) refreshLocked() error {
	if len(p.proxies) > 0 && time.Since(p.lastFetch) < p.ttl {
		return nil
	}
	proxies, err := p.load()
	if err != nil {
		return err
	}
	p.proxies = proxies
	p.lastFetch = time.Now()
	// Drop dead entries for proxies no longer in the list.
	for proxy := range p.dead {
		if !containsString(proxies, proxy) {
			delete(p.dead, proxy)
		}
	}
	return nil
}

// load fetches the proxy list from a URL or a local file.
func (p *proxyPool) load() ([]string, error) {
	var raw []byte
	var err error
	if strings.HasPrefix(p.source, "http://") || strings.HasPrefix(p.source, "https://") {
		resp, err := p.http.Get(p.source)
		if err != nil {
			return nil, fmt.Errorf("fetch proxy list: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("fetch proxy list: status %d", resp.StatusCode)
		}
		raw, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read proxy list: %w", err)
		}
	} else {
		raw, err = os.ReadFile(p.source)
		if err != nil {
			return nil, fmt.Errorf("read proxy list file: %w", err)
		}
	}

	var proxies []string
	if err := json.Unmarshal(raw, &proxies); err != nil {
		return nil, fmt.Errorf("parse proxy list: %w", err)
	}
	if len(proxies) == 0 {
		return nil, fmt.Errorf("proxy list is empty")
	}
	return proxies, nil
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
