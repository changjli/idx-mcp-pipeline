package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// nodriverRequest is the body POSTed to the nodriver sidecar /fetch endpoint.
type nodriverRequest struct {
	URL     string            `json:"url"`
	Proxy   string            `json:"proxy"`
	Referer string            `json:"referer,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Timeout int               `json:"timeout_ms,omitempty"`
}

// nodriverResponse is the sidecar's reply.
type nodriverResponse struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
	Error  string `json:"error,omitempty"`
}

// NodriverClient wraps the nodriver sidecar HTTP API. It solves Cloudflare
// challenges in a single long-lived headless Chrome (held by the sidecar)
// egressing through a rotating proxy pool shared with FlareSolverr, and returns
// the page bytes + status. One warm Chrome on the sidecar => all calls here are
// serialized by a mutex.
type NodriverClient struct {
	baseURL     string
	authToken   string
	timeout     time.Duration
	wakeTimeout time.Duration
	pool        *proxyPool
	http        *http.Client
	log         *logrus.Logger

	mu sync.Mutex
}

// NewNodriverClient builds a nodriver client. base_url is required; the proxy
// list (source/ttl/deadRetryAfter) is shared with the FlareSolverr config so one
// pool config serves both browser modes. The proxy list loads lazily on first
// fetch.
func NewNodriverClient(cfg NodriverConfig, proxySource string, proxyTTL, deadRetryAfter time.Duration, log *logrus.Logger) (*NodriverClient, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("nodriver.base_url is required in nodriver fetch mode")
	}
	if proxySource == "" {
		return nil, fmt.Errorf("flaresolverr.proxies is required in nodriver fetch mode (shared proxy pool)")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	httpClient := &http.Client{Timeout: timeout + 10*time.Second}
	return &NodriverClient{
		baseURL:     strings.TrimRight(cfg.BaseURL, "/"),
		authToken:   cfg.AuthToken,
		timeout:     timeout,
		wakeTimeout: cfg.WakeTimeout,
		pool:        newProxyPool(proxySource, proxyTTL, deadRetryAfter, httpClient, log),
		http:        httpClient,
		log:         log,
	}, nil
}

// Fetch retrieves url through the nodriver sidecar, rotating proxies until one
// succeeds or the pool is exhausted. Returns the page bytes and HTTP status.
func (n *NodriverClient) Fetch(url string, headers map[string]string) ([]byte, int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.wake(); err != nil {
		return nil, 0, err
	}

	referer := ""
	if headers != nil {
		referer = headers["Referer"]
	}

	var lastErr error
	for {
		proxy, err := n.pool.next()
		if err != nil {
			if lastErr != nil {
				return nil, 0, fmt.Errorf("all proxies exhausted: %w (last: %v)", err, lastErr)
			}
			return nil, 0, err
		}

		body, status, err := n.fetchViaProxy(url, referer, headers, proxy)
		if err != nil {
			n.pool.markDead(proxy)
			lastErr = err
			continue
		}
		return body, status, nil
	}
}

// fetchViaProxy runs one /fetch call through a proxy.
func (n *NodriverClient) fetchViaProxy(url, referer string, headers map[string]string, proxy string) ([]byte, int, error) {
	req := nodriverRequest{
		URL:     url,
		Proxy:   proxy,
		Referer: referer,
		Headers: headers,
		Timeout: int(n.timeout / time.Millisecond),
	}
	raw, err := n.post(req)
	if err != nil {
		return nil, 0, err
	}

	var resp nodriverResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, 0, fmt.Errorf("parse nodriver response: %w", err)
	}
	if resp.Status != http.StatusOK {
		return nil, resp.Status, fmt.Errorf("nodriver fetch failed via %s: %s (status %d)", proxy, resp.Error, resp.Status)
	}
	if resp.Body == "" {
		return nil, resp.Status, fmt.Errorf("nodriver fetch returned empty body via %s", proxy)
	}
	return []byte(resp.Body), resp.Status, nil
}

// wake polls the sidecar health endpoint until it is ready. Covers free-tier
// auto-sleep; the sidecar cold-start is Chrome-only, far faster than FlareSolverr.
func (n *NodriverClient) wake() error {
	deadline := time.Now().Add(n.wakeTimeout)
	for {
		req, err := http.NewRequest(http.MethodGet, n.baseURL+"/health", nil)
		if err != nil {
			return err
		}
		// /health is public (Caddy-exempt); no auth header needed.
		resp, err := n.http.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("nodriver sidecar not ready after %v", n.wakeTimeout)
		}
		time.Sleep(2 * time.Second)
	}
}

// post sends a /fetch request to the nodriver sidecar.
func (n *NodriverClient) post(req nodriverRequest) ([]byte, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequest(http.MethodPost, n.baseURL+"/fetch", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if n.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+n.authToken)
	}
	resp, err := n.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("nodriver request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read nodriver response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("nodriver sidecar HTTP %d: %s", resp.StatusCode, truncateBytes(body, 200))
	}
	return body, nil
}

// Close is a no-op for the nodriver client (the Chrome lifecycle is owned by the
// sidecar). Present to satisfy the browserFetcher interface alongside
// FlareSolverrClient.
func (n *NodriverClient) Close() {}