package client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// streamFetchTimeout is the per-request budget for binary stream fetches
// (disclosure PDFs up to 10MB over a rotating proxy) — substantially longer
// than n.timeout, which suits small JSON pages.
const streamFetchTimeout = 2 * time.Minute

// defaultMaxProxyAttempts is the in-place retry cap when the config leaves
// MaxProxyAttempts unset.
const defaultMaxProxyAttempts = 3

// defaultMaxStickySuccesses is the deliberate-rotation cap when the config
// leaves MaxStickySuccesses unset.
const defaultMaxStickySuccesses = 10

// fetchFailureClass tells the rotation loop how to react to a failed sidecar
// fetch: ban the proxy, retry it in place, or surface the failure without
// burning rotation.
type fetchFailureClass int

const (
	// failProxyDead — sidecar 502 proxy_dead: Chrome could not use the proxy
	// at all. The proxy leaves rotation until deadRetryAfter elapses.
	failProxyDead fetchFailureClass = iota
	// failTransient — sidecar 403 blocked / 503 challenge_not_cleared /
	// 504 timeout: the proxy is reachable but the Cloudflare challenge solve
	// is stochastic per attempt, so the same proxy is retried in place before
	// rotating on (without a ban — flakiness is not a proxy property).
	failTransient
	// failTarget — any other non-success status the sidecar passed through
	// (a target 404/5xx) or a sidecar-level response problem: rotating proxies
	// cannot change the outcome, so the error surfaces to the caller directly.
	failTarget
)

// fetchError wraps a failed sidecar fetch with its rotation class.
type fetchError struct {
	class fetchFailureClass
	err   error
}

func (e *fetchError) Error() string { return e.err.Error() }
func (e *fetchError) Unwrap() error { return e.err }

// classifySidecarStatus maps a sidecar-reported failure status to its rotation
// class (server.py documents the codes: 502 proxy_dead, 503
// challenge_not_cleared, 504 timeout, 403 blocked).
func classifySidecarStatus(status int) fetchFailureClass {
	switch status {
	case http.StatusBadGateway:
		return failProxyDead
	case http.StatusForbidden, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return failTransient
	default:
		return failTarget
	}
}

// nodriverRequest is the body POSTed to the nodriver sidecar /fetch endpoint.
type nodriverRequest struct {
	URL     string            `json:"url"`
	Proxy   string            `json:"proxy"`
	Referer string            `json:"referer,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Timeout int               `json:"timeout_ms,omitempty"`
	Binary  bool              `json:"binary,omitempty"`
}

// nodriverResponse is the sidecar's reply. For binary fetches the sidecar sets
// Encoding to "base64" and Body holds the base64-encoded payload.
type nodriverResponse struct {
	Status   int    `json:"status"`
	Body     string `json:"body"`
	Error    string `json:"error,omitempty"`
	Encoding string `json:"encoding,omitempty"`
}

// NodriverClient wraps the nodriver sidecar HTTP API. It solves Cloudflare
// challenges in a single long-lived headless Chrome (held by the sidecar)
// egressing through a rotating proxy pool shared with FlareSolverr, and returns
// the page bytes + status. One warm Chrome on the sidecar => all calls here are
// serialized by a mutex.
//
// Proxies are sticky-until-dead: the last proxy that succeeded is reused for
// subsequent fetches, and the round-robin cursor only advances when that proxy
// dies, flunks out transiently, or reaches maxStickySuccesses. Cloudflare's
// clearance cookie is IP-bound, so reusing the egress IP is what lets the
// sidecar skip a fresh 60-250s challenge solve on every request; the success
// cap bounds the rate-limit risk of one IP carrying a whole backfill.
type NodriverClient struct {
	baseURL            string
	authToken          string
	timeout            time.Duration
	wakeTimeout        time.Duration
	maxProxyAttempts   int
	maxStickySuccesses int
	pool               *proxyPool
	http               *http.Client
	log                *logrus.Logger

	// stickyProxy / stickySuccesses track the reused egress proxy. Both are
	// guarded by mu, like the rest of the rotation loop.
	stickyProxy     string
	stickySuccesses int

	mu sync.Mutex
}

// NewNodriverClient builds a nodriver client. base_url is required; the proxy
// pool is injected (built once in NewClient from the nodriver.proxies config).
// The proxy list loads lazily on first fetch.
func NewNodriverClient(cfg NodriverConfig, pool *proxyPool, log *logrus.Logger) (*NodriverClient, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("nodriver.base_url is required in nodriver fetch mode")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	attempts := cfg.MaxProxyAttempts
	if attempts <= 0 {
		attempts = defaultMaxProxyAttempts
	}
	sticky := cfg.MaxStickySuccesses
	if sticky <= 0 {
		sticky = defaultMaxStickySuccesses
	}
	httpClient := &http.Client{Timeout: timeout + 10*time.Second}
	return &NodriverClient{
		baseURL:            strings.TrimRight(cfg.BaseURL, "/"),
		authToken:          cfg.AuthToken,
		timeout:            timeout,
		wakeTimeout:        cfg.WakeTimeout,
		maxProxyAttempts:   attempts,
		maxStickySuccesses: sticky,
		pool:               pool,
		http:               httpClient,
		log:                log,
	}, nil
}

// Fetch retrieves url through the nodriver sidecar, rotating proxies until one
// succeeds or the pool is exhausted. Returns the page bytes and HTTP status.
func (n *NodriverClient) Fetch(url string, headers map[string]string) ([]byte, int, error) {
	return n.fetch(url, headers, false, n.timeout)
}

// FetchBinary retrieves binary content (disclosure PDFs) through the sidecar's
// base64 transport. Unlike Fetch it accepts any sub-400 status — a 206 partial
// response from a ranged size probe is a success. Uses streamFetchTimeout as
// the per-request budget; PDFs run up to 10MB over a rotating proxy.
func (n *NodriverClient) FetchBinary(url string, headers map[string]string) ([]byte, int, error) {
	return n.fetch(url, headers, true, streamFetchTimeout)
}

// fetch is the shared rotation loop for Fetch/FetchBinary: it retries a proxy
// in place on transient Cloudflare flakes and rotates proxies until one
// succeeds or the attempt budget is spent. A successful proxy is kept for
// later fetches (sticky-until-dead) so the sidecar's Cloudflare clearance is
// reused instead of re-solved per request.
func (n *NodriverClient) fetch(url string, headers map[string]string, binary bool, fetchTimeout time.Duration) ([]byte, int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.wake(); err != nil {
		return nil, 0, err
	}

	referer := ""
	if headers != nil {
		referer = headers["Referer"]
	}

	// The attempt budget bounds the loop: un-banned transient rotation can
	// otherwise cycle the pool forever. Each live proxy is worth
	// maxProxyAttempts tries; the budget is fixed once the pool has loaded.
	// Sticky reuse consumes no budget — one loop iteration, one proxy.
	budget, start := -1, 0
	var lastErr error
	for {
		proxy, err := n.nextProxy()
		if err != nil {
			if lastErr != nil {
				return nil, 0, fmt.Errorf("all proxies exhausted: %w (last: %v)", err, lastErr)
			}
			return nil, 0, err
		}
		if budget < 0 {
			budget = n.maxProxyAttempts * max(1, n.pool.live())
			start = budget
		}

		for attempt := 1; attempt <= n.maxProxyAttempts; attempt++ {
			budget--
			body, status, err := n.fetchViaProxy(url, referer, headers, proxy, binary, fetchTimeout)
			if err == nil {
				n.noteSuccess(proxy)
				return body, status, nil
			}
			lastErr = err
			var ferr *fetchError
			if !errors.As(err, &ferr) {
				// Defensive: fetchViaProxy always classifies its errors.
				return nil, 0, err
			}
			if ferr.class == failTarget {
				// Deterministic failure — rotating proxies cannot change the
				// outcome. Surface it without burning the pool.
				return nil, 0, err
			}
			if ferr.class == failProxyDead {
				n.pool.markDead(proxy)
				break // ban, rotate
			}
			// failTransient: retry the same proxy in place — challenge flakes
			// usually clear on a re-attempt — then rotate without a ban.
		}

		// This proxy is spent for the fetch: dead and banned, or transiently
		// exhausted. Release stickiness so the next iteration rotates on.
		n.dropSticky(proxy)

		if budget <= 0 {
			return nil, 0, fmt.Errorf("all proxies exhausted after %d attempts: %v", start, lastErr)
		}
	}
}

// nextProxy returns the proxy for the next rotation-loop iteration: the sticky
// proxy left by the last successful fetch while it is still live, otherwise the
// next live proxy round-robin.
func (n *NodriverClient) nextProxy() (string, error) {
	if n.stickyProxy != "" && n.pool.isLive(n.stickyProxy) {
		return n.stickyProxy, nil
	}
	n.stickyProxy = ""
	n.stickySuccesses = 0
	return n.pool.next()
}

// noteSuccess records a fetch that came back through proxy. The first success
// makes that proxy sticky — later fetches reuse it, so the sidecar's IP-bound
// Cloudflare clearance survives across requests. At maxStickySuccesses the
// proxy is released deliberately, so the next fetch rotates onto the next live
// IP: sticky-forever would let one IP hammer IDX for a whole backfill.
func (n *NodriverClient) noteSuccess(proxy string) {
	if n.stickyProxy != proxy {
		n.stickyProxy = proxy
		n.stickySuccesses = 1
		return
	}
	n.stickySuccesses++
	if n.stickySuccesses >= n.maxStickySuccesses {
		n.stickyProxy = ""
		n.stickySuccesses = 0
	}
}

// dropSticky releases proxy if it is the sticky one, so the next iteration
// rotates instead of picking it again.
func (n *NodriverClient) dropSticky(proxy string) {
	if n.stickyProxy == proxy {
		n.stickyProxy = ""
		n.stickySuccesses = 0
	}
}

// fetchViaProxy runs one /fetch call through a proxy. Every returned error
// carries a fetchFailureClass so the rotation loop can tell a dead proxy from
// a transient Cloudflare flake from a deterministic failure.
func (n *NodriverClient) fetchViaProxy(url, referer string, headers map[string]string, proxy string, binary bool, fetchTimeout time.Duration) ([]byte, int, error) {
	req := nodriverRequest{
		URL:     url,
		Proxy:   proxy,
		Referer: referer,
		Headers: headers,
		Timeout: int(fetchTimeout / time.Millisecond),
		Binary:  binary,
	}
	raw, err := n.post(req)
	if err != nil {
		// Sidecar-level failure (unreachable, auth, HTTP >= 400). The proxy is
		// irrelevant to most of these, so treat them as transient: retry in
		// place, rotate on without a ban, and let the attempt budget end it.
		return nil, 0, &fetchError{failTransient, fmt.Errorf("nodriver request via %s: %w", proxy, err)}
	}

	var resp nodriverResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, 0, &fetchError{failTarget, fmt.Errorf("parse nodriver response: %w", err)}
	}
	if !binary && resp.Status != http.StatusOK {
		return nil, resp.Status, &fetchError{classifySidecarStatus(resp.Status), fmt.Errorf("nodriver fetch failed via %s: %s (status %d)", proxy, resp.Error, resp.Status)}
	}
	if binary && resp.Status >= 400 {
		return nil, resp.Status, &fetchError{classifySidecarStatus(resp.Status), fmt.Errorf("nodriver fetch failed via %s: %s (status %d)", proxy, resp.Error, resp.Status)}
	}
	if resp.Body == "" {
		return nil, resp.Status, &fetchError{failTransient, fmt.Errorf("nodriver fetch returned empty body via %s", proxy)}
	}
	if binary && resp.Encoding == "base64" {
		decoded, err := base64.StdEncoding.DecodeString(resp.Body)
		if err != nil {
			return nil, resp.Status, &fetchError{failTarget, fmt.Errorf("decode nodriver base64 body via %s: %w", proxy, err)}
		}
		return decoded, resp.Status, nil
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
// sidecar). Present to satisfy the browserFetcher interface.
func (n *NodriverClient) Close() {}

// truncateBytes caps a byte slice for error messages.
func truncateBytes(b []byte, n int) string {
	s := string(b)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
