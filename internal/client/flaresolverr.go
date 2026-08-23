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

// browserFetcher is the headless-browser fetch seam shared by the flaresolverr
// and nodriver fetch modes. Implemented by *FlareSolverrClient and
// *NodriverClient; stubbed in tests to verify the IDX client's browser modes
// without a network.
type browserFetcher interface {
	Fetch(url string, headers map[string]string) ([]byte, int, error)
	Close()
}

// FlareSolverrClient wraps the FlareSolverr /v1 HTTP API. It solves Cloudflare
// challenges in a headless browser egressing through a rotating proxy pool, and
// returns the inner JSON payload (FlareSolverr wraps JSON API bodies in an HTML
// <pre> envelope). Single-session: all calls are serialized by a mutex.
type FlareSolverrClient struct {
	baseURL           string
	authToken         string
	timeout           time.Duration
	sessionTTLMinutes int
	wakeTimeout       time.Duration
	pool              *proxyPool
	http              *http.Client
	log               *logrus.Logger

	mu           sync.Mutex
	session      string // current FlareSolverr session ID
	sessionProxy string // proxy bound to the current session
}

// flareRequest is a FlareSolverr /v1 command payload.
type flareRequest struct {
	Cmd               string            `json:"cmd"`
	URL               string            `json:"url,omitempty"`
	Session           string            `json:"session,omitempty"`
	SessionTTLMinutes int               `json:"session_ttl_minutes,omitempty"`
	MaxTimeout        int               `json:"maxTimeout,omitempty"`
	Headers           map[string]string `json:"headers,omitempty"`
	Proxy             *flareProxy       `json:"proxy,omitempty"`
}

type flareProxy struct {
	URL string `json:"url"`
}

// flareResponse is the FlareSolverr /v1 response envelope.
type flareResponse struct {
	Status   string         `json:"status"`
	Message  string         `json:"message"`
	Session  string         `json:"session"`
	Solution *flareSolution `json:"solution"`
}

type flareSolution struct {
	Status   int    `json:"status"`
	Response string `json:"response"`
}

// NewFlareSolverrClient builds a FlareSolverr client. base_url and proxies are
// required; the proxy list itself is loaded lazily on first fetch.
func NewFlareSolverrClient(cfg FlareSolverrConfig, log *logrus.Logger) (*FlareSolverrClient, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("flaresolverr.base_url is required in flaresolverr fetch mode")
	}
	if cfg.Proxies == "" {
		return nil, fmt.Errorf("flaresolverr.proxies is required in flaresolverr fetch mode")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	// The HTTP client must outlive FlareSolverr's internal browser timeout.
	httpClient := &http.Client{Timeout: timeout + 10*time.Second}
	return &FlareSolverrClient{
		baseURL:           strings.TrimRight(cfg.BaseURL, "/"),
		authToken:         cfg.AuthToken,
		timeout:           timeout,
		sessionTTLMinutes: cfg.SessionTTLMinutes,
		wakeTimeout:       cfg.WakeTimeout,
		pool:              newProxyPool(cfg.Proxies, cfg.ProxiesTTL, cfg.DeadRetryAfter, httpClient, log),
		http:              httpClient,
		log:               log,
	}, nil
}

// Fetch retrieves url through FlareSolverr, rotating proxies until one succeeds
// or the pool is exhausted. Returns the extracted JSON bytes and the HTTP status.
func (f *FlareSolverrClient) Fetch(url string, headers map[string]string) ([]byte, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.wake(); err != nil {
		return nil, 0, err
	}

	var lastErr error
	for {
		proxy, err := f.pool.next()
		if err != nil {
			if lastErr != nil {
				return nil, 0, fmt.Errorf("all proxies exhausted: %w (last: %v)", err, lastErr)
			}
			return nil, 0, err
		}

		body, status, err := f.fetchViaProxy(url, headers, proxy)
		if err != nil {
			f.pool.markDead(proxy)
			lastErr = err
			continue
		}
		return body, status, nil
	}
}

// fetchViaProxy runs one request.get through a session bound to proxy. If the
// session died (FlareSolverr restart / TTL), it is recreated once on the same
// proxy before the proxy is blamed.
func (f *FlareSolverrClient) fetchViaProxy(url string, headers map[string]string, proxy string) ([]byte, int, error) {
	if f.session == "" || f.sessionProxy != proxy {
		f.destroySessionLocked()
		if err := f.createSessionLocked(proxy); err != nil {
			return nil, 0, fmt.Errorf("create session via %s: %w", proxy, err)
		}
	}

	body, status, err := f.requestGet(url, headers)
	if err != nil && status != http.StatusForbidden {
		// Session may have died (FlareSolverr restart / TTL). Retry once with a
		// fresh session on the same proxy before blaming the proxy. A 403 is a
		// burned proxy — retrying is pointless.
		f.destroySessionLocked()
		if err2 := f.createSessionLocked(proxy); err2 != nil {
			return nil, 0, fmt.Errorf("recreate session via %s: %w", proxy, err2)
		}
		body, status, err = f.requestGet(url, headers)
		if err != nil {
			return nil, 0, err
		}
	}
	return body, status, err
}

// requestGet POSTs a request.get and classifies the result:
//   - envelope status != ok       -> error (rotate)
//   - solution.status == 403      -> proxy IP burned by target (rotate)
//   - solution.status != 200      -> unexpected status (rotate)
//   - solution.response not JSON  -> challenge page sneaking through (rotate)
//   - otherwise                   -> extracted JSON bytes (success)
func (f *FlareSolverrClient) requestGet(url string, headers map[string]string) ([]byte, int, error) {
	req := flareRequest{
		Cmd:               "request.get",
		URL:               url,
		Session:           f.session,
		SessionTTLMinutes: f.sessionTTLMinutes,
		MaxTimeout:        int(f.timeout / time.Millisecond),
		Headers:           headers,
	}
	raw, err := f.post(req)
	if err != nil {
		return nil, 0, err
	}

	var resp flareResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, 0, fmt.Errorf("parse FlareSolverr response: %w", err)
	}
	if resp.Status != "ok" {
		return nil, 0, fmt.Errorf("FlareSolverr error: %s", resp.Message)
	}
	if resp.Solution == nil {
		return nil, 0, fmt.Errorf("FlareSolverr response missing solution")
	}
	if resp.Solution.Status == http.StatusForbidden {
		return nil, resp.Solution.Status, fmt.Errorf("proxy blocked by target (403)")
	}
	if resp.Solution.Status != http.StatusOK {
		return nil, resp.Solution.Status, fmt.Errorf("target returned status %d", resp.Solution.Status)
	}

	body, err := extractJSON(resp.Solution.Response)
	if err != nil {
		return nil, resp.Solution.Status, err
	}
	return body, resp.Solution.Status, nil
}

// extractJSON pulls the inner JSON out of a FlareSolverr solution.response.
// FlareSolverr wraps JSON API bodies in <html><body><pre>{...}</pre></body></html>;
// the <pre> inner text is the payload. Falls back to first-{/last-} slicing only
// when the response is not an HTML page (a challenge page's inline JS can
// contain brace-balanced JSON and would otherwise be mistaken for a payload).
func extractJSON(response string) ([]byte, error) {
	// Direct: the whole response is already JSON.
	if json.Valid([]byte(response)) {
		return []byte(response), nil
	}
	// <pre> block: FlareSolverr's HTML envelope.
	if i := strings.Index(response, "<pre>"); i >= 0 {
		rest := response[i+len("<pre>"):]
		if j := strings.Index(rest, "</pre>"); j >= 0 {
			inner := rest[:j]
			if json.Valid([]byte(inner)) {
				return []byte(inner), nil
			}
		}
	}
	// Fallback: slice between the first { and the last }, but only for
	// non-HTML responses.
	if !looksLikeHTML(response) {
		if i := strings.Index(response, "{"); i >= 0 {
			if j := strings.LastIndex(response, "}"); j > i {
				inner := response[i : j+1]
				if json.Valid([]byte(inner)) {
					return []byte(inner), nil
				}
			}
		}
	}
	return nil, fmt.Errorf("no JSON found in FlareSolverr response")
}

func looksLikeHTML(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "<html") || strings.Contains(lower, "<!doctype") || strings.Contains(lower, "<script")
}

// createSessionLocked opens a FlareSolverr session bound to proxy. Caller holds mu.
func (f *FlareSolverrClient) createSessionLocked(proxy string) error {
	req := flareRequest{Cmd: "sessions.create", Proxy: &flareProxy{URL: proxy}}
	raw, err := f.post(req)
	if err != nil {
		return err
	}
	var resp flareResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("parse sessions.create response: %w", err)
	}
	if resp.Status != "ok" || resp.Session == "" {
		return fmt.Errorf("sessions.create failed: %s", resp.Message)
	}
	f.session = resp.Session
	f.sessionProxy = proxy
	f.log.Infof("flaresolverr: session %s created via %s", f.session, proxy)
	return nil
}

// destroySessionLocked tears down the current session, if any. Caller holds mu.
func (f *FlareSolverrClient) destroySessionLocked() {
	if f.session == "" {
		return
	}
	req := flareRequest{Cmd: "sessions.destroy", Session: f.session}
	if _, err := f.post(req); err != nil {
		f.log.Warnf("flaresolverr: session destroy failed: %v", err)
	}
	f.session = ""
	f.sessionProxy = ""
}

// wake polls the FlareSolverr health endpoint until it is ready (covers
// SnapDeploy free-tier auto-sleep: the first request triggers a ~60s wake).
func (f *FlareSolverrClient) wake() error {
	deadline := time.Now().Add(f.wakeTimeout)
	for {
		req, err := http.NewRequest(http.MethodGet, f.baseURL, nil)
		if err != nil {
			return err
		}
		if f.authToken != "" {
			req.Header.Set("Authorization", "Bearer "+f.authToken)
		}
		resp, err := f.http.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("FlareSolverr not ready after %v", f.wakeTimeout)
		}
		time.Sleep(2 * time.Second)
	}
}

// post sends a command to the FlareSolverr /v1 endpoint.
func (f *FlareSolverrClient) post(req flareRequest) ([]byte, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequest(http.MethodPost, f.baseURL+"/v1", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if f.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+f.authToken)
	}
	resp, err := f.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("FlareSolverr request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read FlareSolverr response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("FlareSolverr HTTP %d: %s", resp.StatusCode, truncateBytes(body, 200))
	}
	return body, nil
}

// Close destroys the active session.
func (f *FlareSolverrClient) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroySessionLocked()
}

// truncateBytes caps a byte slice for error messages.
func truncateBytes(b []byte, n int) string {
	s := string(b)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
