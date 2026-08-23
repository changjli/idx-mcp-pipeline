package client

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Client is the shared IDX HTTP client with Cloudflare session management,
// rate limiting, retry with backoff, TLS fallback, and stale-while-error cache.
type Client struct {
	config  Config
	http    *http.Client
	limiter *RateLimiter
	cache   *StaleCache
	cookies *CookieManager
	browser browserFetcher // non-nil in flaresolverr/nodriver fetch modes
	log     *logrus.Logger
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// Global singleton state.
var (
	globalClient   *Client
	globalMu       sync.Mutex
	globalErr      error
	globalInitDone chan struct{} // closed after first init attempt
)

func init() {
	globalInitDone = make(chan struct{})
}

// InitDefaultClient initializes the shared singleton from viper config.
// Safe to call multiple times — only the first call initializes.
func InitDefaultClient(vip *viper.Viper, log *logrus.Logger) (*Client, error) {
	globalMu.Lock()
	defer globalMu.Unlock()

	select {
	case <-globalInitDone:
		return globalClient, globalErr
	default:
	}

	globalClient, globalErr = NewDefaultClient(vip, log)
	close(globalInitDone)
	return globalClient, globalErr
}

// DefaultClient returns the shared singleton. Panics if not yet initialized.
func DefaultClient() *Client {
	<-globalInitDone
	if globalClient == nil {
		panic("IDX HTTP client not initialized — call InitDefaultClient first")
	}
	return globalClient
}

// NewClient creates a fully wired IDX HTTP client.
// Call Close() to stop background cookie refresh.
func NewClient(cfg Config, log *logrus.Logger) (*Client, error) {
	cookieMgr, err := NewCookieManager(cfg.BaseURL, cfg.CookieRefresh, cfg.UserAgent)
	if err != nil {
		return nil, fmt.Errorf("cookie manager: %w", err)
	}

	c := &Client{
		config:  cfg,
		limiter: NewRateLimiter(cfg.RateLimitPerSec),
		cache:   NewStaleCache(cfg.CacheTTL),
		cookies: cookieMgr,
		log:     log,
		stopCh:  make(chan struct{}),
		http: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: newFallbackTransport(cfg.Timeout),
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
	}

	// Browser fetch modes: route GetWithHeaders through a headless browser when
	// idx.fetch_mode=flaresolverr|nodriver.
	switch cfg.FetchMode {
	case "flaresolverr":
		flare, err := NewFlareSolverrClient(cfg.FlareSolverr, log)
		if err != nil {
			return nil, fmt.Errorf("flaresolverr client: %w", err)
		}
		c.browser = flare
	case "nodriver":
		nd, err := NewNodriverClient(cfg.Nodriver, cfg.FlareSolverr.Proxies, cfg.FlareSolverr.ProxiesTTL, cfg.FlareSolverr.DeadRetryAfter, log)
		if err != nil {
			return nil, fmt.Errorf("nodriver client: %w", err)
		}
		c.browser = nd
	}

	// Initial Cloudflare cookie fetch.
	if err := c.cookies.Init(); err != nil {
		log.Warnf("cloudflare cookie init failed (non-fatal): %v", err)
	}

	// Background cookie refresh.
	c.wg.Add(1)
	go c.cookieRefreshLoop()

	return c, nil
}

// NewDefaultClient creates a client from viper config.
func NewDefaultClient(vip *viper.Viper, log *logrus.Logger) (*Client, error) {
	cfg := ConfigFromViper(vip)
	return NewClient(cfg, log)
}

// Close stops background goroutines and tears down the browser fetcher.
func (c *Client) Close() {
	if c.browser != nil {
		c.browser.Close()
	}
	close(c.stopCh)
	c.wg.Wait()
}

// Get performs a rate-limited, retried GET with Cloudflare cookie, TLS fallback,
// and stale-while-error cache.
func (c *Client) Get(path string) (*http.Response, error) {
	return c.GetWithHeaders(path, nil)
}

// GetStream performs a session-aware GET (rate-limited, Cloudflare-cookie'd,
// retried, TLS-fallback) WITHOUT caching or buffering the body. The caller owns
// the response and must close Body. Unlike Get/GetWithHeaders, the body is
// never read into memory nor stored in the stale cache — disclosure PDFs run
// up to 10MB and must not be held there. resolveURL passes absolute URLs
// through untouched, so StaticData PDF links work without a double base-URL.
func (c *Client) GetStream(path string, extraHeaders map[string]string) (*http.Response, error) {
	url := c.resolveURL(path)

	// Rate limit.
	c.limiter.Wait()

	// Ensure Cloudflare cookie is fresh.
	if err := c.cookies.EnsureFresh(); err != nil {
		c.log.Warnf("cookie refresh failed: %v", err)
	}

	return c.doWithRetry(url, extraHeaders)
}

// GetWithHeaders is like Get but allows setting additional request headers.
// The headers map is merged on top of the default headers (User-Agent, Accept, etc.).
func (c *Client) GetWithHeaders(path string, extraHeaders map[string]string) (*http.Response, error) {
	// Browser mode (flaresolverr/nodriver): delegate to the headless-browser fetch path.
	if c.browser != nil {
		return c.getViaBrowser(path, extraHeaders)
	}

	url := c.resolveURL(path)

	// Check cache first.
	if body, status, headers, fresh := c.cache.Get(url); fresh {
		c.log.Debugf("cache HIT (fresh): %s", url)
		return c.syntheticResponse(status, headers, body, url), nil
	}

	// Rate limit.
	c.limiter.Wait()

	// Ensure Cloudflare cookie is fresh.
	if err := c.cookies.EnsureFresh(); err != nil {
		c.log.Warnf("cookie refresh failed: %v", err)
	}

	// Attempt request with retry.
	resp, err := c.doWithRetry(url, extraHeaders)
	if err != nil {
		// Stale-while-error: serve stale cache if available.
		if body, status, headers, _ := c.cache.Get(url); body != nil {
			c.log.Warnf("upstream failed, serving stale cache: %s", url)
			return c.syntheticResponse(status, headers, body, url), nil
		}
		return nil, fmt.Errorf("request failed and no stale cache: %w", err)
	}

	// Read body before caching.
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	// Cache successful responses (non-4xx).
	if resp.StatusCode < 400 {
		c.cache.Set(url, resp.StatusCode, resp.Header, body)
	}

	resp.Body = ReplayBody(body)
	return resp, nil
}

// getViaBrowser fetches through the configured browser fetcher (FlareSolverr or
// nodriver) and surfaces the extracted payload as a synthetic response, reusing
// the caller-side contract (status >= 400 -> error, json.Unmarshal in the task)
// unchanged.
func (c *Client) getViaBrowser(path string, extraHeaders map[string]string) (*http.Response, error) {
	url := c.resolveURL(path)
	body, status, err := c.browser.Fetch(url, extraHeaders)
	if err != nil {
		return nil, fmt.Errorf("browser fetch: %w", err)
	}
	return c.syntheticResponse(status, make(http.Header), body, url), nil
}

// syntheticResponse builds an http.Response from cached data.
func (c *Client) syntheticResponse(status int, headers http.Header, body []byte, rawURL string) *http.Response {
	u, _ := url.Parse(rawURL)
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       ReplayBody(body),
		Request:    &http.Request{URL: u},
	}
}

// doWithRetry executes the request with retry on transient failures.
// extraHeaders are merged on top of default headers.
func (c *Client) doWithRetry(url string, extraHeaders map[string]string) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := backoffDelay(attempt-1, c.config.BaseDelay, c.config.MaxDelay)
			c.log.Debugf("retry attempt %d/%d after %v", attempt, c.config.MaxRetries, delay)
			select {
			case <-time.After(delay):
			case <-c.stopCh:
				return nil, fmt.Errorf("client closed during retry backoff")
			}
		}

		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", c.config.UserAgent)
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
		req.Header.Set("Accept-Language", "en-US,en;q=0.5")

		// Apply extra headers (e.g. Referer).
		for k, v := range extraHeaders {
			req.Header.Set(k, v)
		}

		// Attach Cloudflare cookies.
		c.cookies.ApplyCookies(req)

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue // retry on network errors
		}

		// Retry on server errors (5xx).
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
			resp.Body.Close()
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("all retries exhausted: %w", lastErr)
}

// resolveURL joins base URL with the given path.
func (c *Client) resolveURL(path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	return strings.TrimRight(c.config.BaseURL, "/") + "/" + strings.TrimLeft(path, "/")
}

// cookieRefreshLoop periodically refreshes the Cloudflare cookie.
func (c *Client) cookieRefreshLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.CookieRefresh)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.cookies.EnsureFresh(); err != nil {
				c.log.Warnf("background cookie refresh: %v", err)
			}
		case <-c.stopCh:
			return
		}
	}
}
