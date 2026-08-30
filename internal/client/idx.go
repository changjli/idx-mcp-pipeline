package client

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Client is the shared IDX HTTP client: a thin router over the nodriver
// browser transport. Every fetch — JSON sources and disclosure PDFs — routes
// through the sidecar's headless Chrome + rotating proxy pool, the only
// transport that clears the Cloudflare JS-execution gate (ADR-0007).
type Client struct {
	config  Config
	fetcher Fetcher        // active adapter (browserFetcherAdapter)
	browser browserFetcher // the nodriver sidecar client
	log     *logrus.Logger
}

// NewClient creates a fully wired IDX HTTP client.
// Call Close() to tear down the sidecar session.
func NewClient(cfg Config, log *logrus.Logger) (*Client, error) {
	if cfg.Nodriver.Proxies == "" {
		return nil, fmt.Errorf("nodriver.proxies is required (proxy pool source)")
	}
	pool := newProxyPool(cfg.Nodriver.Proxies, cfg.Nodriver.ProxiesTTL, cfg.Nodriver.DeadRetryAfter, proxyPoolHTTPClient(), log)
	nd, err := NewNodriverClient(cfg.Nodriver, pool, log)
	if err != nil {
		return nil, fmt.Errorf("nodriver client: %w", err)
	}
	return &Client{
		config:  cfg,
		fetcher: &browserFetcherAdapter{browser: nd},
		browser: nd,
		log:     log,
	}, nil
}

// NewDefaultClient creates a client from viper config.
func NewDefaultClient(vip *viper.Viper, log *logrus.Logger) (*Client, error) {
	cfg := ConfigFromViper(vip)
	return NewClient(cfg, log)
}

// Close tears down the browser session. (The sidecar owns the Chrome lifecycle;
// NodriverClient.Close is a no-op.)
func (c *Client) Close() {
	c.browser.Close()
}

// Get performs a browser-fetched GET.
func (c *Client) Get(path string) (*http.Response, error) {
	return c.GetWithHeaders(path, nil)
}

// GetStream performs a session-aware GET for disclosure PDFs. It routes through
// the browser fetcher's binary transport (the sidecar's base64 transport):
// direct GETs on the StaticData host now 403 on the Cloudflare JS-execution
// gate (issue 01), while the browser session carries clearance + Referer. The
// caller owns the response and must close Body. resolveURL passes absolute URLs
// through untouched, so StaticData PDF links work without a double base-URL.
func (c *Client) GetStream(path string, extraHeaders map[string]string) (*http.Response, error) {
	url := c.resolveURL(path)
	c.log.Debugf("GetStream via browser: %s", url)

	hdrs := make(map[string]string, len(extraHeaders)+1)
	for k, v := range extraHeaders {
		hdrs[k] = v
	}
	// Default Referer to the IDX base URL when the caller sends none: the
	// sidecar loads it first to clear Cloudflare on the domain, then in-page
	// fetches the PDF carrying clearance cookies + a legit referrer (the
	// hotlink gate, when present, is satisfied the same way a real browser
	// would).
	if hdrs["Referer"] == "" {
		hdrs["Referer"] = c.config.BaseURL
	}

	return c.fetcher.GetStream(url, hdrs)
}

// GetWithHeaders is like Get but allows setting additional request headers.
// The headers map is merged on top of the browser fetch.
func (c *Client) GetWithHeaders(path string, extraHeaders map[string]string) (*http.Response, error) {
	return c.fetcher.GetWithHeaders(c.resolveURL(path), extraHeaders)
}

// resolveURL joins base URL with the given path.
func (c *Client) resolveURL(path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	return strings.TrimRight(c.config.BaseURL, "/") + "/" + strings.TrimLeft(path, "/")
}
