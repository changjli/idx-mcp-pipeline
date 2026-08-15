package ipot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// DefaultMinDelay is the polite spacing between IPOT calls (≥2s per ticket).
const DefaultMinDelay = 2 * time.Second

// DefaultCacheTTL is how long a fetched ticker+date result stays fresh.
const DefaultCacheTTL = time.Hour

// DefaultMaxCacheEntries bounds the in-memory cache so a long-running server
// iterating many tickers/dates doesn't grow without bound.
const DefaultMaxCacheEntries = 1000

// DefaultBaseURL is the IPOT broker summary AJAX endpoint host.
const DefaultBaseURL = "https://www.indopremier.com"

// ErrUpstream429 is returned when IPOT rate-limits a request. The controller
// maps it to the MCP UPSTREAM_429 error code so clients can back off.
var ErrUpstream429 = errors.New("ipot: upstream rate limited")

// Config holds the tunable knobs for the IPOT client.
type Config struct {
	BaseURL         string
	Timeout         time.Duration
	MinDelay        time.Duration // minimum spacing between upstream calls
	CacheTTL        time.Duration // freshness window per ticker+date
	MaxCacheEntries int           // cap on cached ticker+date results
}

// DefaultConfig returns production defaults: 2s pacing, 1h cache.
func DefaultConfig() Config {
	return Config{
		BaseURL:         DefaultBaseURL,
		Timeout:         30 * time.Second,
		MinDelay:        DefaultMinDelay,
		CacheTTL:        DefaultCacheTTL,
		MaxCacheEntries: DefaultMaxCacheEntries,
	}
}

// cacheEntry holds a parsed result with its creation time.
type cacheEntry struct {
	result  *Result
	created time.Time
}

// Client fetches per-ticker broker summaries from the IPOT AJAX endpoint.
// It enforces a minimum delay between upstream calls and caches each
// ticker+date result so repeated lookups don't hammer the source.
type Client struct {
	baseURL  string
	http     *http.Client
	minDelay time.Duration
	cacheTTL time.Duration
	maxCache int
	log      *logrus.Logger

	// paceMu serializes upstream calls so MinDelay is enforced even under
	// concurrency; cacheMu guards the cache. Separate mutexes so a pacing wait
	// never blocks another request's cache read/write.
	paceMu   sync.Mutex
	lastCall time.Time
	cacheMu  sync.Mutex
	cache    map[string]cacheEntry
}

// NewClient creates an IPOT client with the given config.
func NewClient(cfg Config, log *logrus.Logger) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MinDelay <= 0 {
		cfg.MinDelay = DefaultMinDelay
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = DefaultCacheTTL
	}
	if cfg.MaxCacheEntries <= 0 {
		cfg.MaxCacheEntries = DefaultMaxCacheEntries
	}
	return &Client{
		baseURL:  cfg.BaseURL,
		minDelay: cfg.MinDelay,
		cacheTTL: cfg.CacheTTL,
		maxCache: cfg.MaxCacheEntries,
		log:      log,
		http:     &http.Client{Timeout: cfg.Timeout},
		cache:    make(map[string]cacheEntry),
	}
}

// Fetch returns the broker summary for one ticker on one trading day.
// Results are cached per ticker+date for CacheTTL; a cache hit skips the
// upstream call entirely. A non-trading day returns an empty Result (no rows,
// zero totals) with no error. Empty results are NOT cached — IPOT may publish
// the table later the same day, and caching the empty table would blind the
// on-demand tool to just-published data for the full TTL.
func (c *Client) Fetch(ctx context.Context, ticker string, date time.Time) (*Result, error) {
	key := cacheKey(ticker, date)

	if res, ok := c.cacheGet(key); ok {
		c.log.Debugf("ipot: cache HIT %s", key)
		return cloneResult(res), nil
	}

	c.waitPacing()

	u := c.buildURL(ticker, date)
	c.log.Debugf("ipot: fetching %s", u)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("ipot: build request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ipot: get %s: %w", u, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ipot: read body: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("%w: status=%d", ErrUpstream429, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ipot: upstream error: status=%d body=%s", resp.StatusCode, truncate(string(body), 200))
	}

	res, err := ParseBrokerSummary(body)
	if err != nil {
		return nil, fmt.Errorf("ipot: parse %s: %w", u, err)
	}

	if len(res.Buyers) > 0 || len(res.Sellers) > 0 {
		c.cacheSet(key, res)
	}
	return res, nil
}

// buildURL constructs the data-brokersummary.php URL with the locked params:
// board=RG (matches Stockbit's regular-board view) and fd=all (foreign+domestic
// in one call). Dates use MM/DD/YYYY — DD/MM/YYYY silently returns empty.
// The URL is built as a raw string (not url.Values) to keep the exact param
// order and unencoded slashes the endpoint expects.
func (c *Client) buildURL(ticker string, date time.Time) string {
	d := date.Format("01/02/2006")
	return fmt.Sprintf("%s/module/saham/include/data-brokersummary.php?code=%s&start=%s&end=%s&fd=all&board=RG",
		c.baseURL, ticker, d, d)
}

// waitPacing blocks until MinDelay has elapsed since the last upstream call.
// paceMu is held across the sleep so concurrent Fetch calls serialize and the
// pacing is actually enforced (releasing it during the sleep let N goroutines
// all pass the wait check and fire upstream nearly simultaneously). Cache
// reads/writes use a separate mutex and never block on a pacing wait.
func (c *Client) waitPacing() {
	c.paceMu.Lock()
	defer c.paceMu.Unlock()

	now := time.Now()
	wait := c.minDelay - now.Sub(c.lastCall)
	if wait > 0 {
		time.Sleep(wait)
	}
	c.lastCall = time.Now()
}

func (c *Client) cacheGet(key string) (*Result, bool) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	entry, ok := c.cache[key]
	if !ok {
		return nil, false
	}
	if time.Since(entry.created) >= c.cacheTTL {
		delete(c.cache, key)
		return nil, false
	}
	return entry.result, true
}

func (c *Client) cacheSet(key string, res *Result) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	if len(c.cache) >= c.maxCache {
		// Evict the oldest entry to bound memory.
		var oldestKey string
		var oldest time.Time
		for k, e := range c.cache {
			if oldestKey == "" || e.created.Before(oldest) {
				oldestKey, oldest = k, e.created
			}
		}
		delete(c.cache, oldestKey)
	}
	c.cache[key] = cacheEntry{result: res, created: time.Now()}
}

// cacheKey identifies a ticker+date in the cache.
func cacheKey(ticker string, date time.Time) string {
	return ticker + ":" + date.Format("2006-01-02")
}

// cloneResult returns a deep copy of a cached Result so callers can't mutate
// the shared cache entry.
func cloneResult(res *Result) *Result {
	if res == nil {
		return nil
	}
	cp := *res
	cp.Buyers = append([]Row(nil), res.Buyers...)
	cp.Sellers = append([]Row(nil), res.Sellers...)
	return &cp
}

// truncate truncates a string to maxLen chars, appending "..." if truncated.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
