package client

import (
	"net/http"
	"net/url"
	"sync"
	"time"
)

const cfBMCookie = "__cf_bm"

// CookieManager manages Cloudflare __cf_bm cookie lifecycle.
// Thread-safe; shared across all requests.
type CookieManager struct {
	mu            sync.RWMutex
	cookies       map[string]*http.Cookie
	baseURL       *url.URL
	refreshPeriod time.Duration
	lastRefresh   time.Time
	client        *http.Client
	userAgent     string
}

// NewCookieManager creates a manager for the given base URL.
func NewCookieManager(baseURL string, refreshPeriod time.Duration, userAgent string) (*CookieManager, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	return &CookieManager{
		cookies:       make(map[string]*http.Cookie),
		baseURL:       u,
		refreshPeriod: refreshPeriod,
		client: &http.Client{
			Timeout: 15 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse // don't follow redirects on init
			},
		},
		userAgent: userAgent,
	}, nil
}

// Init performs the initial page hit to obtain the __cf_bm cookie.
func (cm *CookieManager) Init() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.fetchCookie()
}

// EnsureFresh checks if the cookie needs refreshing and does so.
func (cm *CookieManager) EnsureFresh() error {
	cm.mu.RLock()
	needsRefresh := time.Since(cm.lastRefresh) > cm.refreshPeriod
	cm.mu.RUnlock()

	if !needsRefresh {
		return nil
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Double-check after acquiring write lock.
	if time.Since(cm.lastRefresh) <= cm.refreshPeriod {
		return nil
	}

	return cm.fetchCookie()
}

// fetchCookie hits the base URL to get a fresh __cf_bm cookie.
// Must be called with cm.mu write-locked.
func (cm *CookieManager) fetchCookie() error {
	req, err := http.NewRequest(http.MethodGet, cm.baseURL.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", cm.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")

	resp, err := cm.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	for _, c := range resp.Cookies() {
		if c.Name == cfBMCookie {
			cm.cookies[cfBMCookie] = c
			cm.lastRefresh = time.Now()
			return nil
		}
	}

	// No __cf_bm cookie set — site may not require it. Not an error.
	cm.lastRefresh = time.Now()
	return nil
}

// ApplyCookies sets stored cookies on an outgoing request.
func (cm *CookieManager) ApplyCookies(req *http.Request) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, c := range cm.cookies {
		req.AddCookie(c)
	}
}
