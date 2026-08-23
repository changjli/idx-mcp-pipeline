package client

import (
	"time"

	"github.com/spf13/viper"
)

// Config holds all tunable knobs for the IDX HTTP client.
type Config struct {
	BaseURL         string
	Timeout         time.Duration
	RateLimitPerSec float64
	MaxRetries      int
	BaseDelay       time.Duration
	MaxDelay        time.Duration
	CacheTTL        time.Duration
	CookieRefresh   time.Duration
	UserAgent       string
	// FetchMode selects the transport: "direct" (default), "flaresolverr", or "nodriver".
	FetchMode    string
	FlareSolverr FlareSolverrConfig
	Nodriver     NodriverConfig
}

// FlareSolverrConfig holds the FlareSolverr fetch-mode knobs.
type FlareSolverrConfig struct {
	BaseURL           string        // root URL; the client appends /v1
	AuthToken         string        // sent as Authorization: Bearer to the reverse proxy
	Timeout           time.Duration // per-proxy request.get maxTimeout
	Proxies           string        // HTTPS URL or file path to a JSON array of proxy URLs
	ProxiesTTL        time.Duration // in-memory cache TTL for the proxy list
	DeadRetryAfter    time.Duration // how long a dead proxy stays out of rotation
	WakeTimeout       time.Duration // bound for the wake-on-demand health poll
	SessionTTLMinutes int           // FlareSolverr session_ttl_minutes per request
}

// NodriverConfig holds the nodriver sidecar fetch-mode knobs. The proxy pool is
// shared with FlareSolverrConfig (same flaresolverr.proxies source) so one proxy
// list serves both browser modes.
type NodriverConfig struct {
	BaseURL     string        // root URL of the nodriver sidecar (client appends /fetch)
	AuthToken   string        // sent as Authorization: Bearer to the sidecar's reverse proxy
	Timeout     time.Duration // per-fetch budget forwarded to the sidecar
	WakeTimeout time.Duration // bound for the wake-on-demand health poll
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		BaseURL:         "https://www.idx.co.id",
		Timeout:         30 * time.Second,
		RateLimitPerSec: 1.0,
		MaxRetries:      3,
		BaseDelay:       1 * time.Second,
		MaxDelay:        30 * time.Second,
		CacheTTL:        6 * time.Hour,
		CookieRefresh:   10 * time.Minute,
		UserAgent:       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		FetchMode:       "direct",
		FlareSolverr: FlareSolverrConfig{
			Timeout:           15 * time.Second,
			ProxiesTTL:        time.Hour,
			DeadRetryAfter:    6 * time.Hour,
			WakeTimeout:       3 * time.Minute,
			SessionTTLMinutes: 30,
		},
		Nodriver: NodriverConfig{
			Timeout:     20 * time.Second,
			WakeTimeout: 60 * time.Second,
		},
	}
}

// ConfigFromViper reads config from viper (env vars / config.json).
func ConfigFromViper(vip *viper.Viper) Config {
	cfg := DefaultConfig()

	if v := vip.GetString("idx.base_url"); v != "" {
		cfg.BaseURL = v
	}
	if v := vip.GetDuration("idx.timeout"); v > 0 {
		cfg.Timeout = v
	}
	if v := vip.GetFloat64("idx.rate_limit_per_sec"); v > 0 {
		cfg.RateLimitPerSec = v
	}
	if v := vip.GetInt("idx.max_retries"); v > 0 {
		cfg.MaxRetries = v
	}
	if v := vip.GetDuration("idx.base_delay"); v > 0 {
		cfg.BaseDelay = v
	}
	if v := vip.GetDuration("idx.max_delay"); v > 0 {
		cfg.MaxDelay = v
	}
	if v := vip.GetDuration("idx.cache_ttl"); v > 0 {
		cfg.CacheTTL = v
	}
	if v := vip.GetDuration("idx.cookie_refresh"); v > 0 {
		cfg.CookieRefresh = v
	}
	if v := vip.GetString("idx.user_agent"); v != "" {
		cfg.UserAgent = v
	}
	if v := vip.GetString("idx.fetch_mode"); v != "" {
		cfg.FetchMode = v
	}

	fs := &cfg.FlareSolverr
	if v := vip.GetString("flaresolverr.base_url"); v != "" {
		fs.BaseURL = v
	}
	if v := vip.GetString("flaresolverr.auth_token"); v != "" {
		fs.AuthToken = v
	}
	if v := vip.GetDuration("flaresolverr.timeout"); v > 0 {
		fs.Timeout = v
	}
	if v := vip.GetString("flaresolverr.proxies"); v != "" {
		fs.Proxies = v
	}
	if v := vip.GetDuration("flaresolverr.proxies_ttl"); v > 0 {
		fs.ProxiesTTL = v
	}
	if v := vip.GetDuration("flaresolverr.dead_retry_after"); v > 0 {
		fs.DeadRetryAfter = v
	}
	if v := vip.GetDuration("flaresolverr.wake_timeout"); v > 0 {
		fs.WakeTimeout = v
	}
	if v := vip.GetInt("flaresolverr.session_ttl_minutes"); v > 0 {
		fs.SessionTTLMinutes = v
	}

	// nodriver sidecar fetch mode. Proxy pool reuses the flaresolverr.proxies
	// source (and TTL / dead_retry_after) so one proxy list serves both modes.
	nd := &cfg.Nodriver
	if v := vip.GetString("nodriver.base_url"); v != "" {
		nd.BaseURL = v
	}
	if v := vip.GetString("nodriver.auth_token"); v != "" {
		nd.AuthToken = v
	}
	if v := vip.GetDuration("nodriver.timeout"); v > 0 {
		nd.Timeout = v
	}
	if v := vip.GetDuration("nodriver.wake_timeout"); v > 0 {
		nd.WakeTimeout = v
	}

	return cfg
}
