package client

import (
	"time"

	"github.com/spf13/viper"
)

// Config holds the knobs for the IDX client. Every fetch routes through the
// nodriver sidecar (headless Chrome + rotating proxy), the only transport that
// clears the Cloudflare JS-execution gate (ADR-0007).
type Config struct {
	BaseURL  string
	Nodriver NodriverConfig
}

// NodriverConfig holds the nodriver sidecar fetch knobs. The proxy pool
// (Proxies/ProxiesTTL/DeadRetryAfter) was formerly shared with FlareSolverr;
// it is now owned by nodriver alone.
type NodriverConfig struct {
	BaseURL        string        // root URL of the nodriver sidecar (client appends /fetch)
	AuthToken      string        // sent as Authorization: Bearer to the sidecar's reverse proxy
	Timeout        time.Duration // per-fetch budget for JSON pages
	WakeTimeout    time.Duration // bound for the wake-on-demand health poll
	Proxies        string        // HTTPS URL or file path to a JSON array of proxy URLs
	ProxiesTTL     time.Duration // in-memory cache TTL for the proxy list
	DeadRetryAfter time.Duration // how long a dead proxy stays out of rotation
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		BaseURL: "https://www.idx.co.id",
		Nodriver: NodriverConfig{
			Timeout:        20 * time.Second,
			WakeTimeout:    60 * time.Second,
			ProxiesTTL:     time.Hour,
			DeadRetryAfter: 6 * time.Hour,
		},
	}
}

// ConfigFromViper reads config from viper (env vars / config.json).
func ConfigFromViper(vip *viper.Viper) Config {
	cfg := DefaultConfig()

	if v := vip.GetString("idx.base_url"); v != "" {
		cfg.BaseURL = v
	}

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
	if v := vip.GetString("nodriver.proxies"); v != "" {
		nd.Proxies = v
	}
	if v := vip.GetDuration("nodriver.proxies_ttl"); v > 0 {
		nd.ProxiesTTL = v
	}
	if v := vip.GetDuration("nodriver.dead_retry_after"); v > 0 {
		nd.DeadRetryAfter = v
	}

	return cfg
}
