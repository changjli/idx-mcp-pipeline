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

	return cfg
}
