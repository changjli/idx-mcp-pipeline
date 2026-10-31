package indexalpha

import (
	"strings"

	"github.com/spf13/viper"
)

// ConfigFromViper builds an adapter config from the `indexalpha.*` config keys.
// Keys is the full pool; APIKey carries the first key for the legacy single-key
// Client path.
func ConfigFromViper(vip *viper.Viper) Config {
	cfg := DefaultConfig()
	if v := strings.TrimSpace(vip.GetString("indexalpha.base_url")); v != "" {
		cfg.BaseURL = v
	}
	if d := vip.GetDuration("indexalpha.timeout"); d > 0 {
		cfg.Timeout = d
	}
	if vip.IsSet("indexalpha.reserve") {
		cfg.Reserve = vip.GetInt("indexalpha.reserve")
	}
	if cfg.Reserve < 0 {
		cfg.Reserve = 0
	}
	if d := vip.GetDuration("indexalpha.cooldown"); d > 0 {
		cfg.Cooldown = d
	}
	if keys := KeysFromViper(vip); len(keys) > 0 {
		cfg.Keys = keys
		cfg.APIKey = keys[0]
	}
	return cfg
}

// KeysFromViper returns the configured API keys in order, trimmed and
// deduplicated. INDEXALPHA_KEYS is a comma-separated list; a single key is a
// one-element list. INDEXALPHA_KEY is accepted as a singular fallback so a
// one-key deployment does not have to know about the plural name.
func KeysFromViper(vip *viper.Viper) []string {
	raw := vip.GetString("indexalpha.keys")
	if strings.TrimSpace(raw) == "" {
		raw = vip.GetString("indexalpha.key")
	}
	seen := make(map[string]bool)
	var keys []string
	for _, part := range strings.Split(raw, ",") {
		key := strings.TrimSpace(part)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
	}
	return keys
}
