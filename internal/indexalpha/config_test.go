package indexalpha

import (
	"reflect"
	"testing"

	"github.com/spf13/viper"
)

func TestKeysFromViper(t *testing.T) {
	vip := viper.New()
	vip.Set("indexalpha.keys", " key_one , key_two,key_one,,")

	got := KeysFromViper(vip)
	want := []string{"key_one", "key_two"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
}

func TestKeysFromViper_SingularFallback(t *testing.T) {
	vip := viper.New()
	vip.Set("indexalpha.key", "only_key")

	got := KeysFromViper(vip)
	if !reflect.DeepEqual(got, []string{"only_key"}) {
		t.Fatalf("keys = %v, want [only_key]", got)
	}
}

func TestConfigFromViper(t *testing.T) {
	vip := viper.New()
	vip.Set("indexalpha.keys", "first,second")
	vip.Set("indexalpha.base_url", "https://example.test/")
	vip.Set("indexalpha.timeout", "12s")
	vip.Set("indexalpha.reserve", 4)
	vip.Set("indexalpha.cooldown", "45s")

	cfg := ConfigFromViper(vip)
	if cfg.APIKey != "first" {
		t.Errorf("APIKey = %q, want first", cfg.APIKey)
	}
	if !reflect.DeepEqual(cfg.Keys, []string{"first", "second"}) {
		t.Errorf("Keys = %v", cfg.Keys)
	}
	if cfg.BaseURL != "https://example.test/" {
		t.Errorf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.Timeout.Seconds() != 12 {
		t.Errorf("Timeout = %s, want 12s", cfg.Timeout)
	}
	if cfg.Reserve != 4 {
		t.Errorf("Reserve = %d, want 4", cfg.Reserve)
	}
	if cfg.Cooldown.Seconds() != 45 {
		t.Errorf("Cooldown = %s, want 45s", cfg.Cooldown)
	}
}

// TestConfigFromViper_Defaults pins the operating defaults: a ten-unit reserve
// and a one-minute cooldown. No monthly tier is hardcoded anywhere.
func TestConfigFromViper_Defaults(t *testing.T) {
	cfg := ConfigFromViper(viper.New())
	if cfg.Reserve != DefaultReserve {
		t.Errorf("Reserve = %d, want %d", cfg.Reserve, DefaultReserve)
	}
	if cfg.Cooldown != DefaultCooldown {
		t.Errorf("Cooldown = %s, want %s", cfg.Cooldown, DefaultCooldown)
	}
	if cfg.BaseURL != DefaultBaseURL {
		t.Errorf("BaseURL = %q", cfg.BaseURL)
	}
}
