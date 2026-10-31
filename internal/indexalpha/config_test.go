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

	cfg := ConfigFromViper(vip)
	if cfg.APIKey != "first" {
		t.Errorf("APIKey = %q, want first", cfg.APIKey)
	}
	if cfg.BaseURL != "https://example.test/" {
		t.Errorf("BaseURL = %q", cfg.BaseURL)
	}
	if cfg.Timeout.Seconds() != 12 {
		t.Errorf("Timeout = %s, want 12s", cfg.Timeout)
	}
}
