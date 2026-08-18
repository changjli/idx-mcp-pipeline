package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestNewRedisConnOpt(t *testing.T) {
	t.Run("defaults to localhost when address empty", func(t *testing.T) {
		vip := viper.New()
		opt := NewRedisConnOpt(vip)
		if opt.Addr != "localhost:6379" {
			t.Fatalf("Addr = %q, want localhost:6379", opt.Addr)
		}
		if opt.TLSConfig != nil {
			t.Fatalf("TLSConfig = %v, want nil by default", opt.TLSConfig)
		}
	})

	t.Run("carries address, password, db", func(t *testing.T) {
		vip := viper.New()
		vip.Set("redis.address", "redis.example.com:6379")
		vip.Set("redis.password", "sekrit")
		vip.Set("redis.db", 3)

		opt := NewRedisConnOpt(vip)
		if opt.Addr != "redis.example.com:6379" {
			t.Fatalf("Addr = %q", opt.Addr)
		}
		if opt.Password != "sekrit" {
			t.Fatalf("Password = %q", opt.Password)
		}
		if opt.DB != 3 {
			t.Fatalf("DB = %d", opt.DB)
		}
		if opt.TLSConfig != nil {
			t.Fatalf("TLSConfig = %v, want nil when tls off", opt.TLSConfig)
		}
	})

	t.Run("enables TLS when redis.tls set", func(t *testing.T) {
		vip := viper.New()
		vip.Set("redis.address", "redis.example.com:6379")
		vip.Set("redis.tls", true)

		opt := NewRedisConnOpt(vip)
		if opt.TLSConfig == nil {
			t.Fatal("TLSConfig = nil, want non-nil when redis.tls=true")
		}
		if opt.TLSConfig.ServerName != "redis.example.com" {
			t.Fatalf("TLSConfig.ServerName = %q, want redis.example.com", opt.TLSConfig.ServerName)
		}
	})

	t.Run("TLS ServerName falls back to addr when no port", func(t *testing.T) {
		vip := viper.New()
		vip.Set("redis.address", "redis.example.com")
		vip.Set("redis.tls", true)

		opt := NewRedisConnOpt(vip)
		if opt.TLSConfig == nil || opt.TLSConfig.ServerName != "redis.example.com" {
			t.Fatalf("TLSConfig.ServerName = %v, want redis.example.com", opt.TLSConfig)
		}
	})
}
