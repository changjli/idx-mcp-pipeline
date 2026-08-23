package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

func NewViper() *viper.Viper {
	config := viper.New()

	config.SetConfigName("config")
	config.SetConfigType("json")
	config.AddConfigPath("./")
	config.AddConfigPath("./../")

	// Map env vars to nested config keys: DATABASE_HOST -> database.host.
	config.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	config.AutomaticEnv()

	// Explicit bindings where the documented env var name is not the literal
	// dot->underscore uppercased key. flaresolverr.auth_token / nodriver.auth_token
	// would otherwise map to FLARESOLVERR_AUTH_TOKEN / NODRIVER_AUTH_TOKEN, but the
	// deploy config + .env.example use FLARESOLVERR_TOKEN / NODRIVER_TOKEN (and the
	// reverse proxy reads the same var). Bind them so the documented names work.
	_ = config.BindEnv("flaresolverr.auth_token", "FLARESOLVERR_TOKEN")
	_ = config.BindEnv("nodriver.auth_token", "NODRIVER_TOKEN")

	// Config file is optional: when absent (e.g. Heroku, which relies on env
	// vars via AutomaticEnv), fall back to defaults + env instead of panicking.
	// Any other read error (malformed file, permission denied) is still fatal.
	err := config.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			panic(fmt.Errorf("fatal error config file: %w", err))
		}
	}

	return config
}
