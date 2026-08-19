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
