package config

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func NewDatabase(viper *viper.Viper, log *logrus.Logger) *sqlx.DB {
	host := viper.GetString("database.host")
	port := viper.GetInt("database.port")
	user := viper.GetString("database.user")
	password := viper.GetString("database.password")
	dbname := viper.GetString("database.name")
	sslmode := viper.GetString("database.sslmode")
	idle := viper.GetInt("database.pool.idle")
	max := viper.GetInt("database.pool.max")
	lifetime := viper.GetInt("database.pool.lifetime")

	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = 5432
	}
	if dbname == "" {
		dbname = "idx_mcp"
	}
	if sslmode == "" {
		sslmode = "disable"
	}
	if idle == 0 {
		idle = 5
	}
	if max == 0 {
		max = 10
	}
	if lifetime == 0 {
		lifetime = 300
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		user, password, host, port, dbname, sslmode,
	)

	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	db.SetMaxIdleConns(idle)
	db.SetMaxOpenConns(max)
	db.SetConnMaxLifetime(time.Duration(lifetime) * time.Second)

	log.Info("database connected")
	return db
}
