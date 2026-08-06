package config

import (
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// NewRedisConnOpt returns a RedisClientOpt from viper config.
// Shared by asynq client, server, and scheduler.
func NewRedisConnOpt(vip *viper.Viper) asynq.RedisClientOpt {
	addr := vip.GetString("redis.address")
	password := vip.GetString("redis.password")
	db := vip.GetInt("redis.db")

	if addr == "" {
		addr = "localhost:6379"
	}

	return asynq.RedisClientOpt{
		Addr:     addr,
		Password: password,
		DB:       db,
	}
}

// NewAsynqClient creates an asynq client for enqueuing tasks.
func NewAsynqClient(vip *viper.Viper, log *logrus.Logger) *asynq.Client {
	client := asynq.NewClient(NewRedisConnOpt(vip))
	log.Info("asynq client configured")
	return client
}

// NewAsynqServer creates an asynq server for processing tasks.
func NewAsynqServer(vip *viper.Viper, log *logrus.Logger) *asynq.Server {
	srv := asynq.NewServer(
		NewRedisConnOpt(vip),
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"ingest":  10,
				"default": 5,
			},
			Logger: log,
		},
	)
	log.Info("asynq server configured")
	return srv
}
