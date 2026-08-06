package config

import (
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func NewAsynqClient(vip *viper.Viper, log *logrus.Logger) *asynq.Client {
	addr := vip.GetString("redis.address")
	password := vip.GetString("redis.password")
	db := vip.GetInt("redis.db")

	if addr == "" {
		addr = "localhost:6379"
	}

	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	log.Info("asynq client configured")
	return client
}
