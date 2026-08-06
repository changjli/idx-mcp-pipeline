package config

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func NewR2Client(vip *viper.Viper, log *logrus.Logger) *s3.Client {
	endpoint := vip.GetString("r2.endpoint")
	region := vip.GetString("r2.region")
	accessKey := vip.GetString("r2.access_key")
	secretKey := vip.GetString("r2.secret_key")

	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	if region == "" {
		region = "auto"
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		log.Fatalf("failed to load R2 config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})

	log.Info("R2 client configured")
	return client
}
