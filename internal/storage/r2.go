package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ObjectStore is the minimal surface the pipeline needs from object storage.
// Kept small so handlers can accept a fake in tests.
type ObjectStore interface {
	PutObject(ctx context.Context, key string, data []byte) error
}

// R2Store writes objects to a Cloudflare R2 bucket (S3-compatible API).
type R2Store struct {
	client *s3.Client
	bucket string
}

// NewR2Store wraps an S3 client with the target bucket name.
func NewR2Store(client *s3.Client, bucket string) *R2Store {
	return &R2Store{client: client, bucket: bucket}
}

// PutObject uploads data under key, overwriting any existing object.
func (s *R2Store) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("r2 put %s/%s: %w", s.bucket, key, err)
	}
	return nil
}
