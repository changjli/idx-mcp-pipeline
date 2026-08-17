package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// ErrObjectNotFound is returned by GetObject when the key does not exist in
// the bucket. The read_idx_disclosure tool maps it to status="evicted".
var ErrObjectNotFound = errors.New("object not found")

// ObjectStore is the minimal surface the pipeline needs from object storage.
// Kept small so handlers can accept a fake in tests.
type ObjectStore interface {
	PutObject(ctx context.Context, key string, data []byte) error
	GetObject(ctx context.Context, key string) ([]byte, error)
	// DeleteObject removes an object. S3 DeleteObject is idempotent — deleting
	// a missing key succeeds — so retention cleanup can re-run safely.
	DeleteObject(ctx context.Context, key string) error
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

// DeleteObject removes an object from the bucket. Idempotent: deleting a
// missing key succeeds, so cleanup can re-run without erroring on already-
// evicted objects.
func (s *R2Store) DeleteObject(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("r2 delete %s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// GetObject fetches an object's bytes. A missing key maps to
// ErrObjectNotFound so callers can distinguish eviction from real failures.
func (s *R2Store) GetObject(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrObjectNotFound
		}
		// Some S3-compatible endpoints surface a missing key as a generic API
		// error rather than a modeled NoSuchKey; match on the error code.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NoSuchKey", "NotFound":
				return nil, ErrObjectNotFound
			}
		}
		return nil, fmt.Errorf("r2 get %s/%s: %w", s.bucket, key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("r2 read %s/%s: %w", s.bucket, key, err)
	}
	return data, nil
}
