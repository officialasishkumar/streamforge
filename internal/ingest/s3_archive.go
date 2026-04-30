package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	streamtypes "github.com/officialasishkumar/streamforge/internal/types"
	"github.com/sony/gobreaker"
)

type Archiver interface {
	ArchiveBatch(ctx context.Context, tenantID, correlationID string, events []streamtypes.Event) (string, error)
}

type S3Archiver struct {
	client  *s3.Client
	bucket  string
	prefix  string
	breaker *gobreaker.CircuitBreaker
}

func NewS3Archiver(ctx context.Context, endpoint, region, bucket, prefix string) (*S3Archiver, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, fmt.Errorf("s3 archiver: load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return &S3Archiver{
		client: client,
		bucket: bucket,
		prefix: prefix,
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "s3-archiver",
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
			MaxRequests: 1,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
		}),
	}, nil
}

func (a *S3Archiver) ArchiveBatch(ctx context.Context, tenantID, correlationID string, events []streamtypes.Event) (string, error) {
	key := fmt.Sprintf("%s%s/%s/%d.json", a.prefix, tenantID, correlationID, time.Now().UTC().UnixNano())
	blob, err := json.Marshal(map[string]any{
		"tenant_id":      tenantID,
		"correlation_id": correlationID,
		"events":         events,
		"archived_at":    time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return "", fmt.Errorf("s3 archiver: marshal payload: %w", err)
	}

	_, err = a.breaker.Execute(func() (any, error) {
		_, err := a.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(a.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(blob),
			ContentType: aws.String("application/json"),
			Metadata: map[string]string{
				"correlation_id": correlationID,
				"tenant_id":      tenantID,
			},
			StorageClass: types.StorageClassStandard,
		})
		if err != nil {
			return nil, fmt.Errorf("put object: %w", err)
		}
		return nil, nil
	})
	if err != nil {
		return "", fmt.Errorf("s3 archiver: archive batch: %w", err)
	}
	return key, nil
}
