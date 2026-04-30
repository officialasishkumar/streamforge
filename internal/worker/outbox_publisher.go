package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/cenkalti/backoff/v4"
	"github.com/officialasishkumar/streamforge/internal/store"
	"github.com/sony/gobreaker"
)

type Publisher interface {
	Publish(ctx context.Context, rows []store.OutboxRow) error
}

type SQSPublisher struct {
	queueURL string
	client   *sqs.Client
	store    Store
	log      *slog.Logger
	breaker  *gobreaker.CircuitBreaker
}

func NewSQSPublisher(ctx context.Context, endpoint, region, queueURL string, st Store, log *slog.Logger) (*SQSPublisher, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, fmt.Errorf("worker: load aws config: %w", err)
	}
	client := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	return &SQSPublisher{
		queueURL: queueURL,
		client:   client,
		store:    st,
		log:      log.With("component", "outbox_publisher"),
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "sqs-publisher",
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
			MaxRequests: 1,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
		}),
	}, nil
}

func (s *SQSPublisher) Publish(ctx context.Context, rows []store.OutboxRow) error {
	for _, row := range rows {
		payload := string(row.Payload)
		operation := func() error {
			_, err := s.breaker.Execute(func() (any, error) {
				_, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{
					QueueUrl:    aws.String(s.queueURL),
					MessageBody: aws.String(payload),
				})
				return nil, err
			})
			return err
		}
		b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
		if err := backoff.Retry(operation, backoff.WithMaxRetries(b, 5)); err != nil {
			return fmt.Errorf("worker: publish outbox row id=%d: %w", row.ID, err)
		}
		if err := s.store.MarkOutboxSent(ctx, row.ID); err != nil {
			return fmt.Errorf("worker: mark outbox row sent id=%d: %w", row.ID, err)
		}
	}
	return nil
}

func (w *Worker) runOutboxPublisher(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			rows, err := w.store.FindUnsentOutboxRows(ctx, w.cfg.OutboxBatch)
			if err != nil {
				w.log.Error("outbox fetch failed", "error", err)
				continue
			}
			if len(rows) == 0 {
				continue
			}
			if err := w.outbox.Publish(ctx, rows); err != nil {
				w.log.Error("outbox publish failed", "error", err)
				continue
			}
		}
	}
}
