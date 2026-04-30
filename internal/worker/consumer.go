package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/officialasishkumar/streamforge/internal/store"
	"github.com/officialasishkumar/streamforge/internal/types"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Config struct {
	Brokers       string
	Topic         string
	GroupID       string
	PoolSize      int64
	OutboxBatch   int32
	PollTimeoutMs int
}

type Worker struct {
	cfg      Config
	consumer *kafka.Consumer
	store    Store
	idem     IdempotencyChecker
	outbox   Publisher
	log      *slog.Logger
	stopOnce sync.Once
}

func New(cfg Config, st Store, idem IdempotencyChecker, outbox Publisher, log *slog.Logger) (*Worker, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        cfg.Brokers,
		"group.id":                 cfg.GroupID,
		"enable.auto.commit":       false,
		"auto.offset.reset":        "earliest",
		"go.events.channel.enable": false,
	})
	if err != nil {
		return nil, fmt.Errorf("worker: init kafka consumer: %w", err)
	}
	if err := c.Subscribe(cfg.Topic, nil); err != nil {
		return nil, fmt.Errorf("worker: subscribe topic: %w", err)
	}
	return &Worker{
		cfg:      cfg,
		consumer: c,
		store:    st,
		idem:     idem,
		outbox:   outbox,
		log:      log.With("component", "worker"),
	}, nil
}

type Store interface {
	InsertDLQEvent(ctx context.Context, tenantID, eventType string, body json.RawMessage, reason, correlationID string) error
	InsertEventWithOutbox(ctx context.Context, evt store.EventRecord, destination string, outboxPayload json.RawMessage) error
	FindUnsentOutboxRows(ctx context.Context, limit int32) ([]store.OutboxRow, error)
	MarkOutboxSent(ctx context.Context, id int64) error
}

func (w *Worker) Run(ctx context.Context) error {
	grp, ctx := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(w.cfg.PoolSize)

	grp.Go(func() error { return w.runOutboxPublisher(ctx) })
	grp.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			ev := w.consumer.Poll(w.cfg.PollTimeoutMs)
			if ev == nil {
				continue
			}

			switch m := ev.(type) {
			case *kafka.Message:
				if err := sem.Acquire(ctx, 1); err != nil {
					return fmt.Errorf("worker: acquire processing slot: %w", err)
				}
				msg := *m
				grp.Go(func() error {
					defer sem.Release(1)
					if err := w.processMessage(ctx, &msg); err != nil {
						w.log.Error("message processing failed", "error", err)
					}
					return nil
				})
			case kafka.Error:
				w.log.Error("kafka consumer error", "error", m)
			}
		}
	})

	return grp.Wait()
}

func (w *Worker) processMessage(ctx context.Context, msg *kafka.Message) error {
	commit := func() error {
		_, err := w.consumer.CommitMessage(msg)
		return err
	}

	var event types.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return w.processMalformedMessage(ctx, msg.Value, commit, err)
	}

	record := store.EventRecord{
		TenantID:       event.TenantID,
		EventType:      event.EventType,
		EventTime:      event.ClientTS,
		Body:           event.Body,
		CorrelationID:  event.CorrelationID,
		IdempotencyKey: event.IdempotencyKey,
		KafkaTopic:     *msg.TopicPartition.Topic,
		KafkaPartition: msg.TopicPartition.Partition,
		KafkaOffset:    int64(msg.TopicPartition.Offset),
	}
	return w.processDecodedEvent(ctx, event, record, commit)
}

func (w *Worker) processMalformedMessage(ctx context.Context, body []byte, commit func() error, parseErr error) error {
	if dlqErr := w.store.InsertDLQEvent(ctx, "unknown", "unknown", json.RawMessage(body), "invalid_json", "unknown"); dlqErr != nil {
		return fmt.Errorf("worker: parse failed + dlq insert failed: %w", dlqErr)
	}
	if err := commit(); err != nil {
		return fmt.Errorf("worker: commit malformed offset after dlq: %w", err)
	}
	w.log.Warn("malformed event sent to dlq", "error", parseErr)
	return nil
}

func (w *Worker) Close() {
	w.stopOnce.Do(func() {
		w.consumer.Close()
	})
}
