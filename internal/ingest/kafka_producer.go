package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/officialasishkumar/streamforge/internal/types"
	"github.com/sony/gobreaker"
)

type Producer interface {
	PublishBatch(ctx context.Context, tenantID, correlationID, archiveObject string, events []types.Event) error
	Close()
}

type KafkaProducer struct {
	topic   string
	prod    *kafka.Producer
	breaker *gobreaker.CircuitBreaker
}

func NewKafkaProducer(brokers string, topic string) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"acks":               "all",
		"enable.idempotence": true,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka producer: init: %w", err)
	}
	return &KafkaProducer{
		topic: topic,
		prod:  p,
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "kafka-producer",
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
			MaxRequests: 1,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
		}),
	}, nil
}

func (k *KafkaProducer) PublishBatch(ctx context.Context, tenantID, correlationID, archiveObject string, events []types.Event) error {
	_, err := k.breaker.Execute(func() (any, error) {
		for _, e := range events {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("request context canceled: %w", ctx.Err())
			default:
			}
			payload, err := json.Marshal(e)
			if err != nil {
				return nil, fmt.Errorf("marshal event: %w", err)
			}
			headers := []kafka.Header{
				{Key: "x-request-id", Value: []byte(correlationID)},
				{Key: "archive-object", Value: []byte(archiveObject)},
				{Key: "idempotency-key", Value: []byte(e.IdempotencyKey)},
			}

			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
				Key:            []byte(tenantID),
				Value:          payload,
				Headers:        headers,
			}
			if err := k.prod.Produce(msg, nil); err != nil {
				return nil, fmt.Errorf("produce event: %w", err)
			}
		}
		// Deliver reports asynchronously; bounded wait for flush on request path.
		if remaining := k.prod.Flush(3000); remaining > 0 {
			return nil, fmt.Errorf("kafka producer flush incomplete, remaining=%d", remaining)
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("kafka producer: publish batch: %w", err)
	}
	return nil
}

func (k *KafkaProducer) Close() {
	k.prod.Close()
}
