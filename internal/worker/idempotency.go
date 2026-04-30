package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
)

type IdempotencyChecker interface {
	Seen(ctx context.Context, tenantID, key string) (bool, error)
	Mark(ctx context.Context, tenantID, key string, ttl time.Duration) error
}

type RedisIdempotencyChecker struct {
	client  redis.UniversalClient
	prefix  string
	breaker *gobreaker.CircuitBreaker
}

func NewRedisIdempotencyChecker(client redis.UniversalClient, prefix string) *RedisIdempotencyChecker {
	return &RedisIdempotencyChecker{
		client: client,
		prefix: prefix,
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "redis-idempotency",
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
			MaxRequests: 1,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
		}),
	}
}

func (r *RedisIdempotencyChecker) Seen(ctx context.Context, tenantID, key string) (bool, error) {
	lookup := fmt.Sprintf("%s%s:%s", r.prefix, tenantID, key)
	v, err := r.breaker.Execute(func() (any, error) {
		exists, err := r.client.Exists(ctx, lookup).Result()
		if err != nil {
			return nil, err
		}
		return exists > 0, nil
	})
	if err != nil {
		return false, fmt.Errorf("worker: redis seen check: %w", err)
	}
	seen, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("worker: redis seen type assertion failed")
	}
	return seen, nil
}

func (r *RedisIdempotencyChecker) Mark(ctx context.Context, tenantID, key string, ttl time.Duration) error {
	lookup := fmt.Sprintf("%s%s:%s", r.prefix, tenantID, key)
	_, err := r.breaker.Execute(func() (any, error) {
		if err := r.client.Set(ctx, lookup, "1", ttl).Err(); err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("worker: redis mark idempotency: %w", err)
	}
	return nil
}
