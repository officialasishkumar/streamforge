package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
)

type RateLimiter interface {
	Allow(ctx context.Context, tenantID string, cost int) (bool, time.Duration, error)
}

type RedisRateLimiter struct {
	limiter   *redis_rate.Limiter
	failOpen  bool
	rps       int
	burst     int
	keyPrefix string
	breaker   *gobreaker.CircuitBreaker
}

func NewRedisRateLimiter(client redis.UniversalClient, keyPrefix string, rps, burst int, failOpen bool) *RedisRateLimiter {
	return &RedisRateLimiter{
		limiter:   redis_rate.NewLimiter(client),
		failOpen:  failOpen,
		rps:       rps,
		burst:     burst,
		keyPrefix: keyPrefix,
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "redis-ratelimit",
			Interval:    30 * time.Second,
			Timeout:     60 * time.Second,
			MaxRequests: 1,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
		}),
	}
}

func (r *RedisRateLimiter) Allow(ctx context.Context, tenantID string, cost int) (bool, time.Duration, error) {
	key := fmt.Sprintf("%s%s", r.keyPrefix, tenantID)

	resAny, err := r.breaker.Execute(func() (any, error) {
		limit := redis_rate.Limit{
			Rate:   r.rps,
			Burst:  r.burst,
			Period: time.Second,
		}
		return r.limiter.AllowN(ctx, key, limit, cost)
	})
	if err != nil {
		if r.failOpen {
			return true, 0, nil
		}
		return false, 2 * time.Second, fmt.Errorf("ratelimit: allow: %w", err)
	}

	res, ok := resAny.(*redis_rate.Result)
	if !ok {
		return false, 2 * time.Second, fmt.Errorf("ratelimit: unexpected result type")
	}
	if res.Allowed == 0 {
		retryAfter := time.Duration(res.RetryAfter) * time.Second
		if retryAfter <= 0 {
			retryAfter = time.Second
		}
		return false, retryAfter, nil
	}
	return true, 0, nil
}
