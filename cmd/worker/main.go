package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/officialasishkumar/streamforge/internal/config"
	"github.com/officialasishkumar/streamforge/internal/store"
	"github.com/officialasishkumar/streamforge/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("streamforge.yaml")
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}

	st, err := store.New(ctx, cfg.Postgres.DSN, cfg.Postgres.PoolMax, log)
	if err != nil {
		log.Error("init store", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.Redis.Addr,
		DB:   cfg.Redis.DB,
	})
	defer redisClient.Close()

	publisher, err := worker.NewSQSPublisher(ctx, cfg.SQS.Endpoint, cfg.SQS.Region, cfg.SQS.QueueURL, st, log)
	if err != nil {
		log.Error("init outbox publisher", "error", err)
		os.Exit(1)
	}

	w, err := worker.New(worker.Config{
		Brokers:       strings.Join(cfg.Kafka.Brokers, ","),
		Topic:         cfg.Kafka.Topics.Events,
		GroupID:       "streamforge-workers",
		PoolSize:      int64(cfg.Workers.PoolSize),
		OutboxBatch:   int32(cfg.Workers.BatchSize),
		PollTimeoutMs: int(cfg.Workers.FetchTimeout.Milliseconds()),
	}, st, worker.NewRedisIdempotencyChecker(redisClient, "streamforge:idem:"), publisher, log)
	if err != nil {
		log.Error("init worker", "error", err)
		os.Exit(1)
	}
	defer w.Close()

	if err := w.Run(ctx); err != nil {
		log.Error("worker runtime failure", "error", err)
		os.Exit(1)
	}
}
