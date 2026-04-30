package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/officialasishkumar/streamforge/internal/config"
	"github.com/officialasishkumar/streamforge/internal/ingest"
	"github.com/officialasishkumar/streamforge/internal/store"
	"github.com/redis/go-redis/v9"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load("streamforge.yaml")
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	serverCfg := ingest.ServerConfig{
		Port:           cfg.Ingest.Port,
		MaxBatchSize:   cfg.Ingest.MaxBatchSize,
		RequestTimeout: cfg.Ingest.RequestTimeout,
	}

	storeClient, err := store.New(context.Background(), cfg.Postgres.DSN, cfg.Postgres.PoolMax, log)
	if err != nil {
		log.Error("failed to initialize store", "error", err)
		os.Exit(1)
	}
	defer storeClient.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.Redis.Addr,
		DB:   cfg.Redis.DB,
	})
	defer redisClient.Close()

	rateLimiter := ingest.NewRedisRateLimiter(
		redisClient,
		cfg.Redis.RateLimitKeyPrefix,
		cfg.RateLimits.DefaultPerTenantRPS,
		cfg.RateLimits.DefaultBurst,
		true,
	)

	producer, err := ingest.NewKafkaProducer(strings.Join(cfg.Kafka.Brokers, ","), cfg.Kafka.Topics.Events)
	if err != nil {
		log.Error("failed to initialize kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	archiver, err := ingest.NewS3Archiver(context.Background(), cfg.S3.Endpoint, cfg.S3.Region, cfg.S3.Bucket, cfg.S3.ArchivePrefix)
	if err != nil {
		log.Error("failed to initialize s3 archiver", "error", err)
		os.Exit(1)
	}

	handlerDeps := ingest.Dependencies{
		SchemaCache: ingest.NewSchemaCache(storeClient, 60*time.Second),
		RateLimiter: rateLimiter,
		Producer:    producer,
		Archiver:    archiver,
		Readiness: readiness{
			store: storeClient,
			redis: redisClient,
		},
	}

	srv := ingest.NewServer(serverCfg, handlerDeps, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("graceful shutdown failed", "error", err)
		}
	}()

	if err := srv.Start(); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("ingest server exited with error", "error", err)
		os.Exit(1)
	}
}

type readiness struct {
	store *store.Store
	redis *redis.Client
}

func (r readiness) Ready(ctx context.Context) error {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.store.Ping(checkCtx); err != nil {
		return fmt.Errorf("postgres not ready: %w", err)
	}
	if err := r.redis.Ping(checkCtx).Err(); err != nil {
		return fmt.Errorf("redis not ready: %w", err)
	}
	return nil
}
