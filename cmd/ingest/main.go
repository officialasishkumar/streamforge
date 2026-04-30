package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/officialasishkumar/streamforge/internal/config"
	"github.com/officialasishkumar/streamforge/internal/ingest"
	"github.com/officialasishkumar/streamforge/internal/types"
)

type staticReadiness struct{}

func (staticReadiness) Ready(context.Context) error { return nil }

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

	// Step 3 wires concrete implementations in thin slices.
	handlerDeps := ingest.Dependencies{
		SchemaCache: ingest.NewSchemaCache(staticSchemaProvider{}, 60*time.Second),
		RateLimiter: &allowAllRateLimiter{},
		Producer:    &noopProducer{},
		Archiver:    &noopArchiver{},
		Readiness:   staticReadiness{},
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

type staticSchemaProvider struct{}

func (staticSchemaProvider) ActiveSchemasForTenant(context.Context, string) (map[string]json.RawMessage, error) {
	return map[string]json.RawMessage{
		"user.signup": json.RawMessage(`{"type":"object"}`),
	}, nil
}

type allowAllRateLimiter struct{}

func (*allowAllRateLimiter) Allow(context.Context, string, int) (bool, time.Duration, error) {
	return true, 0, nil
}

type noopProducer struct{}

func (*noopProducer) PublishBatch(context.Context, string, string, string, []types.Event) error {
	return nil
}
func (*noopProducer) Close() {}

type noopArchiver struct{}

func (*noopArchiver) ArchiveBatch(context.Context, string, string, []types.Event) (string, error) {
	return "noop/object.json", nil
}
