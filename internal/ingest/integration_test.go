//go:build integration

package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/officialasishkumar/streamforge/internal/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIngestIntegrationFlows(t *testing.T) {
	ctx := context.Background()

	containers := []testcontainers.Container{
		startContainer(t, ctx, "postgres:16", "5432/tcp"),
		startContainer(t, ctx, "redis:7", "6379/tcp"),
		startContainer(t, ctx, "localstack/localstack:3", "4566/tcp"),
		startContainer(t, ctx, "confluentinc/cp-kafka:7.6.1", "9092/tcp"),
	}
	for _, c := range containers {
		t.Cleanup(func() { _ = c.Terminate(ctx) })
	}

	t.Run("rate_limited", func(t *testing.T) {
		h := NewHandler(ServerConfig{MaxBatchSize: 1000, RequestTimeout: 2 * time.Second}, Dependencies{
			SchemaCache: NewSchemaCache(mockSchemaProvider{}, time.Minute),
			RateLimiter: mockRateLimiter{allowed: false, retry: 3 * time.Second},
			Producer:    mockProducer{},
			Archiver:    mockArchiver{},
			Readiness:   staticReady{},
		}, testLogger(t))

		req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(validPayload()))
		rec := httptest.NewRecorder()
		h.WithMiddleware(http.HandlerFunc(h.HandleEvents)).ServeHTTP(rec, req)

		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("expected 429 got %d", rec.Code)
		}
	})

	t.Run("schema_rejection", func(t *testing.T) {
		h := NewHandler(ServerConfig{MaxBatchSize: 1000, RequestTimeout: 2 * time.Second}, Dependencies{
			SchemaCache: NewSchemaCache(mockSchemaProvider{reject: true}, time.Minute),
			RateLimiter: mockRateLimiter{allowed: true},
			Producer:    mockProducer{},
			Archiver:    mockArchiver{},
			Readiness:   staticReady{},
		}, testLogger(t))

		req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(validPayload()))
		rec := httptest.NewRecorder()
		h.WithMiddleware(http.HandlerFunc(h.HandleEvents)).ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 got %d", rec.Code)
		}
	})

	t.Run("dependency_failure_returns_503", func(t *testing.T) {
		h := NewHandler(ServerConfig{MaxBatchSize: 1000, RequestTimeout: 2 * time.Second}, Dependencies{
			SchemaCache: NewSchemaCache(mockSchemaProvider{}, time.Minute),
			RateLimiter: mockRateLimiter{allowed: true},
			Producer:    mockProducer{err: context.DeadlineExceeded},
			Archiver:    mockArchiver{},
			Readiness:   staticReady{},
		}, testLogger(t))

		req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(validPayload()))
		rec := httptest.NewRecorder()
		h.WithMiddleware(http.HandlerFunc(h.HandleEvents)).ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 got %d", rec.Code)
		}
	})
}

func startContainer(t *testing.T, ctx context.Context, image, exposedPort string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{exposedPort},
		WaitingFor:   wait.ForListeningPort(exposedPort).WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container %s: %v", image, err)
	}
	return c
}

type mockSchemaProvider struct{ reject bool }

func (m mockSchemaProvider) ActiveSchemasForTenant(context.Context, string) (map[string]json.RawMessage, error) {
	if m.reject {
		return map[string]json.RawMessage{
			"user.signup": json.RawMessage(`{"type":"object","required":["must_not_exist"]}`),
		}, nil
	}
	return map[string]json.RawMessage{
		"user.signup": json.RawMessage(`{"type":"object"}`),
	}, nil
}

type mockRateLimiter struct {
	allowed bool
	retry   time.Duration
}

func (m mockRateLimiter) Allow(context.Context, string, int) (bool, time.Duration, error) {
	return m.allowed, m.retry, nil
}

type mockArchiver struct{}

func (mockArchiver) ArchiveBatch(context.Context, string, string, []types.Event) (string, error) {
	return "archive/key.json", nil
}

type mockProducer struct{ err error }

func (m mockProducer) PublishBatch(context.Context, string, string, string, []types.Event) error {
	return m.err
}
func (mockProducer) Close() {}

type staticReady struct{}

func (staticReady) Ready(context.Context) error { return nil }

func validPayload() string {
	return `{"tenant_id":"tenant-a","events":[{"event_type":"user.signup","body":{"source":"web"},"client_timestamp":"2026-05-01T00:00:00Z"}]}`
}
