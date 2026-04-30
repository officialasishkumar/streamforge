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

	"github.com/docker/go-connections/nat"
	"github.com/officialasishkumar/streamforge/internal/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIngestIntegrationFlows(t *testing.T) {
	ctx := context.Background()

	specs := []containerSpec{
		{
			image: "postgres:16",
			port:  "5432/tcp",
			env: map[string]string{
				"POSTGRES_DB":       "streamforge",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "postgres",
			},
		},
		{image: "redis:7", port: "6379/tcp"},
		{
			image: "localstack/localstack:3",
			port:  "4566/tcp",
			env: map[string]string{
				"SERVICES":           "s3,sqs",
				"AWS_DEFAULT_REGION": "us-east-1",
			},
		},
		{
			image: "confluentinc/cp-kafka:7.6.1",
			port:  "9092/tcp",
			env: map[string]string{
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
				"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
				"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://localhost:9092",
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
				"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "true",
				"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
			},
		},
	}
	containers := make([]testcontainers.Container, 0, len(specs))
	for _, spec := range specs {
		containers = append(containers, startContainer(t, ctx, spec))
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

type containerSpec struct {
	image string
	port  string
	env   map[string]string
}

func startContainer(t *testing.T, ctx context.Context, spec containerSpec) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        spec.image,
		Env:          spec.env,
		ExposedPorts: []string{spec.port},
		WaitingFor:   wait.ForListeningPort(nat.Port(spec.port)).WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container %s: %v", spec.image, err)
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
