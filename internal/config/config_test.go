package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesEnvironmentOverrides(t *testing.T) {
	path := writeTestConfig(t)
	t.Setenv("STREAMFORGE_POSTGRES_DSN", "postgres://postgres:postgres@postgres:5432/streamforge?sslmode=disable")
	t.Setenv("STREAMFORGE_REDIS_ADDR", "redis:6379")
	t.Setenv("STREAMFORGE_KAFKA_BROKERS", "kafka-a:9092,kafka-b:9092")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Postgres.DSN != "postgres://postgres:postgres@postgres:5432/streamforge?sslmode=disable" {
		t.Fatalf("postgres dsn override not applied: %q", cfg.Postgres.DSN)
	}
	if cfg.Redis.Addr != "redis:6379" {
		t.Fatalf("redis addr override not applied: %q", cfg.Redis.Addr)
	}
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "kafka-a:9092" || cfg.Kafka.Brokers[1] != "kafka-b:9092" {
		t.Fatalf("kafka brokers override not applied: %#v", cfg.Kafka.Brokers)
	}
}

func writeTestConfig(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "streamforge.yaml")
	data := []byte(`
ingest:
  port: 8080
  max_batch_size: 1000
  request_timeout: 5s
workers:
  pool_size: 8
  batch_size: 500
  fetch_timeout: 2s
kafka:
  brokers:
    - localhost:9092
  topics:
    events: streamforge.events
    dlq: streamforge.dlq
    outbox: streamforge.outbox
  partitioner_strategy: tenant_hash
postgres:
  dsn: postgres://postgres:postgres@localhost:5432/streamforge?sslmode=disable
  pool_min: 4
  pool_max: 32
  statement_timeout: 5s
s3:
  bucket: streamforge-events
  endpoint: http://localhost:4566
  region: us-east-1
  archive_prefix: raw-events/
sqs:
  queue_url: http://localhost:4566/000000000000/streamforge-outbox
  endpoint: http://localhost:4566
  region: us-east-1
redis:
  addr: localhost:6379
  db: 0
  ratelimit_key_prefix: "streamforge:ratelimit:"
observability:
  metrics_addr: ":9090"
  log_level: info
  sample_rate: 1.0
rate_limits:
  default_per_tenant_rps: 1000
  default_burst: 2000
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write test config: %v", err)
	}
	return path
}
