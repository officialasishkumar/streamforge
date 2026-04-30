---
description: Project-wide StreamForge production constraints
alwaysApply: true
---

# StreamForge Project Rules

- Build a production-intent self-hosted event analytics platform.
- Use only the approved stack: Go 1.22+, Kafka via `confluent-kafka-go`, PostgreSQL 16 via `pgx/v5` and `sqlc`, Redis via `go-redis/v9`, AWS S3/SQS via `aws-sdk-go-v2`, Prometheus `client_golang`, structured `slog`, Helm and raw Kubernetes manifests, GitHub Actions, and `ghcr.io`.
- Do not introduce forbidden technologies: ClickHouse, Terraform, ArgoCD, KEDA, eksctl, standalone schema registry service, ORMs, Sarama, Gin, Echo, or Fiber.
- Treat reliability patterns as baseline requirements: idempotency, at-least-once processing, outbox pattern, bounded queues, retry budgets with exponential backoff and jitter, bulkheading, graceful shutdown, and strict health/readiness separation.
- Require structured logging with correlation IDs propagated from ingress through Kafka and persistence layers.
- Keep configuration externalized and environment-overridable; never hardcode secrets or endpoints.
