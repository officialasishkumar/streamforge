# Operations

## Health and Readiness

- `/healthz`: process liveness check.
- `/readyz`: dependency readiness check used by load balancers and Kubernetes probes.

## Common Failure Modes

### Postgres Unavailable

- Symptom: worker commit stalls, consumer lag climbs.
- Action: verify Postgres health, monitor outbox backlog, avoid forced offset commits.

### Kafka Instability

- Symptom: ingest returns elevated 503, producer circuit opens.
- Action: inspect broker ISR/under-replicated partitions, reduce ingest load using rate-limits.

### Redis Outage

- Symptom: idempotency and rate-limiter dependency failures.
- Action: use configured fail-open policy for ingest; monitor duplicate write safeguards in Postgres.

### DLQ Growth

- Symptom: `dlq_events` insert rate spikes.
- Action: query DLQ by `event_type` and `correlation_id`, validate schema changes and producer contracts.

## Runbooks

- Replay archived events with controlled `--rps`.
- Trigger partition creation script before month rollover.
- Run chaos scripts after deployment changes to validate resiliency assumptions.
