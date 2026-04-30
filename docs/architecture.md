# Architecture

StreamForge separates ingest durability from processing side effects so any crash before offset commit results in safe redelivery.

## Data Flow

1. Ingest receives tenant-scoped batches over HTTP.
2. Batch is archived to S3 before client ack.
3. Events are published to Kafka keyed by `tenant_id`.
4. Worker consumer processes events with idempotency checks.
5. Worker stores event and outbox row transactionally.
6. Outbox publisher emits SQS notification and marks outbox sent.

## Failure Isolation

- Circuit breakers protect external dependency calls.
- Buffered worker pools and semaphores cap concurrency.
- Redelivery semantics rely on manual Kafka offset commit after durable write.

## Consistency and Ordering

- Tenant key partitioning preserves per-tenant Kafka ordering.
- Postgres unique `(tenant_id, idempotency_key)` prevents duplicate durable writes.
- Redis idempotency cache short-circuits known duplicates in hot paths.
