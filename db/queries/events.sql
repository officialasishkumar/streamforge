-- name: InsertEvent :execrows
INSERT INTO events_partitioned (
    tenant_id,
    event_type,
    event_time,
    body,
    correlation_id,
    idempotency_key,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;

-- name: BatchInsertEvents :copyfrom
INSERT INTO events_partitioned (
    tenant_id,
    event_type,
    event_time,
    body,
    correlation_id,
    idempotency_key,
    kafka_topic,
    kafka_partition,
    kafka_offset
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
