-- name: InsertDLQEvent :one
INSERT INTO dlq_events (
    tenant_id,
    event_type,
    body,
    reason,
    correlation_id
)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, tenant_id, event_type, body, reason, correlation_id, created_at;
