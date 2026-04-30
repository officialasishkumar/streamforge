-- name: InsertOutboxRow :one
INSERT INTO outbox (
    tenant_id,
    event_id,
    event_time,
    destination,
    payload
)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, tenant_id, event_id, event_time, destination, payload, created_at, sent_at;

-- name: FindUnsentOutboxRows :many
SELECT
    id,
    tenant_id,
    event_id,
    event_time,
    destination,
    payload,
    created_at,
    sent_at
FROM outbox
WHERE sent_at IS NULL
ORDER BY id
LIMIT $1;

-- name: MarkOutboxSent :exec
UPDATE outbox
SET sent_at = NOW()
WHERE id = $1;
