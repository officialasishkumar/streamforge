-- name: FetchActiveSchemasByTenant :many
SELECT
    id,
    tenant_id,
    event_type,
    schema,
    active,
    created_at
FROM event_schemas
WHERE tenant_id = $1
  AND active = TRUE
ORDER BY event_type ASC;
