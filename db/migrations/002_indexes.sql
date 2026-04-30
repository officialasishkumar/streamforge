-- +goose Up
CREATE INDEX IF NOT EXISTS idx_events_tenant_time_desc
    ON events_partitioned (tenant_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_events_tenant_type_time_desc
    ON events_partitioned (tenant_id, event_type, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_outbox_unsent
    ON outbox (created_at, id)
    WHERE sent_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_outbox_unsent;
DROP INDEX IF EXISTS idx_events_tenant_type_time_desc;
DROP INDEX IF EXISTS idx_events_tenant_time_desc;
