package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sony/gobreaker"
)

type EventRecord struct {
	TenantID       string
	EventType      string
	EventTime      time.Time
	Body           json.RawMessage
	CorrelationID  string
	IdempotencyKey string
	KafkaTopic     string
	KafkaPartition int32
	KafkaOffset    int64
}

type OutboxRow struct {
	ID          int64
	TenantID    string
	EventID     int64
	EventTime   time.Time
	Destination string
	Payload     json.RawMessage
	CreatedAt   time.Time
	SentAt      *time.Time
}

type Store struct {
	pool       *pgxpool.Pool
	log        *slog.Logger
	breakerSQL *gobreaker.CircuitBreaker
}

func New(ctx context.Context, dsn string, maxConns int32, log *slog.Logger) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse pool config: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 1
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: create pgx pool: %w", err)
	}

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "postgres",
		Interval:    30 * time.Second,
		Timeout:     60 * time.Second,
		MaxRequests: 1,
		ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= 5 },
	})

	return &Store{
		pool:       pool,
		log:        log.With("component", "store"),
		breakerSQL: cb,
	}, nil
}

func (s *Store) Ping(ctx context.Context) error {
	_, err := s.breakerSQL.Execute(func() (any, error) {
		return nil, s.pool.Ping(ctx)
	})
	if err != nil {
		return fmt.Errorf("store: ping postgres: %w", err)
	}
	return nil
}

func (s *Store) InsertEventWithOutbox(ctx context.Context, evt EventRecord, destination string, outboxPayload json.RawMessage) error {
	_, err := s.breakerSQL.Execute(func() (any, error) {
		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return nil, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback(ctx) // nolint:errcheck

		eventInsert := `
INSERT INTO events_partitioned (
	tenant_id, event_type, event_time, body, correlation_id, idempotency_key, kafka_topic, kafka_partition, kafka_offset
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;
`
		ct, err := tx.Exec(
			ctx,
			eventInsert,
			evt.TenantID,
			evt.EventType,
			evt.EventTime,
			evt.Body,
			evt.CorrelationID,
			evt.IdempotencyKey,
			evt.KafkaTopic,
			evt.KafkaPartition,
			evt.KafkaOffset,
		)
		if err != nil {
			return nil, fmt.Errorf("insert event: %w", err)
		}
		if ct.RowsAffected() == 0 {
			s.log.Info("deduplicated event", "tenant_id", evt.TenantID, "event_type", evt.EventType, "idempotency_key", evt.IdempotencyKey)
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit dedup tx: %w", err)
			}
			return nil, nil
		}

		var eventID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM events_partitioned WHERE tenant_id=$1 AND idempotency_key=$2`, evt.TenantID, evt.IdempotencyKey).Scan(&eventID); err != nil {
			return nil, fmt.Errorf("fetch inserted event id: %w", err)
		}

		_, err = tx.Exec(ctx, `
INSERT INTO outbox (tenant_id, event_id, event_time, destination, payload)
VALUES ($1, $2, $3, $4, $5)
`, evt.TenantID, eventID, evt.EventTime, destination, outboxPayload)
		if err != nil {
			return nil, fmt.Errorf("insert outbox row: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit tx: %w", err)
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("store: insert event with outbox: %w", err)
	}
	return nil
}

func (s *Store) FindUnsentOutboxRows(ctx context.Context, limit int32) ([]OutboxRow, error) {
	v, err := s.breakerSQL.Execute(func() (any, error) {
		rows, err := s.pool.Query(ctx, `
SELECT id, tenant_id, event_id, event_time, destination, payload, created_at, sent_at
FROM outbox
WHERE sent_at IS NULL
ORDER BY id
LIMIT $1
`, limit)
		if err != nil {
			return nil, fmt.Errorf("query unsent outbox: %w", err)
		}
		defer rows.Close()

		out := make([]OutboxRow, 0, limit)
		for rows.Next() {
			var row OutboxRow
			if err := rows.Scan(&row.ID, &row.TenantID, &row.EventID, &row.EventTime, &row.Destination, &row.Payload, &row.CreatedAt, &row.SentAt); err != nil {
				return nil, fmt.Errorf("scan unsent outbox row: %w", err)
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate unsent outbox rows: %w", err)
		}
		return out, nil
	})
	if err != nil {
		return nil, fmt.Errorf("store: find unsent outbox rows: %w", err)
	}
	rows, ok := v.([]OutboxRow)
	if !ok {
		return nil, fmt.Errorf("store: unexpected outbox query result type")
	}
	return rows, nil
}

func (s *Store) MarkOutboxSent(ctx context.Context, id int64) error {
	_, err := s.breakerSQL.Execute(func() (any, error) {
		_, err := s.pool.Exec(ctx, `UPDATE outbox SET sent_at = NOW() WHERE id = $1`, id)
		if err != nil {
			return nil, fmt.Errorf("mark outbox sent: %w", err)
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("store: mark outbox sent: %w", err)
	}
	return nil
}

func (s *Store) InsertDLQEvent(ctx context.Context, tenantID, eventType string, body json.RawMessage, reason, correlationID string) error {
	_, err := s.breakerSQL.Execute(func() (any, error) {
		_, err := s.pool.Exec(ctx, `
INSERT INTO dlq_events (tenant_id, event_type, body, reason, correlation_id)
VALUES ($1, $2, $3, $4, $5)
`, tenantID, eventType, body, reason, correlationID)
		if err != nil {
			return nil, fmt.Errorf("insert dlq event: %w", err)
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("store: insert dlq event: %w", err)
	}
	return nil
}

func (s *Store) Close() {
	s.pool.Close()
}
