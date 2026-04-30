package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/officialasishkumar/streamforge/internal/store"
	"github.com/officialasishkumar/streamforge/internal/types"
)

func (w *Worker) processDecodedEvent(ctx context.Context, event types.Event, record store.EventRecord, commit func() error) error {
	seen, err := w.idem.Seen(ctx, event.TenantID, event.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("worker: idempotency check: %w", err)
	}
	if seen {
		return commit()
	}
	outboxPayload, _ := json.Marshal(map[string]any{
		"tenant_id":       event.TenantID,
		"event_type":      event.EventType,
		"idempotency_key": event.IdempotencyKey,
		"correlation_id":  event.CorrelationID,
	})
	if err := w.store.InsertEventWithOutbox(ctx, record, "sqs", outboxPayload); err != nil {
		return fmt.Errorf("worker: write event and outbox: %w", err)
	}
	if err := w.idem.Mark(ctx, event.TenantID, event.IdempotencyKey, 24*time.Hour); err != nil {
		return fmt.Errorf("worker: idempotency mark: %w", err)
	}
	if err := commit(); err != nil {
		return fmt.Errorf("worker: commit offset: %w", err)
	}
	return nil
}
