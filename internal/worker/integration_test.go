//go:build integration

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/officialasishkumar/streamforge/internal/store"
	"github.com/officialasishkumar/streamforge/internal/types"
)

func TestAtLeastOnceAndDedupOnCrash(t *testing.T) {
	ctx := context.Background()

	memStore := &memoryStore{}
	idem := &memoryIdempotency{seen: map[string]bool{}}
	w := &Worker{
		store: memStore,
		idem:  idem,
		log:   slog.Default(),
	}

	event := types.Event{
		TenantID:       "tenant-a",
		EventType:      "user.signup",
		Body:           json.RawMessage(`{"source":"web"}`),
		ClientTS:       time.Now().UTC(),
		CorrelationID:  "req-1",
		IdempotencyKey: "idem-1",
	}
	record := store.EventRecord{
		TenantID:       event.TenantID,
		EventType:      event.EventType,
		EventTime:      event.ClientTS,
		Body:           event.Body,
		CorrelationID:  event.CorrelationID,
		IdempotencyKey: event.IdempotencyKey,
		KafkaTopic:     "streamforge.events",
	}

	commitAttempts := 0
	commitCrashOnce := func() error {
		commitAttempts++
		if commitAttempts == 1 {
			return errors.New("simulated worker crash before offset commit")
		}
		return nil
	}

	// First delivery: DB write succeeds, crash happens before commit.
	if err := w.processDecodedEvent(ctx, event, record, commitCrashOnce); err == nil {
		t.Fatalf("expected first process attempt to fail due to commit crash")
	}

	// Redelivery: idempotency checker sees key, avoids duplicate DB write, commit succeeds.
	if err := w.processDecodedEvent(ctx, event, record, commitCrashOnce); err != nil {
		t.Fatalf("second process attempt failed: %v", err)
	}

	if memStore.eventInsertCount != 1 {
		t.Fatalf("expected single event insert, got %d", memStore.eventInsertCount)
	}
	if commitAttempts != 2 {
		t.Fatalf("expected 2 commit attempts, got %d", commitAttempts)
	}
}

type memoryStore struct {
	eventInsertCount int
}

func (m *memoryStore) InsertDLQEvent(context.Context, string, string, json.RawMessage, string, string) error {
	return nil
}
func (m *memoryStore) InsertEventWithOutbox(context.Context, store.EventRecord, string, json.RawMessage) error {
	m.eventInsertCount++
	return nil
}
func (m *memoryStore) FindUnsentOutboxRows(context.Context, int32) ([]store.OutboxRow, error) {
	return nil, nil
}
func (m *memoryStore) MarkOutboxSent(context.Context, int64) error {
	return nil
}

type memoryIdempotency struct {
	seen map[string]bool
}

func (m *memoryIdempotency) Seen(_ context.Context, tenantID, key string) (bool, error) {
	return m.seen[tenantID+":"+key], nil
}
func (m *memoryIdempotency) Mark(_ context.Context, tenantID, key string, _ time.Duration) error {
	m.seen[tenantID+":"+key] = true
	return nil
}
