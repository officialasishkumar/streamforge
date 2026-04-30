package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/officialasishkumar/streamforge/internal/store"
)

func TestProcessMalformedMessageDLQsAndCommitsOffset(t *testing.T) {
	ctx := context.Background()
	st := &testStore{}
	w := &Worker{store: st, log: slog.Default()}

	commits := 0
	err := w.processMalformedMessage(ctx, []byte(`{"tenant_id":`), func() error {
		commits++
		return nil
	}, errors.New("unexpected end of JSON input"))
	if err != nil {
		t.Fatalf("process malformed message: %v", err)
	}

	if commits != 1 {
		t.Fatalf("expected one offset commit, got %d", commits)
	}
	if len(st.dlqRows) != 1 {
		t.Fatalf("expected one dlq row, got %d", len(st.dlqRows))
	}
	if st.dlqRows[0].reason != "invalid_json" {
		t.Fatalf("expected invalid_json reason, got %q", st.dlqRows[0].reason)
	}
}

func TestProcessMalformedMessageReturnsCommitErrorAfterDLQ(t *testing.T) {
	ctx := context.Background()
	st := &testStore{}
	w := &Worker{store: st, log: slog.Default()}

	err := w.processMalformedMessage(ctx, []byte(`not-json`), func() error {
		return errors.New("commit unavailable")
	}, errors.New("invalid character"))
	if err == nil {
		t.Fatal("expected commit error")
	}
	if len(st.dlqRows) != 1 {
		t.Fatalf("expected one dlq row before commit failure, got %d", len(st.dlqRows))
	}
}

type testStore struct {
	dlqRows []testDLQRow
}

type testDLQRow struct {
	tenantID      string
	eventType     string
	body          json.RawMessage
	reason        string
	correlationID string
}

func (t *testStore) InsertDLQEvent(_ context.Context, tenantID, eventType string, body json.RawMessage, reason, correlationID string) error {
	t.dlqRows = append(t.dlqRows, testDLQRow{
		tenantID:      tenantID,
		eventType:     eventType,
		body:          body,
		reason:        reason,
		correlationID: correlationID,
	})
	return nil
}

func (t *testStore) InsertEventWithOutbox(context.Context, store.EventRecord, string, json.RawMessage) error {
	return nil
}

func (t *testStore) FindUnsentOutboxRows(context.Context, int32) ([]store.OutboxRow, error) {
	return nil, nil
}

func (t *testStore) MarkOutboxSent(context.Context, int64) error {
	return nil
}
