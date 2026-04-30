package types

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventJSONRoundTrip(t *testing.T) {
	t.Parallel()

	in := Event{
		ID:            "evt_1",
		TenantID:      "tenant-a",
		EventType:     "user.signup",
		Body:          json.RawMessage(`{"source":"web","version":1}`),
		ClientTS:      time.Date(2026, 5, 1, 0, 0, 0, 123000000, time.UTC),
		CorrelationID: "req-123",
		ReceivedAt:    time.Date(2026, 5, 1, 0, 0, 1, 0, time.UTC),
	}
	if err := in.EnsureIdempotencyKey(); err != nil {
		t.Fatalf("EnsureIdempotencyKey() error = %v", err)
	}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var out Event
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if out.TenantID != in.TenantID || out.EventType != in.EventType || out.IdempotencyKey != in.IdempotencyKey {
		t.Fatalf("round trip mismatch: got %+v want %+v", out, in)
	}
}

func BenchmarkEventJSONRoundTrip(b *testing.B) {
	ev := Event{
		ID:            "evt_bench",
		TenantID:      "tenant-bench",
		EventType:     "billing.charge_succeeded",
		Body:          json.RawMessage(`{"amount":4999,"currency":"USD","attempt":1}`),
		ClientTS:      time.Now().UTC(),
		CorrelationID: "req-bench",
	}
	if err := ev.EnsureIdempotencyKey(); err != nil {
		b.Fatalf("EnsureIdempotencyKey() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		raw, err := json.Marshal(ev)
		if err != nil {
			b.Fatalf("json.Marshal() error = %v", err)
		}
		var out Event
		if err := json.Unmarshal(raw, &out); err != nil {
			b.Fatalf("json.Unmarshal() error = %v", err)
		}
	}
}
