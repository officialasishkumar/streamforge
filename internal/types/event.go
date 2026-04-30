package types

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type Event struct {
	ID             string          `json:"id,omitempty"`
	TenantID       string          `json:"tenant_id"`
	EventType      string          `json:"event_type"`
	Body           json.RawMessage `json:"body"`
	ClientTS       time.Time       `json:"client_timestamp"`
	CorrelationID  string          `json:"correlation_id,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	ReceivedAt     time.Time       `json:"received_at,omitempty"`
}

func (e *Event) EnsureIdempotencyKey() error {
	if e.IdempotencyKey != "" {
		return nil
	}
	if e.TenantID == "" {
		return fmt.Errorf("event: missing tenant_id for idempotency key generation")
	}
	if e.EventType == "" {
		return fmt.Errorf("event: missing event_type for idempotency key generation")
	}
	if len(e.Body) == 0 {
		return fmt.Errorf("event: missing body for idempotency key generation")
	}
	if e.ClientTS.IsZero() {
		return fmt.Errorf("event: missing client_timestamp for idempotency key generation")
	}

	sum := sha256.Sum256([]byte(fmt.Sprintf(
		"%s|%s|%s|%s",
		e.TenantID,
		e.EventType,
		string(e.Body),
		e.ClientTS.UTC().Format(time.RFC3339Nano),
	)))
	e.IdempotencyKey = hex.EncodeToString(sum[:])
	return nil
}

func (e Event) MarshalJSON() ([]byte, error) {
	type Alias Event
	return json.Marshal(Alias(e))
}

func (e *Event) UnmarshalJSON(data []byte) error {
	type Alias Event
	var a Alias
	if err := json.Unmarshal(data, &a); err != nil {
		return fmt.Errorf("event: unmarshal json: %w", err)
	}
	*e = Event(a)
	return nil
}
