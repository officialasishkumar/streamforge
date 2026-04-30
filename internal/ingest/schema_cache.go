package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/officialasishkumar/streamforge/internal/types"
	"github.com/xeipuuv/gojsonschema"
)

type SchemaProvider interface {
	ActiveSchemasForTenant(ctx context.Context, tenantID string) (map[string]json.RawMessage, error)
}

type SchemaCache struct {
	provider SchemaProvider
	ttl      time.Duration
	mu       sync.RWMutex
	entries  map[string]cacheEntry
}

type cacheEntry struct {
	schemas   map[string]json.RawMessage
	expiresAt time.Time
}

func NewSchemaCache(provider SchemaProvider, ttl time.Duration) *SchemaCache {
	return &SchemaCache{
		provider: provider,
		ttl:      ttl,
		entries:  make(map[string]cacheEntry),
	}
}

func (s *SchemaCache) Validate(ctx context.Context, tenantID string, event types.Event) error {
	schemas, err := s.getSchemas(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("schema cache: load schemas: %w", err)
	}
	schemaBody, ok := schemas[event.EventType]
	if !ok {
		return fmt.Errorf("no active schema for event_type=%s", event.EventType)
	}

	result, err := gojsonschema.Validate(
		gojsonschema.NewBytesLoader(schemaBody),
		gojsonschema.NewBytesLoader(event.Body),
	)
	if err != nil {
		return fmt.Errorf("schema validation engine failed: %w", err)
	}
	if result.Valid() {
		return nil
	}

	errs := make([]string, 0, len(result.Errors()))
	for _, e := range result.Errors() {
		errs = append(errs, e.String())
	}
	return fmt.Errorf("schema validation failed: %v", errs)
}

func (s *SchemaCache) getSchemas(ctx context.Context, tenantID string) (map[string]json.RawMessage, error) {
	s.mu.RLock()
	existing, ok := s.entries[tenantID]
	s.mu.RUnlock()

	now := time.Now()
	if ok && now.Before(existing.expiresAt) {
		return existing.schemas, nil
	}

	schemas, err := s.provider.ActiveSchemasForTenant(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("fetch active schemas: %w", err)
	}

	s.mu.Lock()
	s.entries[tenantID] = cacheEntry{
		schemas:   schemas,
		expiresAt: now.Add(s.ttl),
	}
	s.mu.Unlock()

	return schemas, nil
}
