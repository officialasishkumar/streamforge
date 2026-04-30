package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/officialasishkumar/streamforge/internal/types"
)

type ServerConfig struct {
	Port           int
	MaxBatchSize   int
	RequestTimeout time.Duration
}

type Handler struct {
	cfg  ServerConfig
	deps Dependencies
	log  *slog.Logger
}

type eventBatchRequest struct {
	TenantID string        `json:"tenant_id"`
	Events   []types.Event `json:"events"`
}

func NewHandler(cfg ServerConfig, deps Dependencies, log *slog.Logger) *Handler {
	return &Handler{
		cfg:  cfg,
		deps: deps,
		log:  log.With("component", "ingest_handler"),
	}
}

func (h *Handler) HandleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (h *Handler) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if h.deps.Readiness == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
		return
	}
	if err := h.deps.Readiness.Ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func (h *Handler) HandleEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.RequestTimeout)
	defer cancel()

	var req eventBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_json", "details": err.Error()})
		return
	}
	if req.TenantID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "tenant_id_required"})
		return
	}
	if len(req.Events) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "events_required"})
		return
	}
	if len(req.Events) > h.cfg.MaxBatchSize {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "batch_too_large", "max_batch_size": h.cfg.MaxBatchSize})
		return
	}

	allowed, retryAfter, err := h.deps.RateLimiter.Allow(ctx, req.TenantID, len(req.Events))
	if err != nil {
		writeJSONWithHeaders(w, http.StatusServiceUnavailable, map[string]string{"Retry-After": retryAfter.String()}, map[string]any{"error": "rate_limit_dependency_unavailable"})
		return
	}
	if !allowed {
		writeJSONWithHeaders(w, http.StatusTooManyRequests, map[string]string{"Retry-After": retryAfter.String()}, map[string]any{"error": "rate_limited"})
		return
	}

	for i := range req.Events {
		req.Events[i].TenantID = req.TenantID
		if err := req.Events[i].EnsureIdempotencyKey(); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "idempotency_key_generation_failed", "details": err.Error(), "event_index": i})
			return
		}
		if err := h.deps.SchemaCache.Validate(ctx, req.TenantID, req.Events[i]); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "schema_validation_failed", "details": err.Error(), "event_index": i})
			return
		}
	}

	correlationID := requestIDFromContext(ctx)
	archiveKey, err := h.deps.Archiver.ArchiveBatch(ctx, req.TenantID, correlationID, req.Events)
	if err != nil {
		writeJSONWithHeaders(w, http.StatusServiceUnavailable, map[string]string{"Retry-After": "2"}, map[string]any{"error": "archive_failed"})
		return
	}

	if err := h.deps.Producer.PublishBatch(ctx, req.TenantID, correlationID, archiveKey, req.Events); err != nil {
		writeJSONWithHeaders(w, http.StatusServiceUnavailable, map[string]string{"Retry-After": "2"}, map[string]any{"error": "kafka_publish_failed"})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":         "accepted",
		"events":         len(req.Events),
		"archive_object": archiveKey,
	})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	writeJSONWithHeaders(w, code, nil, payload)
}

func writeJSONWithHeaders(w http.ResponseWriter, code int, headers map[string]string, payload any) {
	w.Header().Set("Content-Type", "application/json")
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error(fmt.Sprintf("ingest: encode response: %v", err))
	}
}
