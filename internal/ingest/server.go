package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Dependencies struct {
	SchemaCache *SchemaCache
	RateLimiter RateLimiter
	Producer    Producer
	Archiver    Archiver
	Readiness   ReadinessChecker
}

type Server struct {
	httpServer *http.Server
	log        *slog.Logger
}

func NewServer(cfg ServerConfig, deps Dependencies, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	h := NewHandler(cfg, deps, log)

	mux.HandleFunc("/healthz", h.HandleHealthz)
	mux.HandleFunc("/readyz", h.HandleReadyz)
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/v1/events", h.WithMiddleware(http.HandlerFunc(h.HandleEvents)))

	return &Server{
		httpServer: &http.Server{
			Addr:              fmt.Sprintf(":%d", cfg.Port),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       cfg.RequestTimeout,
			WriteTimeout:      cfg.RequestTimeout,
			IdleTimeout:       30 * time.Second,
		},
		log: log.With("component", "ingest_server"),
	}
}

func (s *Server) Start() error {
	s.log.Info("starting ingest server", "addr", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("ingest: listen and serve: %w", err)
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("ingest: shutdown: %w", err)
	}
	return nil
}
