// Package api exposes the search service over HTTP.
package api

import (
	"log/slog"
	"net/http"

	"near-real-time-hybrid-search-engine/internal/search"
)

// Handler serves HTTP requests.
type Handler struct {
	search *search.Service
	logger *slog.Logger
}

// NewHandler returns a Handler that uses svc for search requests.
func NewHandler(svc *search.Service, logger *slog.Logger) *Handler {
	return &Handler{search: svc, logger: logger}
}

// Health reports that the process is running.
func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
