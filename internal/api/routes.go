package api

import "net/http"

// NewRouter registers the API routes on a new mux.
func NewRouter(h *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.Health)
	return mux
}
