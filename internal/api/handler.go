// Package api serves the hybrid search service over HTTP.
//
// It handles HTTP concerns only: reading parameters, choosing status codes,
// writing JSON, request IDs, access logs and panic recovery. Searching is the
// hybrid search service's job; this package never talks to OpenSearch,
// Qdrant, the embedding provider or PostgreSQL itself.
package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"near-real-time-hybrid-search-engine/internal/search/hybrid"
)

// Searcher runs a hybrid search. *hybrid.Service implements it.
type Searcher interface {
	Search(ctx context.Context, req hybrid.SearchRequest) ([]hybrid.Result, error)
}

var _ Searcher = (*hybrid.Service)(nil)

// SearchHandler serves GET /api/v1/search.
type SearchHandler struct {
	service Searcher
	timeout time.Duration
}

// NewSearchHandler returns a handler that searches with service. Each search
// is limited to timeout on top of the client's own connection; see
// config.SearchConfig.Timeout.
func NewSearchHandler(service Searcher, timeout time.Duration) *SearchHandler {
	return &SearchHandler{service: service, timeout: timeout}
}

// Search handles GET /api/v1/search?q=<query>&limit=<n>.
func (h *SearchHandler) Search(w http.ResponseWriter, r *http.Request) {
	req, err := parseSearchRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}

	// The request context ends when the client disconnects, which cancels
	// every call the search makes. The timeout bounds it on the server side.
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results, err := h.service.Search(ctx, req)
	if err != nil {
		writeSearchError(ctx, w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newSearchResponse(strings.TrimSpace(req.Query), results))
}

// writeSearchError chooses the response for a failed search. Only validation
// errors carry their own message; anything else gets fixed text, because
// dependency errors can hold hostnames, addresses or provider details. The
// search service has already logged the underlying error, with the request ID.
func writeSearchError(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) {
	var invalid *hybrid.ValidationError
	switch {
	case errors.As(err, &invalid):
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, invalid.Reason)
	case r.Context().Err() != nil:
		writeError(w, statusClientClosedRequest, CodeRequestCancelled, "request cancelled")
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, CodeTimeout, "search timed out")
	default:
		writeError(w, http.StatusInternalServerError, CodeInternal, "search failed; please try again later")
	}
}
