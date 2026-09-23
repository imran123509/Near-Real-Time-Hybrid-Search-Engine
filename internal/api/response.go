package api

import (
	"encoding/json"
	"net/http"

	"near-real-time-hybrid-search-engine/internal/search/hybrid"
)

// Error codes. They are part of the API contract, since clients may branch on
// them, so they only change with a new API version.
const (
	CodeInvalidRequest   = "INVALID_REQUEST"
	CodeNotFound         = "NOT_FOUND"
	CodeMethodNotAllowed = "METHOD_NOT_ALLOWED"
	CodeRequestCancelled = "REQUEST_CANCELLED"
	CodeTimeout          = "TIMEOUT"
	CodeInternal         = "INTERNAL_ERROR"
)

// statusClientClosedRequest is the non-standard status, from nginx, for a
// request the client abandoned before the answer was ready. The client never
// sees it; it keeps those requests apart from server failures in the logs.
const statusClientClosedRequest = 499

// SearchResponse is the body of a successful search.
//
// It is the API's own type rather than hybrid.Result with JSON tags, so the
// wire format stays fixed however the internal model changes.
type SearchResponse struct {
	Query   string         `json:"query"`
	Results []SearchResult `json:"results"`
	// Total is the number of results in this response, not the number of
	// matching documents in the index.
	Total int `json:"total"`
}

// SearchResult is one search hit.
type SearchResult struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	// Content is empty for a document that only vector search found.
	Content string `json:"content"`
	URL     string `json:"url"`
	// Score is the fused ranking score. It orders results within one
	// response and is not comparable across responses.
	Score float64 `json:"score"`
}

// ErrorResponse is the body of every error response.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody says what went wrong without exposing internal details.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// statusResponse is the body of the health and readiness endpoints.
type statusResponse struct {
	Status string `json:"status"`
}

func newSearchResponse(query string, results []hybrid.Result) SearchResponse {
	out := make([]SearchResult, len(results)) // never nil: an empty result list is [] in JSON
	for i, r := range results {
		out[i] = SearchResult{ID: r.ID, Title: r.Title, Content: r.Content, URL: r.URL, Score: r.Score}
	}
	return SearchResponse{Query: query, Results: out, Total: len(out)}
}

// internalErrorBody is written when a response cannot be encoded.
var internalErrorBody = []byte(`{"error":{"code":"INTERNAL_ERROR","message":"internal error"}}` + "\n")

// writeJSON writes v as the response body. It encodes v before sending
// anything, so an encoding failure becomes a clean 500 rather than a
// truncated body after a 200.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		status, body = http.StatusInternalServerError, internalErrorBody
	} else {
		body = append(body, '\n')
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// writeError writes an ErrorResponse. message must be safe for clients: no
// internal error text, hostnames or credentials.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorResponse{Error: ErrorBody{Code: code, Message: message}})
}
