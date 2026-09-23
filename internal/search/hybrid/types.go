package hybrid

import "errors"

// MaxQueryLength is the longest query accepted, in characters. It stops a
// pasted document from turning into an expensive embedding request or a
// keyword query too large for OpenSearch to run.
const MaxQueryLength = 1024

// ErrInvalidRequest means the request was rejected before any retriever was
// called. Every such error is a *ValidationError.
var ErrInvalidRequest = errors.New("invalid search request")

// ValidationError explains why a request was rejected. Reason never contains
// the query text, so it is safe to show to the caller.
type ValidationError struct {
	Reason string
}

func (e *ValidationError) Error() string { return ErrInvalidRequest.Error() + ": " + e.Reason }

// Is makes errors.Is(err, ErrInvalidRequest) true.
func (e *ValidationError) Is(target error) bool { return target == ErrInvalidRequest }

// SearchRequest is one hybrid search.
type SearchRequest struct {
	Query string
	// Limit is the number of results wanted. Zero means the configured
	// default; anything above the configured maximum is rejected.
	Limit int
}

// Result is one hybrid search hit. It does not depend on any retriever's
// types, so the API layer can serve it as it is.
type Result struct {
	ID    string
	Title string
	// Content is the document body as indexed in OpenSearch. It is empty for a
	// document that only vector search found, because the vector index stores
	// no body, only a title and URL beside each vector.
	Content string
	URL     string
	// Score is the fused RRF score. It orders results within one response and
	// means nothing across responses; it is not a relevance percentage.
	Score float64
}
