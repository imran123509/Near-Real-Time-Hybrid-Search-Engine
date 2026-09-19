// Package search runs hybrid keyword and vector search.
package search

import (
	"near-real-time-hybrid-search-engine/internal/postgres"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
)

// Service combines OpenSearch keyword results and Qdrant vector results.
// It does not own its clients; the caller closes them.
type Service struct {
	keyword   *opensearch.Client
	vectors   *qdrant.Client
	documents *postgres.Repository
}

// NewService returns a Service that uses the given dependencies.
func NewService(keyword *opensearch.Client, vectors *qdrant.Client, docs *postgres.Repository) *Service {
	return &Service{keyword: keyword, vectors: vectors, documents: docs}
}
