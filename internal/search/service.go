// Package search runs hybrid keyword and vector search.
package search

import (
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/qdrant/go-client/qdrant"

	"near-real-time-hybrid-search-engine/internal/postgres"
)

// Service combines OpenSearch keyword results and Qdrant vector results.
// It does not own its clients; the caller closes them.
type Service struct {
	opensearch *opensearchapi.Client
	qdrant     *qdrant.Client
	documents  *postgres.Repository
}

// NewService returns a Service that uses the given dependencies.
func NewService(osClient *opensearchapi.Client, qdClient *qdrant.Client, docs *postgres.Repository) *Service {
	return &Service{opensearch: osClient, qdrant: qdClient, documents: docs}
}
