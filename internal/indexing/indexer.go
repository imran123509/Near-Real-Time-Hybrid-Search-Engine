package indexing

import (
	"context"
	"errors"
	"fmt"

	"near-real-time-hybrid-search-engine/internal/embedding"
	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/postgres"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
)

// Indexer writes the current state of a document to OpenSearch and Qdrant.
// It does not own its clients. It is safe for concurrent use.
//
// Idempotency:
//   - The document is always read from PostgreSQL, so replayed or out-of-order
//     events index the latest data rather than the data at event time.
//   - OpenSearch writes carry the row version, so an older or equal version
//     is ignored instead of overwriting newer data.
//   - Qdrant point IDs are derived from the document ID, so an upsert replaces
//     the previous point instead of adding a duplicate.
//   - A document missing from PostgreSQL is deleted from both stores, and
//     deleting something already gone succeeds.
//
// Embeddings come from any cdc.Embedder, which every embedding.Provider
// satisfies, so the indexer neither knows nor cares which provider is behind
// it. What text is embedded is decided by cdc.BuildEmbeddingText, the same rule
// the change-data-capture path uses.
type Indexer struct {
	documents *postgres.Repository
	embedder  cdc.Embedder
	keyword   *opensearch.Client
	vectors   *qdrant.Client
}

// Every embedding provider can be handed to the indexers as it is.
var _ cdc.Embedder = embedding.Provider(nil)

// NewIndexer returns an Indexer that writes keyword data through keyword,
// embeds through embedder and writes vectors through vectors.
func NewIndexer(
	documents *postgres.Repository,
	embedder cdc.Embedder,
	keyword *opensearch.Client,
	vectors *qdrant.Client,
) *Indexer {
	return &Indexer{
		documents: documents,
		embedder:  embedder,
		keyword:   keyword,
		vectors:   vectors,
	}
}

// Index brings both stores in line with the document named by ev.
// Errors that retrying cannot fix are marked permanent.
//
// The order is OpenSearch, then the embedding, then Qdrant, and the first
// failure stops it, for the reasons given on cdc.Service: keyword search stays
// current even while the embedding provider is slow or down, and the only
// partial state is the keyword index being ahead of the vector index until
// the event is retried.
func (ix *Indexer) Index(ctx context.Context, ev Event) error {
	row, err := ix.documents.GetDocument(ctx, ev.DocumentID)
	if errors.Is(err, postgres.ErrNotFound) {
		return ix.delete(ctx, ev.DocumentID)
	}
	if err != nil {
		return fmt.Errorf("load document: %w", err)
	}

	doc := opensearch.Document{
		ID:        row.ID,
		Title:     row.Title,
		Content:   row.Body,
		UpdatedAt: row.UpdatedAt,
		Version:   row.Version,
	}
	text := cdc.BuildEmbeddingText(doc)
	if text == "" {
		return permanent(fmt.Errorf("document %s: %w", doc.ID, embedding.ErrEmptyText))
	}

	if err := ix.keyword.IndexDocument(ctx, doc); err != nil {
		return fmt.Errorf("index in opensearch: %w", classifyKeyword(err))
	}

	vector, err := ix.embedder.Embed(ctx, text)
	if err != nil {
		return fmt.Errorf("embed document: %w", classifyEmbedding(err))
	}

	err = ix.vectors.Upsert(ctx, qdrant.Point{
		ID:      doc.ID,
		Vector:  vector,
		Payload: map[string]any{"title": doc.Title, "version": doc.Version},
	})
	if err != nil {
		return fmt.Errorf("index in qdrant: %w", classifyVector(err))
	}
	return nil
}

func (ix *Indexer) delete(ctx context.Context, id string) error {
	if err := ix.keyword.DeleteDocument(ctx, id); err != nil {
		return fmt.Errorf("delete from opensearch: %w", classifyKeyword(err))
	}
	if err := ix.vectors.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete from qdrant: %w", classifyVector(err))
	}
	return nil
}

// EnsureStores creates the OpenSearch index and Qdrant collection if they do
// not exist. Call it once at startup.
func (ix *Indexer) EnsureStores(ctx context.Context) error {
	if err := ix.keyword.EnsureIndex(ctx); err != nil {
		return fmt.Errorf("ensure opensearch index: %w", err)
	}
	if err := ix.vectors.EnsureCollection(ctx); err != nil {
		return fmt.Errorf("ensure qdrant collection: %w", err)
	}
	return nil
}

// classifyEmbedding marks embedding failures that retrying cannot fix as
// permanent: text the provider will never accept, a vector of the wrong
// dimension, which means the model and the configuration disagree, and a
// request the provider refused outright. Rate limits, timeouts, 5xx and
// transport failures stay retryable.
func classifyEmbedding(err error) error {
	if errors.Is(err, embedding.ErrEmptyText) || errors.Is(err, embedding.ErrDimensionMismatch) {
		return permanent(err)
	}
	var apiErr *embedding.APIError
	if errors.As(err, &apiErr) && !apiErr.Temporary() {
		return permanent(err)
	}
	return err
}

// classifyKeyword marks OpenSearch failures that retrying cannot fix as
// permanent: a document rejected before sending, or a request OpenSearch
// refused. Network errors, timeouts, rate limits and 5xx stay retryable.
func classifyKeyword(err error) error {
	var reqErr *opensearch.RequestError
	if errors.Is(err, opensearch.ErrInvalidDocument) || (errors.As(err, &reqErr) && !reqErr.Temporary()) {
		return permanent(err)
	}
	return err
}

// classifyVector does the same for Qdrant: an invalid point or vector, or a
// request Qdrant rejected, is permanent; unavailability and timeouts are not.
func classifyVector(err error) error {
	var reqErr *qdrant.RequestError
	if errors.Is(err, qdrant.ErrInvalidPoint) || errors.Is(err, qdrant.ErrInvalidVector) ||
		(errors.As(err, &reqErr) && !reqErr.Temporary()) {
		return permanent(err)
	}
	return err
}
