package indexing

import (
	"context"
	"errors"
	"fmt"

	"near-real-time-hybrid-search-engine/internal/embedding"
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
type Indexer struct {
	documents *postgres.Repository
	embedder  *embedding.Client
	keyword   *opensearch.Client
	vectors   *qdrant.Client
}

// NewIndexer returns an Indexer that writes keyword data through keyword and
// vectors through vectors.
func NewIndexer(
	documents *postgres.Repository,
	embedder *embedding.Client,
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
func (ix *Indexer) Index(ctx context.Context, ev Event) error {
	doc, err := ix.documents.GetDocument(ctx, ev.DocumentID)
	if errors.Is(err, postgres.ErrNotFound) {
		return ix.delete(ctx, ev.DocumentID)
	}
	if err != nil {
		return fmt.Errorf("load document: %w", err)
	}

	// Embed first so a Gemini failure leaves both stores untouched.
	vector, err := ix.embedder.EmbedDocument(ctx, doc.Title, doc.Body)
	if err != nil {
		var apiErr *embedding.APIError
		if errors.As(err, &apiErr) && !apiErr.Temporary() {
			err = permanent(err)
		}
		return fmt.Errorf("embed document: %w", err)
	}

	err = ix.keyword.IndexDocument(ctx, opensearch.Document{
		ID:        doc.ID,
		Title:     doc.Title,
		Content:   doc.Body,
		UpdatedAt: doc.UpdatedAt,
		Version:   doc.Version,
	})
	if err != nil {
		return fmt.Errorf("index in opensearch: %w", classifyKeyword(err))
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
