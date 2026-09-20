package cdc

import (
	"context"
	"errors"
	"fmt"

	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
)

// ErrNoEmbeddingText means a row has neither title nor content, so there is
// nothing to embed and nothing worth retrieving. It wraps ErrMalformedEvent.
var ErrNoEmbeddingText = fmt.Errorf("%w: document has no text to embed", ErrMalformedEvent)

// Embedder turns text into a vector.
//
// The service depends on this interface rather than on any particular
// provider, so the provider can be chosen, swapped or faked without the
// service knowing. What text reaches it is decided by BuildEmbeddingText, not
// by the provider.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// EmbedderFunc adapts a function to Embedder, which is how an existing client
// with a different method name is plugged in.
type EmbedderFunc func(ctx context.Context, text string) ([]float32, error)

// Embed calls f.
func (f EmbedderFunc) Embed(ctx context.Context, text string) ([]float32, error) {
	return f(ctx, text)
}

// KeywordIndex is the part of the OpenSearch client the service uses.
// *opensearch.Client satisfies it.
type KeywordIndex interface {
	IndexDocument(ctx context.Context, doc opensearch.Document) error
	DeleteDocument(ctx context.Context, id string) error
}

// VectorIndex is the part of the Qdrant client the service uses.
// *qdrant.Client satisfies it.
type VectorIndex interface {
	Upsert(ctx context.Context, p qdrant.Point) error
	Delete(ctx context.Context, id string) error
}

// Service applies change events to the two search indexes. It does not own its
// dependencies and is safe for concurrent use, so one instance is shared by
// every worker.
//
// # Idempotency
//
// Processing the same event twice leaves exactly one OpenSearch document and
// one Qdrant point, because nothing about a write depends on how often it
// happens:
//
//   - The document ID comes from the row's primary key, so it is the same on
//     every replay. It is the OpenSearch _id, and Qdrant derives its point ID
//     from it, so a second write replaces the first instead of adding a copy.
//   - The mapped version column is sent to OpenSearch as an external version,
//     so a replayed or late event with an older or equal version is ignored
//     rather than overwriting newer data.
//   - Deleting a document that is already gone succeeds in both stores.
//
// This does not rely on Kafka delivering each message once, which it does not
// guarantee: after a crash or a rebalance, messages past the last committed
// offset are delivered again, and re-applying them is harmless.
//
// # Partial failure
//
// OpenSearch and Qdrant are separate systems with no shared transaction, and
// none is simulated here. A two-phase commit across them would need both to
// support prepared writes and would make every index write wait on the slower
// of the two, while still leaving failure windows of its own. Instead the
// indexes are eventually consistent: a failed event is reported rather than
// swallowed, and re-processing it converges both stores, which is safe
// precisely because the writes are idempotent.
//
// An upsert writes OpenSearch first and Qdrant second, and stops at the first
// failure, so only one partial state is reachable: the keyword index holds the
// new document while the vector index still holds the old one, until the event
// succeeds on a later attempt. Until then the document is findable by keyword
// but ranks on a stale vector. A failure before OpenSearch, in the embedder or
// the mapping, leaves both stores untouched.
//
// A delete is the other way round: both stores are asked to delete even if the
// first one fails, and the failures are reported together. Leaving a document
// in one index is worse than a wasted call, so neither delete is skipped.
//
// # Retry boundary
//
// Process tries each write once and returns the error. Deciding whether to
// retry, how long to back off and when to dead-letter belongs to the caller,
// so that the policy lives in one place instead of being spread through the
// pipeline.
//
// # Metrics
//
// Every failure is a *StoreError naming the store that failed, so a metrics
// exporter can count embedder, OpenSearch and Qdrant failures separately with
// errors.As, without this package depending on a metrics library.
type Service struct {
	keyword  KeywordIndex
	vectors  VectorIndex
	embedder Embedder
	mapping  FieldMapping
}

// NewService returns a Service that writes documents described by mapping.
// Use DefaultMapping for the documents table.
func NewService(keyword KeywordIndex, vectors VectorIndex, embedder Embedder, mapping FieldMapping) (*Service, error) {
	switch {
	case keyword == nil:
		return nil, errors.New("keyword index is required")
	case vectors == nil:
		return nil, errors.New("vector index is required")
	case embedder == nil:
		return nil, errors.New("embedder is required")
	case mapping.Title == "" && mapping.Content == "":
		return nil, errors.New("mapping must name a title or content column, or no document would have any text")
	}
	return &Service{keyword: keyword, vectors: vectors, embedder: embedder, mapping: mapping}, nil
}

// Process applies one change event to both search indexes.
//
// Creates, updates and snapshot reads all write the current row; only the
// operation recorded in logs and metrics tells them apart. Deletes remove the
// document from both stores.
func (s *Service) Process(ctx context.Context, ev ChangeEvent) error {
	if err := ev.Validate(); err != nil {
		return err
	}
	if ev.Operation == OperationDelete {
		return s.delete(ctx, ev.ID)
	}
	return s.upsert(ctx, ev)
}

// upsert writes the current state of a row to both indexes.
func (s *Service) upsert(ctx context.Context, ev ChangeEvent) error {
	doc, err := s.mapping.Document(ev)
	if err != nil {
		return err
	}

	text := BuildEmbeddingText(doc)
	if text == "" {
		return fmt.Errorf("%w: %s", ErrNoEmbeddingText, ev.ID)
	}

	// Embed before writing anything, so that an embedder failure leaves both
	// stores as they were instead of updating one of them.
	vector, err := s.embedder.Embed(ctx, text)
	if err != nil {
		return storeErr(StoreEmbedder, ev.ID, err)
	}

	if err := s.keyword.IndexDocument(ctx, doc); err != nil {
		return storeErr(StoreKeyword, ev.ID, err)
	}

	err = s.vectors.Upsert(ctx, qdrant.Point{
		ID:      doc.ID,
		Vector:  vector,
		Payload: s.mapping.VectorPayload(doc),
	})
	if err != nil {
		// OpenSearch already holds the new document. The stores stay out of
		// step until this event is processed again, which is safe because
		// both writes are idempotent.
		return storeErr(StoreVector, ev.ID, err)
	}
	return nil
}

// delete removes a document from both indexes, asking both even when the
// first fails so that a document is never left behind in one of them.
func (s *Service) delete(ctx context.Context, id string) error {
	return errors.Join(
		storeErr(StoreKeyword, id, s.keyword.DeleteDocument(ctx, id)),
		storeErr(StoreVector, id, s.vectors.Delete(ctx, id)),
	)
}
