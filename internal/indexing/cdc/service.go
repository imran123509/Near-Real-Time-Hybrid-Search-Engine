package cdc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
)

// ErrNoEmbeddingText means a row has neither title nor content, so there is
// nothing to embed and nothing worth retrieving. It wraps ErrMalformedEvent.
var ErrNoEmbeddingText = fmt.Errorf("%w: document has no text to embed", ErrMalformedEvent)

// Embedder turns text into a vector.
//
// It is the one method this package needs from an embedding provider, kept
// here beside the code that uses it. Every embedding.Provider satisfies it, so
// the provider can be chosen, swapped or faked without this package importing
// any of them. What text reaches it is decided by BuildEmbeddingText, not by
// the provider.
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
//
// IndexDocument reports what its write did, which is what lets the service
// tell a redelivered event from one the index has already moved past.
type KeywordIndex interface {
	IndexDocument(ctx context.Context, doc opensearch.Document) (opensearch.WriteResult, error)
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
// No record of processed events is kept anywhere, and none is needed: every
// write already says what the final state should be, so applying it twice and
// applying it once leave the same state. An event store would add a database
// to keep consistent with the two indexes, and would still not make the pair
// of writes atomic.
//
// # Stale events
//
// Events for one row share a Kafka key and therefore a partition, so the
// consumer applies them in the order the database made them. Where that order
// can still break -- a rebalance leaving two consumers briefly overlapping, a
// dead-lettered event, a manual replay -- the version column decides instead:
// OpenSearch refuses a write that is not newer than what it holds, and this
// service stops there rather than sending the older row on to the embedder and
// Qdrant, which have no version check of their own. What it reports then is
// EffectStale.
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
// An upsert runs in this order and stops at the first failure:
//
//	map the row -> OpenSearch -> embed -> Qdrant
//
// Keyword indexing comes first so that the document is searchable by keyword
// as soon as possible and does not wait on the embedding provider, the
// slowest and least reliable step. The cost is that only one partial state is
// reachable, whichever later step fails: the keyword index holds the new
// document while the vector index still holds the old vector, or none. Until
// the event succeeds on a later attempt, the document is findable by keyword
// but ranks on a stale vector. A row that cannot be mapped, or has no text,
// leaves both stores untouched.
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
	observe  Observer
}

// StoreCall is one call to one downstream system: how long it took and
// whether it worked. It carries no document ID and no payload.
type StoreCall struct {
	Store Store
	// Operation is what was asked of it: index, delete, upsert or embed. The
	// set is fixed, so it is safe as a metric label.
	Operation string
	Duration  time.Duration
	Err       error
}

// EventApplied is one change event after the service has finished with it.
type EventApplied struct {
	Operation Operation
	Effect    Effect
	Duration  time.Duration
	// Store names the system that failed, when one did.
	Store Store
	Err   error
}

// Observer is told what the service did. It exists so that metrics can be
// attached without this package knowing anything about a metrics library, the
// same way the pipeline above it reports its outcomes.
//
// Both methods run on the worker that processed the event, so neither may
// block.
type Observer interface {
	StoreCalled(call StoreCall)
	EventApplied(event EventApplied)
}

// Observe registers o, replacing any previous observer. A nil observer turns
// observation off.
func (s *Service) Observe(o Observer) { s.observe = o }

// storeCalled reports one downstream call, if anyone is listening.
func (s *Service) storeCalled(store Store, operation string, started time.Time, err error) {
	if s.observe == nil {
		return
	}
	s.observe.StoreCalled(StoreCall{Store: store, Operation: operation, Duration: time.Since(started), Err: err})
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

// Process applies one change event to both search indexes and reports what it
// did, which is EffectNone whenever it returns an error.
//
// Creates, updates and snapshot reads all write the current row; only the
// operation recorded in logs and metrics tells them apart. Deletes remove the
// document from both stores.
func (s *Service) Process(ctx context.Context, ev ChangeEvent) (Effect, error) {
	started := time.Now()
	effect, err := s.process(ctx, ev)

	if s.observe != nil {
		applied := EventApplied{Operation: ev.Operation, Effect: effect, Duration: time.Since(started), Err: err}
		// Which store failed is the difference between a rejected event and a
		// half-applied one, so it is reported rather than only logged.
		var storeErr *StoreError
		if errors.As(err, &storeErr) {
			applied.Store = storeErr.Store
		}
		s.observe.EventApplied(applied)
	}
	return effect, err
}

func (s *Service) process(ctx context.Context, ev ChangeEvent) (Effect, error) {
	if err := ev.Validate(); err != nil {
		return EffectNone, err
	}
	// Starting writes that cannot finish would only widen the gap between
	// the two stores.
	if err := ctx.Err(); err != nil {
		return EffectNone, err
	}
	if ev.Operation == OperationDelete {
		return s.delete(ctx, ev.ID)
	}
	return s.upsert(ctx, ev)
}

// upsert writes the current state of a row to both indexes.
func (s *Service) upsert(ctx context.Context, ev ChangeEvent) (Effect, error) {
	doc, err := s.mapping.Document(ev)
	if err != nil {
		return EffectNone, err
	}

	// Checked before any write: a document with no text is not worth
	// keyword-indexing either.
	text := BuildEmbeddingText(doc)
	if text == "" {
		return EffectNone, fmt.Errorf("%w: %s", ErrNoEmbeddingText, ev.ID)
	}

	started := time.Now()
	written, err := s.keyword.IndexDocument(ctx, doc)
	s.storeCalled(StoreKeyword, "index", started, err)
	if err != nil {
		return EffectNone, storeErr(StoreKeyword, ev.ID, err)
	}
	if written == opensearch.WriteStale {
		// A later event for this row has already been applied. Qdrant has no
		// version check of its own, so embedding this row now and upserting
		// it would replace a newer vector with an older one -- the one thing
		// the keyword index refuses to do. Stopping here is what extends that
		// protection to the vector index, and it costs nothing: there is
		// nothing left in this event that is not already superseded.
		return EffectStale, nil
	}

	// From here on OpenSearch already holds the new document, so any failure
	// leaves the vector index behind until this event is processed again,
	// which is safe because every write is idempotent.
	started = time.Now()
	vector, err := s.embedder.Embed(ctx, text)
	s.storeCalled(StoreEmbedder, "embed", started, err)
	if err != nil {
		return EffectNone, storeErr(StoreEmbedder, ev.ID, err)
	}

	started = time.Now()
	err = s.vectors.Upsert(ctx, qdrant.Point{
		ID:      doc.ID,
		Vector:  vector,
		Payload: s.mapping.VectorPayload(doc),
	})
	s.storeCalled(StoreVector, "upsert", started, err)
	if err != nil {
		return EffectNone, storeErr(StoreVector, ev.ID, err)
	}
	if written == opensearch.WriteDuplicate {
		return EffectReapplied, nil
	}
	return EffectIndexed, nil
}

// delete removes a document from both indexes, asking both even when the
// first fails so that a document is never left behind in one of them.
func (s *Service) delete(ctx context.Context, id string) (Effect, error) {
	keywordStart := time.Now()
	keywordErr := s.keyword.DeleteDocument(ctx, id)
	s.storeCalled(StoreKeyword, "delete", keywordStart, keywordErr)

	vectorStart := time.Now()
	vectorErr := s.vectors.Delete(ctx, id)
	s.storeCalled(StoreVector, "delete", vectorStart, vectorErr)

	err := errors.Join(
		storeErr(StoreKeyword, id, keywordErr),
		storeErr(StoreVector, id, vectorErr),
	)
	if err != nil {
		return EffectNone, err
	}
	// Both stores treat deleting something that is not there as success, so a
	// repeated delete ends here too, with the same final state.
	return EffectDeleted, nil
}
