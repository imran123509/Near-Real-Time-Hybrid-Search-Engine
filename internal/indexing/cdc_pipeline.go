package indexing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
)

// This file joins the change-data-capture path to the machinery this package
// already provides, so that path does not grow its own copy of it:
//
//	kafka.Message -> WorkerPool -> Pipeline -> cdc.ParseChangeEvent
//	                                        -> cdc.ChangeEvent
//	                                        -> cdc.Service -> OpenSearch, Qdrant
//
// The cdc package knows nothing about Kafka; everything Kafka-shaped stops
// here.

// NewCDCPipeline returns a Pipeline that indexes Debezium change events with
// service, retrying and dead-lettering by the same rules as the application's
// own events. Give its Process method to a WorkerPool as the handler.
func NewCDCPipeline(
	service *cdc.Service,
	deadLetters DeadLetterPublisher,
	maxAttempts int,
	logger *slog.Logger,
) *Pipeline[cdc.ChangeEvent] {
	return NewPipeline(decodeChangeEvent, changeEventIndexer{service: service}, deadLetters, maxAttempts, logger)
}

// decodeChangeEvent parses a Debezium message value.
//
// A tombstone is turned into ErrSkipMessage rather than an error: Debezium
// writes one after every delete so log compaction can drop the key, it always
// follows the real delete event, and dead-lettering it would fill the
// dead-letter topic with normal traffic.
func decodeChangeEvent(data []byte) (cdc.ChangeEvent, error) {
	ev, err := cdc.ParseChangeEvent(data)
	if errors.Is(err, cdc.ErrTombstone) {
		return ev, fmt.Errorf("%w: %w", ErrSkipMessage, err)
	}
	return ev, err
}

// changeEventIndexer adapts cdc.Service to the pipeline's EventIndexer and
// works out which of its failures are worth retrying.
type changeEventIndexer struct {
	service *cdc.Service
}

func (i changeEventIndexer) Index(ctx context.Context, ev cdc.ChangeEvent) error {
	return classifyChange(i.service.Process(ctx, ev))
}

// classifyChange marks the failures of a change event that retrying cannot fix
// as permanent, so the pipeline dead-letters them instead of retrying.
//
// A delete asks both stores to delete and reports both failures together, so
// the event is only permanent when retrying would help neither of them.
func classifyChange(err error) error {
	if err == nil || isPermanent(err) {
		return err
	}
	// A bad event, an unknown operation, a missing key or a row the mapping
	// cannot read will all fail again in exactly the same way.
	if errors.Is(err, cdc.ErrMalformedEvent) {
		return permanent(err)
	}
	for _, failure := range flattenErrors(err) {
		if !isPermanent(classifyStore(failure)) {
			return err
		}
	}
	return permanent(err)
}

// classifyStore applies the rules of whichever store reported the failure.
func classifyStore(err error) error {
	var storeErr *cdc.StoreError
	if !errors.As(err, &storeErr) {
		return err
	}
	switch storeErr.Store {
	case cdc.StoreKeyword:
		return classifyKeyword(err)
	case cdc.StoreVector:
		return classifyVector(err)
	case cdc.StoreEmbedder:
		return classifyEmbedding(err)
	default:
		return err
	}
}

// flattenErrors returns the individual failures inside an errors.Join, and the
// error itself when it is not a join.
func flattenErrors(err error) []error {
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return []error{err}
	}
	var out []error
	for _, e := range joined.Unwrap() {
		out = append(out, flattenErrors(e)...)
	}
	return out
}
