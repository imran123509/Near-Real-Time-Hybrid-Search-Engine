// Package indexing keeps OpenSearch and Qdrant in sync with the documents
// table by processing document events from Kafka.
package indexing

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ErrMalformedEvent means a message can never be processed as sent.
var ErrMalformedEvent = errors.New("malformed event")

// Operation is the change that produced an event.
type Operation string

const (
	OperationUpsert Operation = "upsert"
	OperationDelete Operation = "delete"
)

// Event announces that a document changed. It carries identifiers only; the
// indexer reads the current document from PostgreSQL. That makes processing
// idempotent: handling the same event twice, or an old event after a newer
// one, always indexes whatever PostgreSQL holds now.
//
// Producers must use DocumentID as the Kafka message key so that events for
// the same document stay in order.
type Event struct {
	EventID    string    `json:"event_id"`
	DocumentID string    `json:"document_id"`
	Operation  Operation `json:"operation"`
	Version    int64     `json:"version"`
}

// LogAttrs returns the key/value pairs describing this event for structured
// logging, so the pipeline can log it without knowing its shape.
func (e Event) LogAttrs() []any {
	return []any{
		"event_id", e.EventID,
		"document_id", e.DocumentID,
		"operation", string(e.Operation),
	}
}

// DecodeEvent parses and validates a message value. Any failure wraps
// ErrMalformedEvent.
func DecodeEvent(data []byte) (Event, error) {
	var ev Event
	if err := json.Unmarshal(data, &ev); err != nil {
		return Event{}, fmt.Errorf("%w: %w", ErrMalformedEvent, err)
	}
	if ev.EventID == "" {
		return Event{}, fmt.Errorf("%w: event_id is required", ErrMalformedEvent)
	}
	if err := uuid.Validate(ev.DocumentID); err != nil {
		return Event{}, fmt.Errorf("%w: document_id must be a UUID: %w", ErrMalformedEvent, err)
	}
	if ev.Operation != OperationUpsert && ev.Operation != OperationDelete {
		return Event{}, fmt.Errorf("%w: unknown operation %q", ErrMalformedEvent, ev.Operation)
	}
	if ev.Version <= 0 {
		return Event{}, fmt.Errorf("%w: version must be positive", ErrMalformedEvent)
	}
	return ev, nil
}

// permanentError marks a failure that retrying will not fix.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func permanent(err error) error {
	return &permanentError{err: err}
}

func isPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}
