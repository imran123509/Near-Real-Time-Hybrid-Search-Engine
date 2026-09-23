// Package dlq describes messages that could not be processed and the contract
// for storing them.
//
// A dead-letter message keeps the original payload byte for byte, together
// with where it came from and why it failed, so it can be investigated later
// and, once the cause is fixed, replayed onto the original topic. The package
// holds no transport: internal/kafka publishes these messages to the
// dead-letter topic.
package dlq

import (
	"context"
	"time"
)

// maxErrorLength caps the stored error text. A dependency can return a very
// long message, and the dead-letter topic is for triage, not for stack traces.
const maxErrorLength = 2048

// Source is where a failed message came from.
type Source struct {
	Topic     string
	Partition int
	Offset    int64
	// Key is the original message key, which decides the partition. In this
	// pipeline it is Debezium's JSON primary key, such as {"id":"..."}.
	Key []byte
	// Payload is the original message value, stored unchanged.
	Payload []byte
}

// Message is one failed event as written to the dead-letter topic.
//
// Payload is the original value: JSON encodes it as base64, so it survives
// byte for byte, whatever it contained. Nothing else in the message is
// derived from it, so no part of a document's contents is duplicated into the
// metadata.
type Message struct {
	OriginalTopic     string `json:"original_topic"`
	OriginalPartition int    `json:"original_partition"`
	OriginalOffset    int64  `json:"original_offset"`

	EventKey string `json:"event_key"`

	Payload []byte `json:"payload"`

	// Error is why processing failed, trimmed to a usable length.
	Error string `json:"error"`
	// ErrorType is the failure's classification, such as "non_retryable";
	// see retry.Kind.
	ErrorType string `json:"error_type"`
	// Attempts is how many times processing was tried before giving up.
	Attempts int `json:"attempts"`
	// FailedAt is when the message was given up on, in UTC.
	FailedAt time.Time `json:"failed_at"`
}

// New builds a dead-letter message for a failed source message.
//
// The payload is copied, so a later reuse of the consumer's buffer cannot
// change what was recorded.
func New(src Source, cause error, errorType string, attempts int) Message {
	reason := ""
	if cause != nil {
		reason = truncate(cause.Error(), maxErrorLength)
	}
	return Message{
		OriginalTopic:     src.Topic,
		OriginalPartition: src.Partition,
		OriginalOffset:    src.Offset,
		EventKey:          string(src.Key),
		Payload:           append([]byte(nil), src.Payload...),
		Error:             reason,
		ErrorType:         errorType,
		Attempts:          attempts,
		FailedAt:          time.Now().UTC(),
	}
}

// LogAttrs returns the fields describing this message for structured logging.
// The payload and the key are left out: the payload is the document's
// contents, and the offset already identifies the message.
func (m Message) LogAttrs() []any {
	return []any{
		"original_topic", m.OriginalTopic,
		"original_partition", m.OriginalPartition,
		"original_offset", m.OriginalOffset,
		"error_type", m.ErrorType,
		"attempts", m.Attempts,
	}
}

// Publisher stores dead-letter messages. internal/kafka implements it against
// the dead-letter topic.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(ctx context.Context, msg Message) error

// Publish calls f.
func (f PublisherFunc) Publish(ctx context.Context, msg Message) error { return f(ctx, msg) }

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
