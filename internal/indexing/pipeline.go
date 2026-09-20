package indexing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"near-real-time-hybrid-search-engine/internal/kafka"
)

const (
	baseRetryDelay = 500 * time.Millisecond
	maxRetryDelay  = 10 * time.Second
)

// ErrSkipMessage marks a message that is valid but carries nothing to index,
// such as a Kafka tombstone. Its offset is committed and nothing is
// dead-lettered.
var ErrSkipMessage = errors.New("message skipped")

// LoggableEvent is an event the pipeline can describe in a log line without
// knowing its shape. Both Event and cdc.ChangeEvent satisfy it, which is what
// lets one pipeline serve the application's own events and Debezium change
// events instead of each ingestion path growing its own copy of the retry and
// dead-letter rules.
type LoggableEvent interface {
	LogAttrs() []any
}

// DecodeFunc parses a message value into an event. Returning an error that
// wraps ErrSkipMessage tells the pipeline the message is finished and needs no
// indexing.
type DecodeFunc[E LoggableEvent] func(data []byte) (E, error)

// EventIndexer applies one event to the search stores.
type EventIndexer[E LoggableEvent] interface {
	Index(ctx context.Context, ev E) error
}

// DeadLetterPublisher stores messages that could not be indexed.
type DeadLetterPublisher interface {
	Publish(ctx context.Context, msg kafka.Message, cause error) error
}

// Pipeline decodes a message, indexes it with bounded retries and sends
// failures to the dead-letter topic.
//
// How errors are handled:
//   - Skipped message (a tombstone): finished without indexing.
//   - Malformed event: dead-lettered at once; retrying cannot fix it.
//   - Permanent error (a store or Gemini rejected the request): dead-lettered
//     at once.
//   - Temporary error (timeout, rate limit, 5xx, connection failure): retried
//     with exponential backoff, then dead-lettered after maxAttempts.
//   - Context cancelled (shutdown): returned without dead-lettering, so the
//     offset is not committed and the message is delivered again later.
//   - Dead-letter publish failed: returned, because committing the offset
//     would lose the message.
//
// This is the only place that decides to retry. The indexers below it try each
// write once and return the error, so the policy lives here rather than being
// spread through the pipeline.
type Pipeline[E LoggableEvent] struct {
	decode      DecodeFunc[E]
	indexer     EventIndexer[E]
	deadLetters DeadLetterPublisher
	maxAttempts int
	logger      *slog.Logger
	backoff     func(attempt int) time.Duration
}

// NewPipeline returns a Pipeline that parses messages with decode and tries
// each event at most maxAttempts times.
func NewPipeline[E LoggableEvent](
	decode DecodeFunc[E],
	indexer EventIndexer[E],
	deadLetters DeadLetterPublisher,
	maxAttempts int,
	logger *slog.Logger,
) *Pipeline[E] {
	return &Pipeline[E]{
		decode:      decode,
		indexer:     indexer,
		deadLetters: deadLetters,
		maxAttempts: max(maxAttempts, 1),
		logger:      logger,
		backoff:     backoff,
	}
}

// Process handles one message. A nil result means the message is finished,
// either indexed or dead-lettered, and its offset may be committed. A non-nil
// result means the offset must not be committed.
func (p *Pipeline[E]) Process(ctx context.Context, msg kafka.Message) error {
	log := p.logger.With("partition", msg.Partition, "offset", msg.Offset)

	ev, err := p.decode(msg.Value)
	switch {
	case errors.Is(err, ErrSkipMessage):
		log.Debug("message skipped", "reason", err)
		return nil
	case err != nil:
		log.Warn("malformed event", "error", err)
		return p.deadLetter(ctx, log, msg, err)
	}
	log = log.With(ev.LogAttrs()...)
	started := time.Now()

	for attempt := 1; ; attempt++ {
		err := p.indexer.Index(ctx, ev)
		switch {
		case err == nil:
			log.Debug("event indexed", "attempts", attempt, "duration", time.Since(started))
			return nil
		case ctx.Err() != nil:
			return fmt.Errorf("index message at offset %d: %w", msg.Offset, ctx.Err())
		case isPermanent(err):
			log.Error("indexing failed permanently", "error", err)
			return p.deadLetter(ctx, log, msg, err)
		case attempt >= p.maxAttempts:
			log.Error("indexing failed after retries", "attempts", attempt, "error", err)
			return p.deadLetter(ctx, log, msg, err)
		}

		delay := p.backoff(attempt)
		log.Warn("indexing failed, will retry", "attempt", attempt, "retry_in", delay, "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("index message at offset %d: %w", msg.Offset, ctx.Err())
		case <-time.After(delay):
		}
	}
}

func (p *Pipeline[E]) deadLetter(ctx context.Context, log *slog.Logger, msg kafka.Message, cause error) error {
	if err := p.deadLetters.Publish(ctx, msg, cause); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("dead-letter message: %w", ctx.Err())
		}
		return errors.Join(err, cause)
	}
	log.Info("message sent to dead-letter topic")
	return nil
}

// backoff returns an exponential delay with jitter: roughly 0.5s, 1s, 2s, 4s...
// capped at maxRetryDelay.
func backoff(attempt int) time.Duration {
	shift := min(max(attempt-1, 0), 10) // avoid overflow for large attempt counts
	d := min(baseRetryDelay<<shift, maxRetryDelay)
	return d/2 + rand.N(d/2)
}
