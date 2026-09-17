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

// EventIndexer applies one event to the search stores.
type EventIndexer interface {
	Index(ctx context.Context, ev Event) error
}

// DeadLetterPublisher stores messages that could not be indexed.
type DeadLetterPublisher interface {
	Publish(ctx context.Context, msg kafka.Message, cause error) error
}

// Pipeline decodes a message, indexes it with bounded retries and sends
// failures to the dead-letter topic.
//
// How errors are handled:
//   - Malformed event: dead-lettered at once; retrying cannot fix it.
//   - Permanent error (a store or Gemini rejected the request): dead-lettered
//     at once.
//   - Temporary error (timeout, rate limit, 5xx, connection failure): retried
//     with exponential backoff, then dead-lettered after maxAttempts.
//   - Context cancelled (shutdown): returned without dead-lettering, so the
//     offset is not committed and the message is delivered again later.
//   - Dead-letter publish failed: returned, because committing the offset
//     would lose the message.
type Pipeline struct {
	indexer     EventIndexer
	deadLetters DeadLetterPublisher
	maxAttempts int
	logger      *slog.Logger
	backoff     func(attempt int) time.Duration
}

// NewPipeline returns a Pipeline that tries each event at most maxAttempts times.
func NewPipeline(indexer EventIndexer, deadLetters DeadLetterPublisher, maxAttempts int, logger *slog.Logger) *Pipeline {
	return &Pipeline{
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
func (p *Pipeline) Process(ctx context.Context, msg kafka.Message) error {
	log := p.logger.With("partition", msg.Partition, "offset", msg.Offset)

	ev, err := DecodeEvent(msg.Value)
	if err != nil {
		log.Warn("malformed event", "error", err)
		return p.deadLetter(ctx, log, msg, err)
	}
	log = log.With("event_id", ev.EventID, "document_id", ev.DocumentID, "operation", ev.Operation)

	for attempt := 1; ; attempt++ {
		err := p.indexer.Index(ctx, ev)
		switch {
		case err == nil:
			log.Debug("event indexed", "attempts", attempt)
			return nil
		case ctx.Err() != nil:
			return fmt.Errorf("index event %s: %w", ev.EventID, ctx.Err())
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
			return fmt.Errorf("index event %s: %w", ev.EventID, ctx.Err())
		case <-time.After(delay):
		}
	}
}

func (p *Pipeline) deadLetter(ctx context.Context, log *slog.Logger, msg kafka.Message, cause error) error {
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
