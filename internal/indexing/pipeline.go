package indexing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"near-real-time-hybrid-search-engine/internal/dlq"
	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/kafka"
	"near-real-time-hybrid-search-engine/internal/retry"
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

// EventIndexer applies one event to the search stores and reports what the
// writes did, so the pipeline can tell an ordinary change from a redelivery
// and from an event the indexes have already moved past.
type EventIndexer[E LoggableEvent] interface {
	Index(ctx context.Context, ev E) (cdc.Effect, error)
}

// Outcome is how one message ended. It is what a metrics exporter will count
// later: every message produces exactly one, reported through Pipeline's
// observer, so counters cannot drift from what the logs say.
type Outcome string

const (
	// OutcomeIndexed means the event was written to the search stores.
	OutcomeIndexed Outcome = "indexed"
	// OutcomeSkipped means the message carried nothing to index.
	OutcomeSkipped Outcome = "skipped"
	// OutcomeDeadLettered means the message was stored in the dead-letter
	// topic. Its offset is committed; the event is not in the indexes.
	OutcomeDeadLettered Outcome = "dead_lettered"
	// OutcomeCancelled means processing stopped for shutdown. The offset is
	// not committed and the message will be delivered again.
	OutcomeCancelled Outcome = "cancelled"
	// OutcomeFailed means the message could be neither indexed nor
	// dead-lettered. The offset is not committed.
	OutcomeFailed Outcome = "failed"
)

// Report describes one finished message, for logging and, later, metrics.
//
// It carries everything the planned counters need: Outcome and Attempts for
// events, retries and failures, Effect for duplicates and superseded events,
// Duration for a latency histogram, and Err, which is a *cdc.StoreError
// whenever one store failed after another had already been written -- the
// partial failures that make re-processing necessary.
type Report struct {
	Outcome Outcome
	// Effect is what the writes did, for a message that reached the stores.
	Effect cdc.Effect
	// Attempts is how many times indexing was tried, so Attempts-1 is the
	// number of retries.
	Attempts int
	// ErrorKind classifies the failure behind a dead-lettered or failed
	// message; it is meaningless for a success.
	ErrorKind retry.Kind
	Duration  time.Duration
	Err       error
}

// Pipeline decodes a message, indexes it with bounded retries and sends what
// it cannot index to the dead-letter topic.
//
// Commit rules, which the worker pool applies by acknowledging a message only
// when Process returns nil:
//
//	indexed                      -> commit
//	skipped (tombstone)          -> commit
//	retried, then indexed        -> commit
//	retries exhausted -> DLQ ok  -> commit
//	not retryable     -> DLQ ok  -> commit
//	DLQ publish failed           -> DO NOT commit; Kafka redelivers
//	shutdown (context cancelled) -> DO NOT commit; Kafka redelivers
//
// The order matters: the dead-letter message is stored first and the original
// offset is committed only afterwards, so a crash in between causes a
// duplicate rather than a lost event. Duplicates are safe because indexing is
// idempotent; lost events are not recoverable.
//
// This is the only place that decides to retry. The indexers below it try each
// write once and return the error, so the policy lives here rather than being
// spread through the pipeline.
type Pipeline[E LoggableEvent] struct {
	decode      DecodeFunc[E]
	indexer     EventIndexer[E]
	deadLetters dlq.Publisher
	retries     *retry.Runner
	logger      *slog.Logger
	observe     func(Report)
}

// NewPipeline returns a Pipeline that parses messages with decode and retries
// failures under policy. An invalid policy is an error rather than a silent
// fallback.
func NewPipeline[E LoggableEvent](
	decode DecodeFunc[E],
	indexer EventIndexer[E],
	deadLetters dlq.Publisher,
	policy retry.Policy,
	logger *slog.Logger,
) (*Pipeline[E], error) {
	if decode == nil || indexer == nil || deadLetters == nil || logger == nil {
		return nil, errors.New("indexing pipeline: decoder, indexer, dead-letter publisher and logger are required")
	}
	p := &Pipeline[E]{decode: decode, indexer: indexer, deadLetters: deadLetters, logger: logger}

	runner, err := retry.New(policy, ClassifyError, p.logRetry)
	if err != nil {
		return nil, fmt.Errorf("indexing pipeline: %w", err)
	}
	p.retries = runner
	return p, nil
}

// Observe registers a function called once per finished message. It is the
// single place to add metrics later, and it must not block.
func (p *Pipeline[E]) Observe(f func(Report)) { p.observe = f }

// Process handles one message. A nil result means the message is finished,
// either indexed or dead-lettered, and its offset may be committed. A non-nil
// result means the offset must not be committed.
func (p *Pipeline[E]) Process(ctx context.Context, msg kafka.Message) error {
	started := time.Now()
	log := p.logger.With("topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)

	ev, err := p.decode(msg.Value)
	switch {
	case errors.Is(err, ErrSkipMessage):
		log.Debug("message_skipped", "reason", err)
		p.report(Report{Outcome: OutcomeSkipped, Duration: time.Since(started)})
		return nil
	case err != nil:
		// A message that cannot be parsed will not parse next time either.
		log.Warn("event_processing_failed",
			"attempt", 0, "max_attempts", p.retries.Policy().MaxAttempts,
			"error_type", retry.NonRetryable.String(), "error", err)
		return p.deadLetter(ctx, log, msg, err, retry.NonRetryable, 0, started)
	}
	log = log.With(ev.LogAttrs()...)

	var effect cdc.Effect
	result := p.retries.Do(ctx, func(ctx context.Context) error {
		var err error
		effect, err = p.indexer.Index(ctx, ev)
		return err
	})

	switch {
	case result.Succeeded():
		if result.Attempts > 1 {
			log.Info("event_processing_retry_succeeded",
				"attempts", result.Attempts, "effect", string(effect), "duration", time.Since(started))
		}
		switch effect {
		case cdc.EffectReapplied:
			// The same event arrived again, after a redelivery, a restart or
			// a replay. Both stores were written again and hold what they
			// held before.
			log.Info("event_reprocessed", "attempts", result.Attempts, "duration", time.Since(started))
		case cdc.EffectStale:
			// A later event for this document is already indexed, so this one
			// had nothing left to apply.
			log.Info("event_skipped_as_stale", "attempts", result.Attempts, "duration", time.Since(started))
		default:
			if result.Attempts == 1 {
				log.Debug("event_indexed", "effect", string(effect), "duration", time.Since(started))
			}
		}
		p.report(Report{Outcome: OutcomeIndexed, Effect: effect, Attempts: result.Attempts, Duration: time.Since(started)})
		return nil

	case retry.IsCancellation(result.Err) && ctx.Err() != nil:
		// Shutdown, not a failure: leave the offset uncommitted.
		log.Debug("event_processing_cancelled", "attempts", result.Attempts)
		p.report(Report{Outcome: OutcomeCancelled, Attempts: result.Attempts, Duration: time.Since(started), Err: result.Err})
		return fmt.Errorf("index message at offset %d: %w", msg.Offset, result.Err)

	default:
		log.Error("event_processing_failed",
			"attempt", result.Attempts, "max_attempts", p.retries.Policy().MaxAttempts,
			"error_type", result.Kind.String(), "error", result.Err)
		return p.deadLetter(ctx, log, msg, result.Err, result.Kind, result.Attempts, started)
	}
}

// deadLetter stores the message and its failure. It returns an error only
// when the message was not stored, which keeps the original offset
// uncommitted so Kafka delivers it again.
func (p *Pipeline[E]) deadLetter(
	ctx context.Context,
	log *slog.Logger,
	msg kafka.Message,
	cause error,
	kind retry.Kind,
	attempts int,
	started time.Time,
) error {
	letter := dlq.New(dlq.Source{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Payload:   msg.Value,
	}, cause, kind.String(), attempts)

	if err := p.deadLetters.Publish(ctx, letter); err != nil {
		if ctx.Err() != nil {
			log.Debug("event_processing_cancelled", "attempts", attempts)
			p.report(Report{Outcome: OutcomeCancelled, Attempts: attempts, ErrorKind: kind, Duration: time.Since(started), Err: ctx.Err()})
			return fmt.Errorf("dead-letter message at offset %d: %w", msg.Offset, ctx.Err())
		}
		// Nothing may be committed now: the event is in neither the indexes
		// nor the dead-letter topic.
		joined := errors.Join(err, cause)
		log.Error("event_dlq_publish_failed", "error", err, "cause", cause)
		p.report(Report{Outcome: OutcomeFailed, Attempts: attempts, ErrorKind: kind, Duration: time.Since(started), Err: joined})
		return joined
	}

	log.Warn("event_sent_to_dlq", letter.LogAttrs()...)
	p.report(Report{Outcome: OutcomeDeadLettered, Attempts: attempts, ErrorKind: kind, Duration: time.Since(started), Err: cause})
	return nil
}

// logRetry records one failed attempt and the wait before the next.
func (p *Pipeline[E]) logRetry(a retry.Attempt) {
	p.logger.Warn("event_processing_failed",
		"attempt", a.Number, "max_attempts", p.retries.Policy().MaxAttempts,
		"error_type", a.Kind.String(), "backoff", a.Delay, "error", a.Err)
}

func (p *Pipeline[E]) report(r Report) {
	if p.observe != nil {
		p.observe(r)
	}
}

// ClassifyError decides whether a failure is worth retrying.
//
// It reads the typed errors the packages below already return, never their
// text: a wrapper marks failures the stores rejected outright, and errors
// carrying Temporary() say so themselves. Anything unrecognised counts as
// Unknown, which is retried within the policy rather than dead-lettered on a
// guess.
func ClassifyError(err error) retry.Kind {
	if err == nil {
		return retry.Unknown
	}
	if isPermanent(err) {
		return retry.NonRetryable
	}
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) {
		if temporary.Temporary() {
			return retry.Retryable
		}
		return retry.NonRetryable
	}
	return retry.Unknown
}
