package metrics

import (
	"near-real-time-hybrid-search-engine/internal/kafka"
	"near-real-time-hybrid-search-engine/internal/retry"
)

// This file covers the consumer side of Kafka: what was fetched, what was
// committed, what was retried and what ended up in the dead-letter topic.

// ConsumerObserver returns the function to register with
// kafka.Consumer.Observe.
//
// Fetched and committed are counted separately on purpose. Messages that are
// processed but never committed are the shape of a consumer that is working
// and losing its work: the processed count rises while the commit count does
// not.
func (m *Metrics) ConsumerObserver() func(kafka.Event) {
	return func(ev kafka.Event) { m.RecordConsumerEvent(ev) }
}

// RecordConsumerEvent records one thing the consumer did with the broker.
func (m *Metrics) RecordConsumerEvent(ev kafka.Event) {
	if m == nil {
		return
	}
	switch ev.Kind {
	case kafka.EventFetched:
		m.kafkaMessages.WithLabelValues(ev.Topic, "consumed").Inc()
	case kafka.EventCommitted:
		m.kafkaCommits.Inc()
	case kafka.EventCommitFailed:
		m.kafkaErrors.WithLabelValues("commit").Inc()
	case kafka.EventFetchFailed:
		m.kafkaErrors.WithLabelValues("fetch").Inc()
	}
}

// RetryObserver returns the function to register with
// Pipeline.ObserveRetry. It is called once per retried attempt, before the
// backoff is waited out.
func (m *Metrics) RetryObserver() func(retry.Attempt) {
	return func(a retry.Attempt) { m.RecordRetry(a) }
}

// RecordRetry records one retried attempt and the wait before it.
func (m *Metrics) RecordRetry(a retry.Attempt) {
	if m == nil {
		return
	}
	m.retries.WithLabelValues(a.Kind.String()).Inc()
	m.retryDelay.Observe(seconds(a.Delay))
}

// RecordDeadLetter records that a message was stored in the dead-letter
// topic. reason is why it was given up on, from a fixed set.
func (m *Metrics) RecordDeadLetter(reason string) {
	if m == nil {
		return
	}
	m.dlqMessages.WithLabelValues(reason).Inc()
}

// RecordDeadLetterFailure records a dead-letter publish that did not work.
//
// It is kept apart from a successful one because the two mean opposite
// things: a stored message is finished and its offset is committed, while a
// failed publish leaves the message uncommitted, to be delivered again. This
// counter rising means the consumer is stuck, not that data was lost.
func (m *Metrics) RecordDeadLetterFailure() {
	if m == nil {
		return
	}
	m.dlqErrors.Inc()
	m.kafkaErrors.WithLabelValues("process").Inc()
}
