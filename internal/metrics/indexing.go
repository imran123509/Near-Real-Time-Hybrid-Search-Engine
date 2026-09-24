package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"near-real-time-hybrid-search-engine/internal/indexing"
	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/retry"
)

// This file covers the consumer's own work: what happened to each message, how
// long applying it took, and how full the worker pool is.

// PipelineObserver returns the function to register with Pipeline.Observe. It
// is called once per finished message, whatever its outcome, which is what
// keeps the Kafka counters consistent with each other: consumed equals
// indexed plus skipped plus dead-lettered plus failed, give or take what is
// in flight.
func (m *Metrics) PipelineObserver(topic string) func(indexing.Report) {
	return func(r indexing.Report) { m.RecordMessage(topic, r) }
}

// RecordMessage records one finished message.
func (m *Metrics) RecordMessage(topic string, r indexing.Report) {
	if m == nil {
		return
	}
	m.kafkaMessages.WithLabelValues(topic, string(r.Outcome)).Inc()
	m.kafkaProcessing.WithLabelValues(topic).Observe(seconds(r.Duration))

	switch r.Outcome {
	case indexing.OutcomeDeadLettered:
		m.RecordDeadLetter(deadLetterReason(r))
		// Attempts were spent and did not help, which is different from a
		// message that was never worth retrying.
		if r.ErrorKind != retry.NonRetryable {
			m.retryExhausted.WithLabelValues(r.ErrorKind.String()).Inc()
		}
	case indexing.OutcomeFailed:
		m.RecordDeadLetterFailure()
	}
}

// deadLetterReason says why a message was given up on, in two bounded values.
func deadLetterReason(r indexing.Report) string {
	if r.ErrorKind == retry.NonRetryable {
		return "non_retryable"
	}
	return "retry_exhausted"
}

// CDCObserver returns the observer to register with cdc.Service.Observe. It
// reports both what each event did and what each downstream call cost, which
// is how a slow indexing pipeline is attributed to OpenSearch, the embedding
// provider or Qdrant.
func (m *Metrics) CDCObserver() cdc.Observer {
	return cdcObserver{m}
}

type cdcObserver struct{ m *Metrics }

func (o cdcObserver) StoreCalled(call cdc.StoreCall) {
	o.m.RecordDependency(dependencyName(call.Store), call.Operation, call.Duration, call.Err != nil)
}

func (o cdcObserver) EventApplied(event cdc.EventApplied) {
	m := o.m
	if m == nil {
		return
	}
	operation := operationLabel(event.Operation)
	effect := string(event.Effect)
	if event.Err != nil {
		effect = "failed"
	}

	m.indexingEvents.WithLabelValues(operation, effect).Inc()
	m.indexingDuration.WithLabelValues(operation).Observe(seconds(event.Duration))
	if event.Err != nil {
		// An event that failed after one store had already been written is a
		// partial write; the dependency label says which one to look at.
		m.indexingErrors.WithLabelValues(dependencyName(event.Store)).Inc()
	}
}

// operationLabel renders an operation the way an operator reads it, in a
// fixed set of values.
func operationLabel(op cdc.Operation) string {
	switch op {
	case cdc.OperationCreate, cdc.OperationUpdate, cdc.OperationDelete, cdc.OperationRead:
		return strings.ToLower(string(op))
	default:
		return "unknown"
	}
}

// dependencyName maps a store onto the dependency label, which is the same
// set of values the search side uses, so one dashboard covers both.
func dependencyName(store cdc.Store) string {
	switch store {
	case cdc.StoreKeyword:
		return "opensearch"
	case cdc.StoreVector:
		return "qdrant"
	case cdc.StoreEmbedder:
		return "embedder"
	default:
		return "unknown"
	}
}

// BindWorkerPool exposes the pool's depth as gauges.
//
// They are read when Prometheus scrapes, not written while messages are
// processed, so watching the queue costs the pipeline nothing. stats must be
// safe to call from another goroutine; indexing.WorkerPool.Stats is.
func (m *Metrics) BindWorkerPool(stats func() indexing.Stats) {
	if m == nil || stats == nil {
		return
	}
	factory := promauto.With(m.registerer)

	factory.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: Namespace, Name: "indexing_queue_size",
		Help: "Messages waiting in the worker queues right now. At capacity, the consumer stops fetching, which is backpressure rather than a fault.",
	}, func() float64 { return float64(stats().Queued) })

	factory.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: Namespace, Name: "indexing_queue_capacity",
		Help: "How many messages the worker queues can hold.",
	}, func() float64 { return float64(stats().Capacity) })

	factory.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: Namespace, Name: "indexing_workers_active",
		Help: "Workers applying an event right now. Sitting at the worker count means the pool is the limit on throughput.",
	}, func() float64 { return float64(stats().Active) })

	factory.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: Namespace, Name: "indexing_workers_total",
		Help: "Workers in the pool, which is INDEXING_WORKERS.",
	}, func() float64 { return float64(stats().Workers) })
}
