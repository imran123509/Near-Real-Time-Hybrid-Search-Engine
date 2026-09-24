package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"

	"near-real-time-hybrid-search-engine/internal/indexing"
	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/kafka"
	"near-real-time-hybrid-search-engine/internal/retry"
	"near-real-time-hybrid-search-engine/internal/search/hybrid"
)

// These tests check the mapping from what a component reports to what
// Prometheus stores: the labels, and which counter moves. They use the real
// observation types from the packages being measured, so a change to one of
// those hooks breaks here rather than silently stopping a metric.

func TestSearchObserver(t *testing.T) {
	tests := []struct {
		name       string
		stats      hybridStats
		wantStatus string
		wantStage  string
	}{
		{
			name:       "a successful search",
			stats:      hybridStats{results: 7, keyword: 20 * time.Millisecond, embed: 30 * time.Millisecond, vector: 10 * time.Millisecond, total: 65 * time.Millisecond},
			wantStatus: "success",
		},
		{
			name: "a failed search names the stage",
			stats: hybridStats{
				keyword: 5 * time.Millisecond, embed: 2 * time.Millisecond, total: 8 * time.Millisecond,
				stage: "embedding", err: errors.New("provider unavailable"),
			},
			wantStatus: "error",
			wantStage:  "embedding",
		},
		{
			// A caller that gave up is not a failure of the search service,
			// and counting it as one would make a client timeout look like an
			// outage.
			name:       "a cancelled search is not an error",
			stats:      hybridStats{keyword: time.Millisecond, total: 2 * time.Millisecond, cancelled: true, err: context.Canceled},
			wantStatus: "cancelled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reg := newTestMetrics(t)
			m.SearchObserver()(tt.stats.build())

			if got := gatheredValue(t, reg, "hybrid_search_search_requests_total", map[string]string{"status": tt.wantStatus}); got != 1 {
				t.Errorf("search_requests_total{status=%s} = %v, want 1", tt.wantStatus, got)
			}
			if got := histogramCount(t, reg, "hybrid_search_search_duration_seconds", map[string]string{"status": tt.wantStatus}); got != 1 {
				t.Errorf("search_duration_seconds{status=%s} recorded %d observations, want 1", tt.wantStatus, got)
			}
			if tt.wantStage != "" {
				if got := gatheredValue(t, reg, "hybrid_search_search_errors_total", map[string]string{"stage": tt.wantStage}); got != 1 {
					t.Errorf("search_errors_total{stage=%s} = %v, want 1", tt.wantStage, got)
				}
			} else if count := countSeries(t, reg, "hybrid_search_search_errors_total"); count != 0 {
				t.Errorf("search_errors_total has %d series for a search that did not fail", count)
			}
		})
	}
}

// Every stage of a search is a call to a dependency, and which one is slow is
// the question these metrics exist to answer.
func TestSearchObserverRecordsDependencies(t *testing.T) {
	m, reg := newTestMetrics(t)
	m.SearchObserver()(hybridStats{
		results: 3,
		keyword: 20 * time.Millisecond, embed: 40 * time.Millisecond, vector: 15 * time.Millisecond,
		total: 80 * time.Millisecond,
	}.build())

	for _, want := range []struct{ dependency, operation string }{
		{"opensearch", "search"}, {"embedder", "embed"}, {"qdrant", "search"},
	} {
		labels := map[string]string{"dependency": want.dependency, "operation": want.operation, "status": "success"}
		if got := gatheredValue(t, reg, "hybrid_search_dependency_requests_total", labels); got != 1 {
			t.Errorf("dependency_requests_total{%s,%s} = %v, want 1", want.dependency, want.operation, got)
		}
	}

	// A stage that never ran must not be counted as a call that took no time.
	m2, reg2 := newTestMetrics(t)
	m2.SearchObserver()(hybridStats{keyword: 5 * time.Millisecond, stage: "keyword", err: errors.New("down"), total: 5 * time.Millisecond}.build())
	if got := gatheredValue(t, reg2, "hybrid_search_dependency_errors_total",
		map[string]string{"dependency": "opensearch", "operation": "search"}); got != 1 {
		t.Errorf("dependency_errors_total for the failed stage = %v, want 1", got)
	}
	if count := countSeries(t, reg2, "hybrid_search_dependency_requests_total"); count != 1 {
		t.Errorf("%d dependency series, want only the stage that ran", count)
	}
}

func TestConsumerObserver(t *testing.T) {
	m, reg := newTestMetrics(t)
	observe := m.ConsumerObserver()

	observe(kafka.Event{Kind: kafka.EventFetched, Topic: "events"})
	observe(kafka.Event{Kind: kafka.EventFetched, Topic: "events"})
	observe(kafka.Event{Kind: kafka.EventCommitted, Topic: "events"})
	observe(kafka.Event{Kind: kafka.EventCommitFailed, Topic: "events", Err: errors.New("broker down")})
	observe(kafka.Event{Kind: kafka.EventFetchFailed, Topic: "events", Err: errors.New("broker down")})

	if got := gatheredValue(t, reg, "hybrid_search_kafka_messages_total",
		map[string]string{"topic": "events", "outcome": "consumed"}); got != 2 {
		t.Errorf("consumed = %v, want 2", got)
	}
	if got := gatheredValue(t, reg, "hybrid_search_kafka_offset_commits_total", nil); got != 1 {
		t.Errorf("commits = %v, want 1", got)
	}
	for kind, want := range map[string]float64{"commit": 1, "fetch": 1} {
		if got := gatheredValue(t, reg, "hybrid_search_kafka_consumer_errors_total", map[string]string{"type": kind}); got != want {
			t.Errorf("consumer_errors_total{type=%s} = %v, want %v", kind, got, want)
		}
	}
	// The broker's error text must never become a label.
	assertNoLabelContains(t, reg, "broker down")
}

func TestRetryObserver(t *testing.T) {
	m, reg := newTestMetrics(t)
	observe := m.RetryObserver()

	observe(retry.Attempt{Number: 1, Delay: 500 * time.Millisecond, Kind: retry.Retryable, Err: errors.New("opensearch unavailable")})
	observe(retry.Attempt{Number: 2, Delay: time.Second, Kind: retry.Retryable, Err: errors.New("opensearch unavailable")})
	observe(retry.Attempt{Number: 1, Delay: 500 * time.Millisecond, Kind: retry.Unknown, Err: errors.New("something new")})

	if got := gatheredValue(t, reg, "hybrid_search_kafka_retries_total", map[string]string{"error_type": "retryable"}); got != 2 {
		t.Errorf("retries{retryable} = %v, want 2", got)
	}
	if got := gatheredValue(t, reg, "hybrid_search_kafka_retries_total", map[string]string{"error_type": "unknown"}); got != 1 {
		t.Errorf("retries{unknown} = %v, want 1", got)
	}
	if got := histogramCount(t, reg, "hybrid_search_kafka_retry_delay_seconds", nil); got != 3 {
		t.Errorf("retry_delay_seconds recorded %d observations, want 3", got)
	}
	assertNoLabelContains(t, reg, "opensearch unavailable")
}

// The pipeline's report is where a message's fate becomes a metric: indexed,
// skipped, dead-lettered, or not stored anywhere at all.
func TestPipelineObserver(t *testing.T) {
	tests := []struct {
		name        string
		report      indexing.Report
		wantOutcome string
		wantDLQ     string
		wantFailure bool
		wantExhaust string
	}{
		{
			name:        "indexed",
			report:      indexing.Report{Outcome: indexing.OutcomeIndexed, Attempts: 1, Duration: 50 * time.Millisecond},
			wantOutcome: "indexed",
		},
		{
			name:        "skipped",
			report:      indexing.Report{Outcome: indexing.OutcomeSkipped, Duration: time.Millisecond},
			wantOutcome: "skipped",
		},
		{
			// Retries were spent and did not help.
			name: "dead-lettered after the retries ran out",
			report: indexing.Report{
				Outcome: indexing.OutcomeDeadLettered, Attempts: 5,
				ErrorKind: retry.Retryable, Duration: 8 * time.Second,
			},
			wantOutcome: "dead_lettered",
			wantDLQ:     "retry_exhausted",
			wantExhaust: "retryable",
		},
		{
			// Never worth retrying, so no attempts were wasted on it.
			name: "dead-lettered without retrying",
			report: indexing.Report{
				Outcome: indexing.OutcomeDeadLettered, Attempts: 1,
				ErrorKind: retry.NonRetryable, Duration: 10 * time.Millisecond,
			},
			wantOutcome: "dead_lettered",
			wantDLQ:     "non_retryable",
		},
		{
			// Neither indexed nor stored: the offset stays uncommitted.
			name: "stored nowhere",
			report: indexing.Report{
				Outcome: indexing.OutcomeFailed, Attempts: 5,
				ErrorKind: retry.Retryable, Duration: 9 * time.Second,
			},
			wantOutcome: "failed",
			wantFailure: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reg := newTestMetrics(t)
			m.PipelineObserver("events")(tt.report)

			labels := map[string]string{"topic": "events", "outcome": tt.wantOutcome}
			if got := gatheredValue(t, reg, "hybrid_search_kafka_messages_total", labels); got != 1 {
				t.Errorf("kafka_messages_total{outcome=%s} = %v, want 1", tt.wantOutcome, got)
			}
			if got := histogramCount(t, reg, "hybrid_search_kafka_message_processing_duration_seconds",
				map[string]string{"topic": "events"}); got != 1 {
				t.Errorf("processing duration recorded %d observations, want 1", got)
			}

			if tt.wantDLQ != "" {
				if got := gatheredValue(t, reg, "hybrid_search_kafka_dlq_messages_total",
					map[string]string{"reason": tt.wantDLQ}); got != 1 {
					t.Errorf("dlq_messages_total{reason=%s} = %v, want 1", tt.wantDLQ, got)
				}
			} else if count := countSeries(t, reg, "hybrid_search_kafka_dlq_messages_total"); count != 0 {
				t.Errorf("%d dead-letter series for a message that was not dead-lettered", count)
			}

			// A stored dead letter and a failed publish must never look the
			// same: one means the offset is committed, the other that it is
			// not.
			gotFailure := gatheredValue(t, reg, "hybrid_search_kafka_dlq_publish_errors_total", nil)
			if (gotFailure > 0) != tt.wantFailure {
				t.Errorf("dlq_publish_errors_total = %v, want failure: %v", gotFailure, tt.wantFailure)
			}

			if tt.wantExhaust != "" {
				if got := gatheredValue(t, reg, "hybrid_search_kafka_retry_exhausted_total",
					map[string]string{"error_type": tt.wantExhaust}); got != 1 {
					t.Errorf("retry_exhausted_total{%s} = %v, want 1", tt.wantExhaust, got)
				}
			} else if count := countSeries(t, reg, "hybrid_search_kafka_retry_exhausted_total"); count != 0 {
				t.Errorf("%d exhausted series, want none: no attempts were wasted", count)
			}
		})
	}
}

func TestCDCObserverRecordsStoreCalls(t *testing.T) {
	m, reg := newTestMetrics(t)
	observer := m.CDCObserver()

	observer.StoreCalled(cdc.StoreCall{Store: cdc.StoreKeyword, Operation: "index", Duration: 12 * time.Millisecond})
	observer.StoreCalled(cdc.StoreCall{Store: cdc.StoreEmbedder, Operation: "embed", Duration: 40 * time.Millisecond})
	observer.StoreCalled(cdc.StoreCall{Store: cdc.StoreVector, Operation: "upsert", Duration: 8 * time.Millisecond})
	observer.StoreCalled(cdc.StoreCall{
		Store: cdc.StoreVector, Operation: "delete", Duration: 5 * time.Millisecond,
		Err: errors.New("qdrant unavailable at 10.0.0.5"),
	})

	for _, want := range []struct {
		dependency, operation, status string
	}{
		{"opensearch", "index", "success"},
		{"embedder", "embed", "success"},
		{"qdrant", "upsert", "success"},
		{"qdrant", "delete", "error"},
	} {
		labels := map[string]string{"dependency": want.dependency, "operation": want.operation, "status": want.status}
		if got := gatheredValue(t, reg, "hybrid_search_dependency_requests_total", labels); got != 1 {
			t.Errorf("dependency_requests_total{%s,%s,%s} = %v, want 1", want.dependency, want.operation, want.status, got)
		}
	}
	if got := gatheredValue(t, reg, "hybrid_search_dependency_errors_total",
		map[string]string{"dependency": "qdrant", "operation": "delete"}); got != 1 {
		t.Errorf("dependency_errors_total = %v, want 1", got)
	}
	// An address in an error message is exactly the kind of thing that must
	// never become a label.
	assertNoLabelContains(t, reg, "10.0.0.5")
}

func TestCDCObserverRecordsEvents(t *testing.T) {
	m, reg := newTestMetrics(t)
	observer := m.CDCObserver()

	observer.EventApplied(cdc.EventApplied{Operation: cdc.OperationCreate, Effect: cdc.EffectIndexed, Duration: 30 * time.Millisecond})
	observer.EventApplied(cdc.EventApplied{Operation: cdc.OperationUpdate, Effect: cdc.EffectReapplied, Duration: 25 * time.Millisecond})
	observer.EventApplied(cdc.EventApplied{Operation: cdc.OperationDelete, Effect: cdc.EffectDeleted, Duration: 10 * time.Millisecond})
	observer.EventApplied(cdc.EventApplied{Operation: cdc.OperationRead, Effect: cdc.EffectStale, Duration: 5 * time.Millisecond})
	observer.EventApplied(cdc.EventApplied{
		Operation: cdc.OperationCreate, Duration: 2 * time.Second,
		Store: cdc.StoreVector, Err: errors.New("qdrant unavailable"),
	})

	for _, want := range []struct {
		operation, effect string
	}{
		{"create", "indexed"}, {"update", "reapplied"}, {"delete", "deleted"}, {"read", "stale"}, {"create", "failed"},
	} {
		labels := map[string]string{"operation": want.operation, "effect": want.effect}
		if got := gatheredValue(t, reg, "hybrid_search_indexing_events_total", labels); got != 1 {
			t.Errorf("indexing_events_total{%s,%s} = %v, want 1", want.operation, want.effect, got)
		}
	}
	// Which dependency failed is what tells a partial write from a rejected
	// event.
	if got := gatheredValue(t, reg, "hybrid_search_indexing_dependency_errors_total",
		map[string]string{"dependency": "qdrant"}); got != 1 {
		t.Errorf("indexing_dependency_errors_total{qdrant} = %v, want 1", got)
	}
	if got := histogramCount(t, reg, "hybrid_search_indexing_processing_duration_seconds",
		map[string]string{"operation": "create"}); got != 2 {
		t.Errorf("indexing duration for creates recorded %d observations, want 2", got)
	}
}

// The worker pool's depth is read at scrape time, so the gauges must follow
// whatever the pool reports without the pool pushing anything.
func TestBindWorkerPool(t *testing.T) {
	m, reg := newTestMetrics(t)

	stats := indexing.Stats{Workers: 8, Active: 3, Queued: 12, Capacity: 100}
	m.BindWorkerPool(func() indexing.Stats { return stats })

	checks := map[string]float64{
		"hybrid_search_indexing_workers_total":  8,
		"hybrid_search_indexing_workers_active": 3,
		"hybrid_search_indexing_queue_size":     12,
		"hybrid_search_indexing_queue_capacity": 100,
	}
	for name, want := range checks {
		if got := gatheredValue(t, reg, name, nil); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}

	// A later scrape sees the new value, which is the point of a gauge that
	// is read rather than written.
	stats = indexing.Stats{Workers: 8, Active: 8, Queued: 100, Capacity: 100}
	if got := gatheredValue(t, reg, "hybrid_search_indexing_queue_size", nil); got != 100 {
		t.Errorf("queue size = %v after the pool filled up, want 100", got)
	}
}

// ---------------------------------------------------------------- helpers

// hybridStats builds a hybrid.SearchStats without repeating its field names
// in every test case.
type hybridStats struct {
	results                int
	keyword, embed, vector time.Duration
	total                  time.Duration
	stage                  string
	cancelled              bool
	err                    error
}

func (s hybridStats) build() hybrid.SearchStats {
	return hybrid.SearchStats{
		QueryLength:       12,
		Limit:             10,
		Results:           s.results,
		KeywordDuration:   s.keyword,
		EmbeddingDuration: s.embed,
		VectorDuration:    s.vector,
		Duration:          s.total,
		Stage:             s.stage,
		Cancelled:         s.cancelled,
		Err:               s.err,
	}
}

// gather reads everything the registry holds.
func gather(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	return families
}

// gatheredValue returns the value of one series, read back through the
// registry the way a scrape would see it. Labels that are not given are not
// compared, and a missing series is 0 rather than a failure, so a test can
// assert that nothing was recorded.
func gatheredValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	for _, family := range gather(t, reg) {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if !matchesLabels(metric, labels) {
				continue
			}
			switch {
			case metric.Counter != nil:
				return metric.Counter.GetValue()
			case metric.Gauge != nil:
				return metric.Gauge.GetValue()
			case metric.Histogram != nil:
				return float64(metric.Histogram.GetSampleCount())
			}
		}
	}
	return 0
}

// histogramCount returns how many observations a histogram holds.
func histogramCount(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) int {
	t.Helper()
	for _, family := range gather(t, reg) {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if matchesLabels(metric, labels) && metric.Histogram != nil {
				return int(metric.Histogram.GetSampleCount())
			}
		}
	}
	return 0
}

// countSeries returns how many series a metric has, which is how a test
// checks that nothing was recorded.
func countSeries(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	for _, family := range gather(t, reg) {
		if family.GetName() == name {
			return len(family.GetMetric())
		}
	}
	return 0
}

func matchesLabels(metric *dto.Metric, labels map[string]string) bool {
	for name, want := range labels {
		found := false
		for _, label := range metric.GetLabel() {
			if label.GetName() == name && label.GetValue() == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// assertNoLabelContains is the cardinality guard: error text, addresses,
// queries and IDs must never reach a label, however they were passed in.
func assertNoLabelContains(t *testing.T, reg *prometheus.Registry, forbidden string) {
	t.Helper()
	for _, family := range gather(t, reg) {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetValue() == forbidden || len(label.GetValue()) > 64 {
					t.Errorf("%s has a label %s=%q, which does not belong in a metric",
						family.GetName(), label.GetName(), label.GetValue())
				}
			}
		}
	}
}
