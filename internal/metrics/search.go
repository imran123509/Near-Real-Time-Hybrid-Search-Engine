package metrics

import (
	"time"

	"near-real-time-hybrid-search-engine/internal/search/hybrid"
)

// This file turns what the search side reports into metrics. The hybrid
// service knows nothing about Prometheus; it reports a SearchStats, and the
// mapping from that to labels lives here, in one place, where the label values
// can be checked against a bounded set.

// SearchObserver returns the function to register with
// hybrid.Service.Observe.
//
// One search produces one search_requests_total increment and, because every
// search calls OpenSearch, an embedding provider and Qdrant, up to three
// dependency observations. That is what makes it possible to see which of the
// three is responsible when searches get slower.
func (m *Metrics) SearchObserver() func(hybrid.SearchStats) {
	return func(st hybrid.SearchStats) { m.RecordSearch(st) }
}

// RecordSearch records one finished search.
func (m *Metrics) RecordSearch(st hybrid.SearchStats) {
	if m == nil {
		return
	}

	outcome := searchStatus(st)
	m.searchRequests.WithLabelValues(outcome).Inc()
	m.searchDuration.WithLabelValues(outcome).Observe(seconds(st.Duration))

	switch outcome {
	case "success":
		m.searchResults.Observe(float64(st.Results))
	case "error":
		stage := st.Stage
		if stage == "" {
			stage = "unknown"
		}
		m.searchErrors.WithLabelValues(stage).Inc()
	}

	// Each stage that ran is a dependency call. A stage that never started
	// has a zero duration and is left out, so the counts stay honest about
	// what was actually asked of each service.
	m.recordSearchStage(st, "opensearch", "search", st.KeywordDuration, "keyword")
	m.recordSearchStage(st, "embedder", "embed", st.EmbeddingDuration, "embedding")
	m.recordSearchStage(st, "qdrant", "search", st.VectorDuration, "vector")
}

// recordSearchStage records one dependency call made during a search. The
// stage that failed is the one whose name the service reported; the others
// completed, whatever happened afterwards.
func (m *Metrics) recordSearchStage(st hybrid.SearchStats, dependency, operation string, took time.Duration, stage string) {
	if took <= 0 {
		return
	}
	failed := st.Stage == stage
	m.RecordDependency(dependency, operation, took, failed)
}

// RecordDependency records one call to OpenSearch, Qdrant or the embedding
// provider, wherever it was made from.
func (m *Metrics) RecordDependency(dependency, operation string, took time.Duration, failed bool) {
	if m == nil {
		return
	}
	outcome := "success"
	if failed {
		outcome = "error"
		m.dependencyErrors.WithLabelValues(dependency, operation).Inc()
	}
	m.dependencyRequests.WithLabelValues(dependency, operation, outcome).Inc()
	m.dependencyDuration.WithLabelValues(dependency, operation).Observe(seconds(took))
}

// searchStatus reduces a search to one of three bounded outcomes.
//
// A cancelled search is kept apart from a failed one: the caller gave up or
// ran out of time, and counting that as an error would make a client timeout
// look like a broken search service.
func searchStatus(st hybrid.SearchStats) string {
	switch {
	case st.Err == nil:
		return "success"
	case st.Cancelled:
		return "cancelled"
	default:
		return "error"
	}
}
