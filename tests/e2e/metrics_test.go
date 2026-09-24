package e2e

import (
	"fmt"
	"strings"
	"testing"
)

// These tests check that both services really expose what docs/metrics.md
// says they expose, and that the counters move when the system does the work.
//
// They assert direction, never exact values: the stack is shared with the
// other tests in this suite, and anything else running against it moves the
// same counters. A counter that rises after a search is the claim; "rises by
// exactly one" would be a claim about the rest of the suite.

// TestMetricsEndpointsAreServed checks both endpoints answer in the
// Prometheus exposition format, with the Go runtime metrics the client
// provides and this application's own.
func TestMetricsEndpointsAreServed(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name     string
		endpoint string
		expect   []string
	}{
		{
			name:     "api",
			endpoint: env.MetricsURL,
			expect: []string{
				"go_goroutines",
				"hybrid_search_http_requests_total",
				"hybrid_search_search_requests_total",
				`hybrid_search_application_info{application=`,
				"hybrid_search_application_ready",
			},
		},
		{
			name:     "consumer",
			endpoint: env.ConsumerMetricsURL,
			expect: []string{
				"go_goroutines",
				"hybrid_search_kafka_messages_total",
				"hybrid_search_indexing_queue_size",
				"hybrid_search_indexing_workers_total",
				`hybrid_search_application_info{application=`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := FetchMetrics(ctx, tt.endpoint)
			if err != nil {
				t.Fatalf("%s: %v", tt.endpoint, err)
			}
			for _, want := range tt.expect {
				if !strings.Contains(body, want) {
					t.Errorf("%s does not expose %s", tt.endpoint, want)
				}
			}
			// The format itself, which is what makes it scrapeable.
			if !strings.Contains(body, "# HELP ") || !strings.Contains(body, "# TYPE ") {
				t.Errorf("%s does not answer in the exposition format", tt.endpoint)
			}
			// Nothing in a label may look like a query, an ID or an address.
			assertBoundedLabels(t, body)
		})
	}
}

// TestSearchMovesTheMetrics runs searches and checks the API's counters and
// its latency histogram follow.
func TestSearchMovesTheMetrics(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "metrics-search")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForSearchHit(t, ctx, doc.Token, doc.ID)

	before, err := FetchMetrics(ctx, env.MetricsURL)
	if err != nil {
		t.Fatal(err)
	}
	const searches = 5
	for range searches {
		if _, status, err := Search(ctx, env, doc.Token, 10); err != nil || status != 200 {
			t.Fatalf("search: status %d, err %v", status, err)
		}
	}
	after, err := FetchMetrics(ctx, env.MetricsURL)
	if err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		metric, match string
	}{
		{"hybrid_search_search_requests_total", `status="success"`},
		{"hybrid_search_search_duration_seconds_count", ""},
		{"hybrid_search_http_requests_total", `route="/api/v1/search"`},
		{"hybrid_search_http_request_duration_seconds_count", `route="/api/v1/search"`},
		// Every search calls all three dependencies.
		{"hybrid_search_dependency_requests_total", `dependency="opensearch"`},
		{"hybrid_search_dependency_requests_total", `dependency="qdrant"`},
		{"hybrid_search_dependency_requests_total", `dependency="embedder"`},
	}
	for _, check := range checks {
		name := check.metric
		if check.match != "" {
			name = fmt.Sprintf("%s{%s}", check.metric, check.match)
		}
		grew := MetricValue(after, check.metric, check.match) - MetricValue(before, check.metric, check.match)
		if grew < searches {
			t.Errorf("%s rose by %v after %d searches, want at least %d", name, grew, searches, searches)
		}
	}

	// The API reports itself ready, from the same checks /ready runs.
	if got := MetricValue(after, "hybrid_search_application_ready", ""); got != 1 {
		t.Errorf("application_ready = %v while searches are succeeding, want 1", got)
	}
}

// TestIndexingMovesTheMetrics checks the consumer's counters follow a row
// travelling through the pipeline.
func TestIndexingMovesTheMetrics(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "metrics-indexing")

	before, err := FetchMetrics(ctx, env.ConsumerMetricsURL)
	if err != nil {
		t.Fatal(err)
	}

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)

	after, err := FetchMetrics(ctx, env.ConsumerMetricsURL)
	if err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		metric, match, why string
	}{
		{"hybrid_search_kafka_messages_total", `outcome="consumed"`, "the message was fetched"},
		{"hybrid_search_kafka_messages_total", `outcome="indexed"`, "the message was applied"},
		{"hybrid_search_kafka_message_processing_duration_seconds_count", "", "processing was timed"},
		{"hybrid_search_indexing_events_total", `operation="create"`, "an insert was indexed"},
		{"hybrid_search_indexing_processing_duration_seconds_count", `operation="create"`, "applying it was timed"},
		{"hybrid_search_dependency_requests_total", `dependency="opensearch"`, "OpenSearch was written"},
		{"hybrid_search_dependency_requests_total", `dependency="qdrant"`, "Qdrant was written"},
		{"hybrid_search_dependency_requests_total", `dependency="embedder"`, "the row was embedded"},
		{"hybrid_search_kafka_offset_commits_total", "", "the offset was committed"},
	}
	for _, check := range checks {
		grew := MetricValue(after, check.metric, check.match) - MetricValue(before, check.metric, check.match)
		if grew <= 0 {
			t.Errorf("%s{%s} did not rise, but %s", check.metric, check.match, check.why)
		}
	}

	// The worker pool reports its shape, which is what the queue-depth and
	// scaling questions are answered from.
	if got := MetricValue(after, "hybrid_search_indexing_workers_total", ""); got < 1 {
		t.Errorf("indexing_workers_total = %v, want the configured worker count", got)
	}
	if got := MetricValue(after, "hybrid_search_indexing_queue_capacity", ""); got < 1 {
		t.Errorf("indexing_queue_capacity = %v, want the configured queue size", got)
	}
}

// A rejected request must be counted as a 4xx, not as a failure of the search
// service: the two mean different things to whoever is on call.
func TestRejectedRequestsAreCountedSeparately(t *testing.T) {
	ctx := t.Context()

	before, err := FetchMetrics(ctx, env.MetricsURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, status, err := Search(ctx, env, "", 10); err != nil || status != 400 {
		t.Fatalf("an empty query answered %d (err %v), want 400", status, err)
	}
	after, err := FetchMetrics(ctx, env.MetricsURL)
	if err != nil {
		t.Fatal(err)
	}

	if grew := MetricValue(after, "hybrid_search_http_requests_total", `status="400"`) -
		MetricValue(before, "hybrid_search_http_requests_total", `status="400"`); grew < 1 {
		t.Errorf("http_requests_total{status=400} rose by %v, want at least 1", grew)
	}
	if grew := MetricValue(after, "hybrid_search_search_requests_total", `status="error"`) -
		MetricValue(before, "hybrid_search_search_requests_total", `status="error"`); grew != 0 {
		t.Errorf("search_requests_total{status=error} rose by %v for a rejected request, "+
			"which never reached the search service", grew)
	}
}

// assertBoundedLabels fails if any label value looks like something that
// varies per request: a query, an ID, a URL or an error message. One of those
// is enough to turn a metric into millions of series.
func assertBoundedLabels(t *testing.T, body string) {
	t.Helper()

	for line := range strings.SplitSeq(body, "\n") {
		if !strings.HasPrefix(line, "hybrid_search_") {
			continue
		}
		open := strings.Index(line, "{")
		close := strings.LastIndex(line, "}")
		if open < 0 || close < open {
			continue
		}
		for _, pair := range strings.Split(line[open+1:close], ",") {
			name, value, found := strings.Cut(pair, "=")
			if !found {
				continue
			}
			value = strings.Trim(value, `"`)
			switch {
			case len(value) > 64:
				t.Errorf("label %s has a %d-character value, which is not a bounded set: %q", name, len(value), value)
			case strings.Contains(value, "http://"), strings.Contains(value, "?q="):
				t.Errorf("label %s carries a URL or a query: %q", name, value)
			}
		}
	}
}

// The document ID of a live document must not appear anywhere in either
// endpoint's output.
func TestDocumentIDsNeverReachLabels(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "metrics-cardinality")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	if _, _, err := Search(ctx, env, doc.Token, 10); err != nil {
		t.Fatal(err)
	}

	for _, endpoint := range []string{env.MetricsURL, env.ConsumerMetricsURL} {
		body, err := FetchMetrics(ctx, endpoint)
		if err != nil {
			t.Fatalf("%s: %v", endpoint, err)
		}
		if strings.Contains(body, doc.ID) {
			t.Errorf("%s exposes the document id %s", endpoint, doc.ID)
		}
		if strings.Contains(body, doc.Token) {
			t.Errorf("%s exposes the search term %s", endpoint, doc.Token)
		}
	}
}
