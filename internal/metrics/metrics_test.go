package metrics

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"near-real-time-hybrid-search-engine/internal/indexing"
	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/kafka"
	"near-real-time-hybrid-search-engine/internal/retry"
	"near-real-time-hybrid-search-engine/internal/search/hybrid"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// newTestMetrics returns metrics on a registry of their own, so each test
// starts from zero and no test can register the same metric twice.
func newTestMetrics(t *testing.T) (*Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return New(reg), reg
}

// Every metric must reach the registry and accept the labels it is recorded
// with: one that is defined but never registered does not exist as far as
// Prometheus is concerned, and a label set that does not match its definition
// panics at the call site rather than here.
//
// A metric with no observations is not gathered, so this exercises the whole
// surface once and then reads back what a scrape would see. Adding a metric
// makes this fail until it is listed, which is the reminder to document it in
// docs/metrics.md.
func TestNewRegistersEveryMetric(t *testing.T) {
	m, reg := newTestMetrics(t)
	exerciseEveryMetric(t, m)

	want := []string{
		"hybrid_search_http_requests_total",
		"hybrid_search_http_request_duration_seconds",
		"hybrid_search_http_requests_in_flight",
		"hybrid_search_search_requests_total",
		"hybrid_search_search_duration_seconds",
		"hybrid_search_search_errors_total",
		"hybrid_search_dependency_requests_total",
		"hybrid_search_dependency_duration_seconds",
		"hybrid_search_kafka_messages_total",
		"hybrid_search_kafka_message_processing_duration_seconds",
		"hybrid_search_kafka_offset_commits_total",
		"hybrid_search_kafka_consumer_errors_total",
		"hybrid_search_kafka_retries_total",
		"hybrid_search_kafka_retry_exhausted_total",
		"hybrid_search_kafka_dlq_messages_total",
		"hybrid_search_kafka_dlq_publish_errors_total",
		"hybrid_search_indexing_events_total",
		"hybrid_search_indexing_processing_duration_seconds",
		"hybrid_search_indexing_dependency_errors_total",
		"hybrid_search_application_ready",
		"hybrid_search_application_info",
		"hybrid_search_http_response_errors_total",
		"hybrid_search_search_results_returned",
		"hybrid_search_dependency_errors_total",
		"hybrid_search_kafka_retry_delay_seconds",
		"hybrid_search_indexing_queue_size",
		"hybrid_search_indexing_queue_capacity",
		"hybrid_search_indexing_workers_active",
		"hybrid_search_indexing_workers_total",
	}

	gathered := map[string]bool{}
	for _, family := range gather(t, reg) {
		gathered[family.GetName()] = true
	}
	for _, name := range want {
		if !gathered[name] {
			t.Errorf("%s was not registered, or nothing recorded into it", name)
		}
	}
	for name := range gathered {
		if !slices.Contains(want, name) {
			t.Errorf("%s is exposed but not listed here; add it to docs/metrics.md too", name)
		}
	}
}

// exerciseEveryMetric records one observation into every metric, through the
// same methods the application uses.
func exerciseEveryMetric(t *testing.T, m *Metrics) {
	t.Helper()

	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/search?q=go", nil))

	m.RecordSearch(hybrid.SearchStats{Results: 3, KeywordDuration: time.Millisecond, Duration: 2 * time.Millisecond})
	m.RecordSearch(hybrid.SearchStats{Stage: "vector", Duration: time.Millisecond, Err: errors.New("down")})
	m.RecordDependency("qdrant", "search", time.Millisecond, true)

	m.RecordConsumerEvent(kafka.Event{Kind: kafka.EventFetched, Topic: "events"})
	m.RecordConsumerEvent(kafka.Event{Kind: kafka.EventCommitted, Topic: "events"})
	m.RecordConsumerEvent(kafka.Event{Kind: kafka.EventFetchFailed, Topic: "events", Err: errors.New("down")})
	m.RecordRetry(retry.Attempt{Number: 1, Delay: time.Second, Kind: retry.Retryable})
	m.RecordMessage("events", indexing.Report{
		Outcome: indexing.OutcomeDeadLettered, Attempts: 3, ErrorKind: retry.Retryable, Duration: time.Second,
	})
	m.RecordDeadLetterFailure()

	observer := m.CDCObserver()
	observer.StoreCalled(cdc.StoreCall{Store: cdc.StoreKeyword, Operation: "index", Duration: time.Millisecond})
	observer.EventApplied(cdc.EventApplied{Operation: cdc.OperationCreate, Effect: cdc.EffectIndexed, Duration: time.Millisecond})
	observer.EventApplied(cdc.EventApplied{
		Operation: cdc.OperationUpdate, Duration: time.Millisecond,
		Store: cdc.StoreVector, Err: errors.New("down"),
	})

	m.BindWorkerPool(func() indexing.Stats { return indexing.Stats{Workers: 4, Capacity: 40} })
	m.SetReady(true)
	m.SetInfo("near-realtime-search", "test", "api")
}

// Two registries must not interfere with each other. This is what lets a test
// build metrics without the process-wide registry, and it is the reason New
// takes a registerer instead of reaching for the default one itself.
func TestMetricsOnSeparateRegistriesAreIndependent(t *testing.T) {
	first, firstReg := newTestMetrics(t)
	second, secondReg := newTestMetrics(t)

	first.SetReady(true)
	second.SetReady(false)

	if got := testutil.ToFloat64(readyGauge(t, firstReg)); got != 1 {
		t.Errorf("first registry: application_ready = %v, want 1", got)
	}
	if got := testutil.ToFloat64(readyGauge(t, secondReg)); got != 0 {
		t.Errorf("second registry: application_ready = %v, want 0", got)
	}
}

func readyGauge(t *testing.T, reg *prometheus.Registry) prometheus.Gauge {
	t.Helper()
	// Reading the value back through the registry, rather than through the
	// struct, checks that what a scrape sees is what was recorded.
	value := gatheredValue(t, reg, "hybrid_search_application_ready", nil)
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "copy"})
	gauge.Set(value)
	return gauge
}

// A component built without metrics must still work. Every method on a nil
// *Metrics does nothing, so a test or a tool can leave them out entirely
// without a branch at each call site.
func TestNilMetricsIsSafe(t *testing.T) {
	var m *Metrics

	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("a nil *Metrics panicked: %v", p)
		}
	}()

	m.SetReady(true)
	m.SetInfo("app", "test", "api")
	m.RecordDependency("opensearch", "search", 0, false)
	m.RecordDeadLetter("non_retryable")
	m.RecordDeadLetterFailure()
	m.BindWorkerPool(nil)

	// The middleware must pass requests through untouched.
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/search?q=go", nil))
	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want the handler's own %d", rec.Code, http.StatusTeapot)
	}
}

func TestMiddlewareCountsAndTimesRequests(t *testing.T) {
	m, reg := newTestMetrics(t)
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for range 3 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/search?q=kafka", nil))
	}

	labels := map[string]string{"method": "GET", "route": "/api/v1/search", "status": "200"}
	if got := gatheredValue(t, reg, "hybrid_search_http_requests_total", labels); got != 3 {
		t.Errorf("http_requests_total = %v, want 3", got)
	}
	if got := histogramCount(t, reg, "hybrid_search_http_request_duration_seconds",
		map[string]string{"method": "GET", "route": "/api/v1/search"}); got != 3 {
		t.Errorf("duration histogram recorded %d observations, want 3", got)
	}
	// In-flight is a gauge, so it has to come back to zero when the requests
	// are done, or it would climb forever and look like an overloaded server.
	if got := gatheredValue(t, reg, "hybrid_search_http_requests_in_flight", nil); got != 0 {
		t.Errorf("http_requests_in_flight = %v after the requests finished, want 0", got)
	}
}

// The query string must never reach a label, and an unknown path must not
// create a time series of its own: either would let a caller grow the metric
// without limit.
func TestMiddlewareKeepsRouteCardinalityBounded(t *testing.T) {
	m, reg := newTestMetrics(t)
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))

	paths := []string{
		"/api/v1/search?q=one", "/api/v1/search?q=two", // same route
		"/wp-login.php", "/../etc/passwd", "/random/deep/path", // all "other"
		"/health", "/ready", "/metrics",
	}
	for _, path := range paths {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	}

	routes := map[string]bool{}
	for _, family := range gather(t, reg) {
		if family.GetName() != "hybrid_search_http_requests_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "route" {
					routes[label.GetValue()] = true
				}
				if strings.Contains(label.GetValue(), "q=") || strings.Contains(label.GetValue(), "?") {
					t.Errorf("a label carries the query string: %s=%s", label.GetName(), label.GetValue())
				}
			}
		}
	}

	want := map[string]bool{"/api/v1/search": true, "/health": true, "/ready": true, "/metrics": true, routeOther: true}
	if len(routes) != len(want) {
		t.Errorf("routes = %v, want exactly %v", routes, want)
	}
	for route := range want {
		if !routes[route] {
			t.Errorf("route %q is missing from %v", route, routes)
		}
	}
}

func TestMiddlewareRecordsErrorClasses(t *testing.T) {
	tests := []struct {
		status int
		class  string
	}{
		{http.StatusOK, ""},
		{http.StatusBadRequest, "4xx"},
		{http.StatusInternalServerError, "5xx"},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			m, reg := newTestMetrics(t)
			handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
			}))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/search?q=go", nil))

			if tt.class == "" {
				if count := countSeries(t, reg, "hybrid_search_http_response_errors_total"); count != 0 {
					t.Errorf("a successful response was counted as an error")
				}
				return
			}
			labels := map[string]string{"route": "/api/v1/search", "class": tt.class}
			if got := gatheredValue(t, reg, "hybrid_search_http_response_errors_total", labels); got != 1 {
				t.Errorf("http_response_errors_total{class=%s} = %v, want 1", tt.class, got)
			}
		})
	}
}

// A handler that writes nothing has answered 200, and that is what the metric
// must say.
func TestMiddlewareCountsAnImplicitOK(t *testing.T) {
	m, reg := newTestMetrics(t)
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	labels := map[string]string{"method": "GET", "route": "/health", "status": "200"}
	if got := gatheredValue(t, reg, "hybrid_search_http_requests_total", labels); got != 1 {
		t.Errorf("http_requests_total{route=/health,status=200} = %v, want 1", got)
	}
}

// The endpoint has to answer in the Prometheus exposition format, and carry
// both the runtime metrics the client provides and this application's own.
func TestMetricsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := New(reg)

	// Something to report, so the application metric is not empty.
	m.searchRequests.WithLabelValues("success").Inc()

	srv := httptest.NewServer(Handler(reg, discardLogger()))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)

	for _, want := range []string{
		"go_goroutines",                       // from the Go collector
		"hybrid_search_search_requests_total", // this application's
		`hybrid_search_search_requests_total{status="success"} 1`,
		"# HELP hybrid_search_search_requests_total", // the exposition format
		"# TYPE hybrid_search_search_requests_total counter",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the metrics output does not contain %q", want)
		}
	}
	// Process metrics are not available on every platform, so their absence
	// is not a failure; their presence is checked only when they are there.
	if strings.Contains(text, "process_") && !strings.Contains(text, "# TYPE process_") {
		t.Error("process metrics are present but not described")
	}
}

func TestMetricsServerServesMetricsAndHealth(t *testing.T) {
	reg := prometheus.NewRegistry()
	New(reg)

	srv := NewServer(":0", "/metrics", reg, discardLogger())
	// The server is not started; its handler is what is being checked, so
	// nothing has to listen on a port.
	for path, wantStatus := range map[string]int{"/metrics": http.StatusOK, "/health": http.StatusOK, "/other": http.StatusNotFound} {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != wantStatus {
			t.Errorf("GET %s = %d, want %d", path, rec.Code, wantStatus)
		}
	}
}

func TestSetInfoAndReady(t *testing.T) {
	m, reg := newTestMetrics(t)

	m.SetInfo("near-realtime-search", "development", "consumer")
	labels := map[string]string{"application": "near-realtime-search", "environment": "development", "role": "consumer"}
	if got := gatheredValue(t, reg, "hybrid_search_application_info", labels); got != 1 {
		t.Errorf("application_info = %v, want 1", got)
	}

	for _, ready := range []bool{true, false, true} {
		m.SetReady(ready)
		want := 0.0
		if ready {
			want = 1
		}
		if got := gatheredValue(t, reg, "hybrid_search_application_ready", nil); got != want {
			t.Errorf("application_ready = %v after SetReady(%v), want %v", got, ready, want)
		}
	}
}
