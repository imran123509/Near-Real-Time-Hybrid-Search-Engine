// Package metrics exposes what the system is doing as Prometheus metrics.
//
// It is observational: nothing here changes how a search is answered or how an
// event is indexed. The application packages know nothing about Prometheus;
// they offer small observation hooks, and the adapters in this package turn
// what those hooks report into metrics. That keeps the dependency pointing one
// way,
//
//	metrics -> application packages
//
// so instrumentation can be added, changed or left out without touching the
// code being measured, and so that every package's own tests still run without
// a metrics registry.
//
// # Cardinality
//
// Every label here has a small, fixed set of values: method, route, status,
// operation, dependency, provider, outcome, error_type, reason. Nothing that
// varies per request or per document — no query text, no document or event ID,
// no Kafka offset, no error message — is ever a label. One unbounded label is
// enough to turn a metric into millions of time series and take Prometheus
// down with it.
//
// # Safety
//
// A nil *Metrics is usable: every method on it does nothing. That is what lets
// a component be built without metrics in a test and with them in production,
// without a branch at every call site. Recording is a few atomic operations
// and never blocks, fails or returns an error, because a metric must not be
// able to break the request it is measuring.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Namespace prefixes every metric this application defines, so its series can
// be told apart from the Go runtime's and from anything else scraped by the
// same Prometheus.
const Namespace = "hybrid_search"

// Bucket sets. Each operation gets buckets that cover what it actually costs:
// the same buckets everywhere would put every HTTP request in one bucket and
// every indexing event in another, and neither would produce a usable
// percentile.
var (
	// httpBuckets span a fast local search to a request that times out.
	httpBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	// dependencyBuckets are for one call to OpenSearch, Qdrant or an
	// embedding provider. The top of the range covers a provider that is
	// rate-limiting or a store that is struggling.
	dependencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	// indexingBuckets cover one event through the whole pipeline, retries and
	// their backoff included, which is why they reach much further than the
	// HTTP ones.
	indexingBuckets = []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
	// retryBuckets match the configured backoff: 500ms, 1s, 2s, 4s, ... 30s.
	retryBuckets = []float64{0.1, 0.5, 1, 2, 4, 8, 16, 30, 60}
)

// Metrics is every metric the application reports. One instance is built at
// startup and handed to the components that report into it.
type Metrics struct {
	// HTTP
	httpRequests  *prometheus.CounterVec
	httpDuration  *prometheus.HistogramVec
	httpInFlight  prometheus.Gauge
	httpResponses *prometheus.CounterVec

	// Search
	searchRequests *prometheus.CounterVec
	searchDuration *prometheus.HistogramVec
	searchErrors   *prometheus.CounterVec
	searchResults  prometheus.Histogram

	// Dependencies: OpenSearch, Qdrant and the embedding provider, whether
	// they were called while searching or while indexing.
	dependencyRequests *prometheus.CounterVec
	dependencyDuration *prometheus.HistogramVec
	dependencyErrors   *prometheus.CounterVec

	// Kafka
	kafkaMessages   *prometheus.CounterVec
	kafkaProcessing *prometheus.HistogramVec
	kafkaCommits    prometheus.Counter
	kafkaErrors     *prometheus.CounterVec

	// Retry and dead letters
	retries        *prometheus.CounterVec
	retryExhausted *prometheus.CounterVec
	retryDelay     prometheus.Histogram
	dlqMessages    *prometheus.CounterVec
	dlqErrors      prometheus.Counter

	// Indexing
	indexingEvents   *prometheus.CounterVec
	indexingDuration *prometheus.HistogramVec
	indexingErrors   *prometheus.CounterVec

	// Worker pool. These are gauges read at scrape time, so they cost nothing
	// while messages are being processed; see BindWorkerPool.
	registerer prometheus.Registerer

	// Application
	ready prometheus.Gauge
	info  *prometheus.GaugeVec
}

// New builds the metrics and registers them with reg.
//
// It is called once, at startup, with prometheus.DefaultRegisterer, which
// already carries the Go runtime and process collectors. Tests pass their own
// registry, so each one starts from zero and no test can register the same
// metric twice.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		registerer: reg,

		httpRequests: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "http_requests_total",
			Help: "HTTP requests answered, by method, route and status code.",
		}, []string{"method", "route", "status"}),
		httpDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "http_request_duration_seconds",
			Help:    "Time to answer an HTTP request, in seconds.",
			Buckets: httpBuckets,
		}, []string{"method", "route"}),
		httpInFlight: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "http_requests_in_flight",
			Help: "HTTP requests being handled right now. A number that keeps climbing means the server is not keeping up.",
		}),
		httpResponses: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "http_response_errors_total",
			Help: "HTTP responses with a 4xx or 5xx status, by route and class.",
		}, []string{"route", "class"}),

		searchRequests: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "search_requests_total",
			Help: "Hybrid searches run, by outcome.",
		}, []string{"status"}),
		searchDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "search_duration_seconds",
			Help:    "Time for one hybrid search, from request to fused results.",
			Buckets: httpBuckets,
		}, []string{"status"}),
		searchErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "search_errors_total",
			Help: "Hybrid searches that failed, by the stage that failed.",
		}, []string{"stage"}),
		searchResults: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "search_results_returned",
			Help:    "Results returned per successful search. Zero results are not an error but are worth watching.",
			Buckets: []float64{0, 1, 5, 10, 20, 50},
		}),

		dependencyRequests: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "dependency_requests_total",
			Help: "Calls to OpenSearch, Qdrant and the embedding provider, by dependency, operation and outcome.",
		}, []string{"dependency", "operation", "status"}),
		dependencyDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "dependency_duration_seconds",
			Help:    "Time for one call to a dependency, in seconds.",
			Buckets: dependencyBuckets,
		}, []string{"dependency", "operation"}),
		dependencyErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "dependency_errors_total",
			Help: "Failed calls to a dependency, by dependency and operation.",
		}, []string{"dependency", "operation"}),

		kafkaMessages: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "kafka_messages_total",
			Help: "Kafka messages by what happened to them: consumed, indexed, skipped, dead-lettered or failed.",
		}, []string{"topic", "outcome"}),
		kafkaProcessing: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "kafka_message_processing_duration_seconds",
			Help: "Time from a message being handed to the pipeline until it is finished, in seconds. " +
				"It does not include how long the message waited in Kafka.",
			Buckets: indexingBuckets,
		}, []string{"topic"}),
		kafkaCommits: factory.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "kafka_offset_commits_total",
			Help: "Offset commits the consumer made. Processing that succeeds but never commits shows up as this lagging behind the processed count.",
		}),
		kafkaErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "kafka_consumer_errors_total",
			Help: "Consumer errors by kind: fetch, process or commit. The error text is logged, never labelled.",
		}, []string{"type"}),

		retries: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "kafka_retries_total",
			Help: "Retried processing attempts, by how the failure was classified.",
		}, []string{"error_type"}),
		retryExhausted: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "kafka_retry_exhausted_total",
			Help: "Messages that used every attempt and were given up on, by error type.",
		}, []string{"error_type"}),
		retryDelay: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "kafka_retry_delay_seconds",
			Help:    "Backoff waited before a retry, in seconds.",
			Buckets: retryBuckets,
		}),
		dlqMessages: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "kafka_dlq_messages_total",
			Help: "Messages stored in the dead-letter topic, by why they were given up on.",
		}, []string{"reason"}),
		dlqErrors: factory.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "kafka_dlq_publish_errors_total",
			Help: "Failed dead-letter publishes. These messages are not committed and will be delivered again, " +
				"so this counter rising means the consumer is stuck rather than losing data.",
		}),

		indexingEvents: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "indexing_events_total",
			Help: "Change events applied to the search indexes, by operation and effect.",
		}, []string{"operation", "effect"}),
		indexingDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "indexing_processing_duration_seconds",
			Help:    "Time to apply one change event to both indexes, in seconds.",
			Buckets: indexingBuckets,
		}, []string{"operation"}),
		indexingErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Name: "indexing_dependency_errors_total",
			Help: "Indexing failures by which dependency failed, which is what tells a partial write apart from a rejected event.",
		}, []string{"dependency"}),

		ready: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "application_ready",
			Help: "1 when every dependency answered its readiness check, 0 when one did not.",
		}),
		info: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "application_info",
			Help: "Always 1. Its labels describe the running service: name, environment and role.",
		}, []string{"application", "environment", "role"}),
	}
}

// SetInfo records what this process is. The labels are fixed for the lifetime
// of the process and hold no configuration values or secrets.
func (m *Metrics) SetInfo(application, environment, role string) {
	if m == nil {
		return
	}
	m.info.WithLabelValues(application, environment, role).Set(1)
}

// SetReady records the result of a readiness check.
func (m *Metrics) SetReady(ready bool) {
	if m == nil {
		return
	}
	m.ready.Set(boolValue(ready))
}

// seconds converts a duration for a histogram. Prometheus measures time in
// seconds; using milliseconds anywhere would make the bucket boundaries and
// the metric name disagree.
func seconds(d time.Duration) float64 { return d.Seconds() }

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// status is the bounded outcome label used wherever something can fail.
func status(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}
