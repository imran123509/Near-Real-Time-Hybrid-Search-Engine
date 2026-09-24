package metrics

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// routeOther is the route label for anything that is not one of the API's
// endpoints, which is what keeps a flood of unknown paths from creating a
// time series each.
const routeOther = "other"

// knownRoutes are the only route label values that exist. The label is the
// route, never the request's path: two searches for different queries are the
// same route, and a request for /wp-login.php is "other".
var knownRoutes = map[string]string{
	"/api/v1/search": "/api/v1/search",
	"/health":        "/health",
	"/ready":         "/ready",
	"/metrics":       "/metrics",
}

// Middleware counts and times every HTTP request.
//
// It is the outermost layer that matters for measurement: it records what the
// client actually got, including responses written by the panic recovery
// middleware, and it never changes the response itself.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := routeLabel(r.URL.Path)

		m.httpInFlight.Inc()
		defer m.httpInFlight.Dec()

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start)

		code := rec.statusOrOK()
		m.httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(code)).Inc()
		m.httpDuration.WithLabelValues(r.Method, route).Observe(seconds(elapsed))
		if class := statusClass(code); class != "" {
			m.httpResponses.WithLabelValues(route, class).Inc()
		}
	})
}

// routeLabel maps a request path onto the bounded set of routes.
func routeLabel(path string) string {
	if route, ok := knownRoutes[path]; ok {
		return route
	}
	return routeOther
}

// statusClass groups error responses. Success needs no class: the status code
// label on http_requests_total already carries it.
func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	default:
		return ""
	}
}

// statusRecorder remembers the status a handler wrote. It mirrors the one in
// internal/api, which is unexported there; duplicating six lines is better
// than exporting a type from the API package so that metrics can reach it.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *statusRecorder) statusOrOK() int {
	if !r.wroteHeader {
		return http.StatusOK
	}
	return r.status
}

// Handler returns the endpoint Prometheus scrapes, in the standard exposition
// format. Errors while gathering are logged and answered with a 500 rather
// than written into the body as a half-valid response.
func Handler(gatherer prometheus.Gatherer, logger *slog.Logger) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		ErrorLog:          slogErrorLogger{logger},
		ErrorHandling:     promhttp.HTTPErrorOnError,
		EnableOpenMetrics: true,
	})
}

// slogErrorLogger adapts the application logger to what promhttp expects.
type slogErrorLogger struct{ logger *slog.Logger }

func (l slogErrorLogger) Println(v ...any) {
	if l.logger == nil {
		return
	}
	l.logger.Error("gathering metrics failed", "error", fmt.Sprint(v...))
}

// NewServer returns an HTTP server that serves only the metrics endpoint.
//
// The API serves /metrics from its own router, beside /health and /ready, so
// it needs none of this. The consumer has no HTTP server at all, and its
// metrics — Kafka, retries, the dead-letter topic, the worker pool — are the
// ones worth watching most, so it gets this small listener rather than going
// unmeasured. It shares no state with the consumer's work, so a scrape cannot
// slow down or block message processing.
func NewServer(addr, path string, gatherer prometheus.Gatherer, logger *slog.Logger) *http.Server {
	if path == "" {
		path = "/metrics"
	}
	mux := http.NewServeMux()
	mux.Handle(path, Handler(gatherer, logger))
	// A liveness endpoint costs nothing here and gives the consumer container
	// something to answer, which it otherwise has no way to do.
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}
