package api

import (
	"log/slog"
	"net/http"
)

// RegisterRoutes adds the API's endpoints to mux:
//
//	GET /api/v1/search   hybrid search
//	GET /health          liveness: the process is up
//	GET /ready           readiness: the dependencies answer
//
// Every other path gets a JSON 404, and every other method a JSON 405.
// Patterns are registered without a method, with the method checked by
// allowMethods, because ServeMux's own 404 and 405 answers are plain text and
// every response from this API is JSON.
func RegisterRoutes(mux *http.ServeMux, search *SearchHandler, ready *ReadinessHandler) {
	mux.Handle("/api/v1/search", allowMethods(http.MethodGet, http.HandlerFunc(search.Search)))
	mux.Handle("/health", allowMethods(http.MethodGet, http.HandlerFunc(Health)))
	mux.Handle("/ready", allowMethods(http.MethodGet, http.HandlerFunc(ready.Ready)))
	mux.Handle("/", http.HandlerFunc(notFound))
}

// NewRouter returns the server's complete handler: the routes behind request
// ID, access logging and panic recovery middleware, in that order from the
// outside in, so that even a request that panics is logged with its ID.
func NewRouter(search *SearchHandler, ready *ReadinessHandler, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	RegisterRoutes(mux, search, ready)
	return withRequestID(withLogging(logger, withRecovery(logger, mux)))
}

// allowMethods answers requests with any other method with a JSON 405. GET
// also admits HEAD, as net/http does.
func allowMethods(method string, next http.Handler) http.Handler {
	allow := method
	if method == http.MethodGet {
		allow = "GET, HEAD"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method && !(method == http.MethodGet && r.Method == http.MethodHead) {
			w.Header().Set("Allow", allow)
			writeError(w, http.StatusMethodNotAllowed, CodeMethodNotAllowed, "method not allowed")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func notFound(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, CodeNotFound, "not found")
}
