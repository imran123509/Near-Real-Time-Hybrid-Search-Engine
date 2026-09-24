package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/search/hybrid"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
)

var discardLogger = slog.New(slog.DiscardHandler)

// fakeSearcher stands in for the hybrid search service.
type fakeSearcher struct {
	results []hybrid.Result
	err     error
	// block, if set, runs before the fake answers; returning an error makes
	// the search fail with it.
	block func(ctx context.Context) error

	mu   sync.Mutex
	reqs []hybrid.SearchRequest
	ctxs []context.Context
}

func (f *fakeSearcher) Search(ctx context.Context, req hybrid.SearchRequest) ([]hybrid.Result, error) {
	f.mu.Lock()
	f.reqs, f.ctxs = append(f.reqs, req), append(f.ctxs, ctx)
	f.mu.Unlock()
	if f.block != nil {
		if err := f.block(ctx); err != nil {
			return nil, err
		}
	}
	return f.results, f.err
}

func (f *fakeSearcher) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

func newTestRouter(s Searcher, checks ...ReadinessCheck) http.Handler {
	return NewRouter(NewSearchHandler(s, time.Second), NewReadinessHandler(discardLogger, checks...), discardLogger, Options{})
}

func serve(h http.Handler, method, target string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for k, values := range header {
		for _, v := range values {
			req.Header.Add(k, v) // canonicalizes k, as a real server does
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("body is not valid JSON: %v\n%s", err, rec.Body.String())
	}
	return v
}

func expectError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) ErrorBody {
	t.Helper()
	if rec.Code != status {
		t.Errorf("status = %d, want %d; body %s", rec.Code, status, rec.Body.String())
	}
	body := decode[ErrorResponse](t, rec).Error
	if body.Code != code {
		t.Errorf("error code = %q, want %q", body.Code, code)
	}
	return body
}

// Stand-ins for the retrievers, so tests can run the real hybrid service and
// check that its validation reaches the client as a 400.
type stubKeyword struct{ calls int }

func (s *stubKeyword) Search(context.Context, string, int) ([]opensearch.SearchResult, error) {
	s.calls++
	return []opensearch.SearchResult{{ID: "doc-1", Document: opensearch.Document{
		ID: "doc-1", Title: "Go concurrency", Content: "Goroutines and channels.", URL: "https://example.com/go",
	}}}, nil
}

type stubVectors struct{}

func (stubVectors) Search(context.Context, []float32, int) ([]qdrant.SearchResult, error) {
	return []qdrant.SearchResult{{ID: "doc-2", Payload: map[string]any{"title": "Go memory model", "url": "https://example.com/mm"}}}, nil
}

type stubEmbedder struct{}

func (stubEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{0.1, 0.2}, nil
}

func newRealService(t *testing.T, kw *stubKeyword) *hybrid.Service {
	t.Helper()
	svc, err := hybrid.New(kw, stubVectors{}, stubEmbedder{},
		config.SearchConfig{DefaultLimit: 10, MaxLimit: 50, CandidateLimit: 50, RRFK: 60}, discardLogger)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestSearchSuccess(t *testing.T) {
	kw := &stubKeyword{}
	rec := serve(newTestRouter(newRealService(t, kw)), http.MethodGet, "/api/v1/search?q=golang", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rec.Code, rec.Body.String())
	}
	resp := decode[SearchResponse](t, rec)
	if resp.Query != "golang" || resp.Total != 2 || len(resp.Results) != 2 {
		t.Fatalf("response = %+v, want query golang with 2 results", resp)
	}
	first := resp.Results[0]
	if first.ID != "doc-1" || first.Title != "Go concurrency" || first.Content != "Goroutines and channels." ||
		first.URL != "https://example.com/go" || first.Score <= 0 {
		t.Errorf("first result = %+v", first)
	}
	if second := resp.Results[1]; second.ID != "doc-2" || second.Title != "Go memory model" || second.Content != "" {
		t.Errorf("second result = %+v", second)
	}
}

// TestSearchResponseFieldNames pins the JSON field names, which clients rely on.
func TestSearchResponseFieldNames(t *testing.T) {
	s := &fakeSearcher{results: []hybrid.Result{{ID: "a", Title: "t", Content: "c", URL: "u", Score: 0.5}}}
	rec := serve(newTestRouter(s), http.MethodGet, "/api/v1/search?q=go", nil)

	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"query", "results", "total"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("response is missing %q: %s", key, rec.Body.String())
		}
	}
	result := raw["results"].([]any)[0].(map[string]any)
	for _, key := range []string{"id", "title", "content", "url", "score"} {
		if _, ok := result[key]; !ok {
			t.Errorf("result is missing %q: %s", key, rec.Body.String())
		}
	}

	// No results is an empty list, not null.
	empty := serve(newTestRouter(&fakeSearcher{}), http.MethodGet, "/api/v1/search?q=go", nil)
	if !strings.Contains(empty.Body.String(), `"results":[]`) {
		t.Errorf("empty response = %s, want \"results\":[]", empty.Body.String())
	}
}

func TestSearchPassesParametersToService(t *testing.T) {
	s := &fakeSearcher{}
	serve(newTestRouter(s), http.MethodGet, "/api/v1/search?q=distributed+systems&limit=5", nil)
	if s.calls() != 1 || s.reqs[0] != (hybrid.SearchRequest{Query: "distributed systems", Limit: 5}) {
		t.Fatalf("service got %+v", s.reqs)
	}

	serve(newTestRouter(s), http.MethodGet, "/api/v1/search?q=kafka", nil)
	if s.reqs[1].Limit != 0 {
		t.Errorf("limit = %d without a limit parameter, want 0 (the service default)", s.reqs[1].Limit)
	}
}

func TestSearchValidation(t *testing.T) {
	tests := []struct {
		name, target, wantMessage string
	}{
		{"missing q", "/api/v1/search", "query parameter q is required"},
		{"empty q", "/api/v1/search?q=", "query is empty"},
		{"whitespace q", "/api/v1/search?q=%20%20%09", "query is empty"},
		{"negative limit", "/api/v1/search?q=golang&limit=-1", "limit must not be negative"},
		{"limit above maximum", "/api/v1/search?q=golang&limit=51", "exceeds the maximum of 50"},
		{"non-numeric limit", "/api/v1/search?q=golang&limit=ten", "limit must be a whole number"},
		{"fractional limit", "/api/v1/search?q=golang&limit=1.5", "limit must be a whole number"},
		{"overflowing limit", "/api/v1/search?q=golang&limit=99999999999999999999", "limit must be a whole number"},
		{"malformed query string", "/api/v1/search?q=%zz", "query string is malformed"},
		{"query too long", "/api/v1/search?q=" + strings.Repeat("a", hybrid.MaxQueryLength+1), "longer than"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kw := &stubKeyword{}
			rec := serve(newTestRouter(newRealService(t, kw)), http.MethodGet, tt.target, nil)

			body := expectError(t, rec, http.StatusBadRequest, CodeInvalidRequest)
			if !strings.Contains(body.Message, tt.wantMessage) {
				t.Errorf("message = %q, want it to contain %q", body.Message, tt.wantMessage)
			}
			if kw.calls != 0 {
				t.Error("the keyword index was searched for an invalid request")
			}
		})
	}

	// The maximum itself is allowed.
	rec := serve(newTestRouter(newRealService(t, &stubKeyword{})), http.MethodGet, "/api/v1/search?q=golang&limit=50", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("limit=50: status %d, want 200", rec.Code)
	}
}

func TestSearchServiceFailure(t *testing.T) {
	// A realistic dependency error: it names an internal host and embeds a credential.
	s := &fakeSearcher{err: errors.New("keyword_search: dial tcp 10.0.3.7:9200: user admin:s3cret refused")}
	rec := serve(newTestRouter(s), http.MethodGet, "/api/v1/search?q=golang", nil)

	body := expectError(t, rec, http.StatusInternalServerError, CodeInternal)
	for _, leak := range []string{"10.0.3.7", "9200", "s3cret", "dial tcp", "keyword_search"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("error response leaks %q: %s", leak, rec.Body.String())
		}
	}
	if body.Message == "" {
		t.Error("error response has no message")
	}
}

type ctxMarker struct{}

func TestSearchUsesRequestContext(t *testing.T) {
	s := &fakeSearcher{}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=golang", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxMarker{}, "from the request"))
	newTestRouter(s).ServeHTTP(httptest.NewRecorder(), req)

	ctx := s.ctxs[0]
	if ctx.Value(ctxMarker{}) != "from the request" {
		t.Error("the service's context does not descend from the request context")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Error("the service's context has no deadline; the search timeout is not applied")
	}
	if RequestIDFromContext(ctx) == "" {
		t.Error("the service's context carries no request ID")
	}
}

func TestSearchClientCancellation(t *testing.T) {
	started := make(chan struct{})
	var sawCancel bool
	s := &fakeSearcher{block: func(ctx context.Context) error {
		close(started)
		select {
		case <-ctx.Done():
			// Canceled, not DeadlineExceeded: the client's disconnect must
			// reach the search, not merely the server-side timeout.
			sawCancel = errors.Is(ctx.Err(), context.Canceled)
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("never cancelled")
		}
	}}

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=golang", nil).WithContext(ctx)
	go func() {
		<-started
		cancel() // the client disconnects mid-search
	}()
	rec := httptest.NewRecorder()
	newTestRouter(s).ServeHTTP(rec, req)

	if !sawCancel {
		t.Error("the search was not cancelled when the client went away")
	}
	expectError(t, rec, statusClientClosedRequest, CodeRequestCancelled)
}

func TestSearchTimeout(t *testing.T) {
	s := &fakeSearcher{block: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	h := NewRouter(NewSearchHandler(s, 20*time.Millisecond), NewReadinessHandler(discardLogger), discardLogger, Options{})

	rec := serve(h, http.MethodGet, "/api/v1/search?q=golang", nil)
	expectError(t, rec, http.StatusGatewayTimeout, CodeTimeout)
}

func TestHealth(t *testing.T) {
	rec := serve(newTestRouter(&fakeSearcher{}), http.MethodGet, "/health", nil)
	if rec.Code != http.StatusOK || decode[statusResponse](t, rec).Status != "ok" {
		t.Errorf("GET /health = %d %s, want 200 {\"status\":\"ok\"}", rec.Code, rec.Body.String())
	}
}

func TestReady(t *testing.T) {
	ok := func(context.Context) error { return nil }
	down := func(context.Context) error { return errors.New("dial tcp 10.0.3.7:6334: connection refused") }

	t.Run("all dependencies ready", func(t *testing.T) {
		var gotDeadline bool
		check := func(ctx context.Context) error {
			_, gotDeadline = ctx.Deadline()
			return nil
		}
		rec := serve(newTestRouter(&fakeSearcher{}, ReadinessCheck{"opensearch", check}, ReadinessCheck{"qdrant", ok}),
			http.MethodGet, "/ready", nil)
		if rec.Code != http.StatusOK || decode[statusResponse](t, rec).Status != "ready" {
			t.Errorf("GET /ready = %d %s, want 200 ready", rec.Code, rec.Body.String())
		}
		if !gotDeadline {
			t.Error("readiness check ran without a deadline")
		}
	})

	t.Run("a dependency is down", func(t *testing.T) {
		rec := serve(newTestRouter(&fakeSearcher{}, ReadinessCheck{"opensearch", ok}, ReadinessCheck{"qdrant", down}),
			http.MethodGet, "/ready", nil)
		if rec.Code != http.StatusServiceUnavailable || decode[statusResponse](t, rec).Status != "not_ready" {
			t.Errorf("GET /ready = %d %s, want 503 not_ready", rec.Code, rec.Body.String())
		}
		for _, leak := range []string{"qdrant", "10.0.3.7", "refused"} {
			if strings.Contains(rec.Body.String(), leak) {
				t.Errorf("readiness response leaks %q: %s", leak, rec.Body.String())
			}
		}
	})

	t.Run("a hung dependency fails the probe instead of hanging it", func(t *testing.T) {
		hung := func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}
		ready := NewReadinessHandler(discardLogger, ReadinessCheck{"qdrant", hung})
		ready.timeout = 20 * time.Millisecond
		h := NewRouter(NewSearchHandler(&fakeSearcher{}, time.Second), ready, discardLogger, Options{})

		start := time.Now()
		rec := serve(h, http.MethodGet, "/ready", nil)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", rec.Code)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("probe took %s; the readiness timeout was not applied", elapsed)
		}
	})
}

func TestEveryResponseIsJSON(t *testing.T) {
	h := newTestRouter(&fakeSearcher{err: errors.New("boom")})
	tests := []struct {
		name, method, target string
		status               int
	}{
		{"validation error", http.MethodGet, "/api/v1/search", http.StatusBadRequest},
		{"service error", http.MethodGet, "/api/v1/search?q=golang", http.StatusInternalServerError},
		{"unknown path", http.MethodGet, "/nope", http.StatusNotFound},
		{"trailing slash", http.MethodGet, "/api/v1/search/", http.StatusNotFound},
		{"unversioned path", http.MethodGet, "/api/search?q=golang", http.StatusNotFound},
		{"wrong method", http.MethodPost, "/api/v1/search?q=golang", http.StatusMethodNotAllowed},
		{"health", http.MethodGet, "/health", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := serve(h, tt.method, tt.target, nil)
			if rec.Code != tt.status {
				t.Errorf("status = %d, want %d", rec.Code, tt.status)
			}
			decode[map[string]any](t, rec)
		})
	}

	rec := serve(h, http.MethodDelete, "/health", nil)
	if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Errorf("Allow = %q, want \"GET, HEAD\"", allow)
	}
}

// The metrics endpoint is optional and lives on the same server as everything
// else, behind the same middleware, so that one port and one shutdown cover
// the whole API.
func TestMetricsEndpointIsOptional(t *testing.T) {
	t.Run("absent unless a handler is given", func(t *testing.T) {
		h := NewRouter(NewSearchHandler(&fakeSearcher{}, time.Second), NewReadinessHandler(discardLogger), discardLogger, Options{})
		if rec := serve(h, http.MethodGet, "/metrics", nil); rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404 when metrics are switched off", rec.Code)
		}
	})

	t.Run("served at the configured path", func(t *testing.T) {
		exposition := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("# HELP something_total help\n"))
		})
		h := NewRouter(NewSearchHandler(&fakeSearcher{}, time.Second), NewReadinessHandler(discardLogger), discardLogger,
			Options{Metrics: exposition, MetricsPath: "/metrics"})

		rec := serve(h, http.MethodGet, "/metrics", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if body := rec.Body.String(); !strings.Contains(body, "# HELP") {
			t.Errorf("body = %q, want the exposition format untouched", body)
		}
		// Writing metrics is not something a client does.
		if rec := serve(h, http.MethodPost, "/metrics", nil); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST /metrics = %d, want 405", rec.Code)
		}
	})

	t.Run("middleware wraps every route", func(t *testing.T) {
		var seen []string
		h := NewRouter(NewSearchHandler(&fakeSearcher{}, time.Second), NewReadinessHandler(discardLogger), discardLogger,
			Options{
				Metrics:     http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
				MetricsPath: "/metrics",
				Middleware: func(next http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						seen = append(seen, r.URL.Path)
						next.ServeHTTP(w, r)
					})
				},
			})

		for _, path := range []string{"/api/v1/search?q=go", "/health", "/ready", "/metrics", "/nothing"} {
			serve(h, http.MethodGet, path, nil)
		}
		if len(seen) != 5 {
			t.Errorf("middleware saw %v, want all five requests", seen)
		}
	})
}

// Readiness is reported to whoever is watching, whether the probe came from a
// client or from the metrics sampler, so that the gauge and the endpoint can
// never disagree.
func TestReadinessIsObservable(t *testing.T) {
	failing := func(context.Context) error { return errors.New("unavailable") }
	ready := NewReadinessHandler(discardLogger, ReadinessCheck{"opensearch", failing})

	var results []bool
	ready.Observe(func(ok bool) { results = append(results, ok) })

	h := NewRouter(NewSearchHandler(&fakeSearcher{}, time.Second), ready, discardLogger, Options{})
	if rec := serve(h, http.MethodGet, "/ready", nil); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if ready.Check(context.Background()) {
		t.Error("Check reported ready while the endpoint answered 503")
	}
	if len(results) != 2 || results[0] || results[1] {
		t.Errorf("observed %v, want two failing checks", results)
	}
}
