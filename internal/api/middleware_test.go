package api

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"near-real-time-hybrid-search-engine/internal/search/hybrid"
)

func TestRequestID(t *testing.T) {
	t.Run("generated when missing", func(t *testing.T) {
		s := &fakeSearcher{}
		rec := serve(newTestRouter(s), http.MethodGet, "/api/v1/search?q=golang", nil)

		id := rec.Header().Get(RequestIDHeader)
		if _, err := uuid.Parse(id); err != nil {
			t.Fatalf("X-Request-ID = %q, want a generated UUID", id)
		}
		if got := RequestIDFromContext(s.ctxs[0]); got != id {
			t.Errorf("request ID in context = %q, want %q", got, id)
		}
	})

	t.Run("propagated when supplied", func(t *testing.T) {
		s := &fakeSearcher{}
		rec := serve(newTestRouter(s), http.MethodGet, "/api/v1/search?q=golang",
			http.Header{RequestIDHeader: {"client-req_42.a:b"}})

		if got := rec.Header().Get(RequestIDHeader); got != "client-req_42.a:b" {
			t.Errorf("X-Request-ID = %q, want the client's ID", got)
		}
		if got := RequestIDFromContext(s.ctxs[0]); got != "client-req_42.a:b" {
			t.Errorf("request ID in context = %q, want the client's ID", got)
		}
	})

	t.Run("replaced when malformed", func(t *testing.T) {
		for _, bad := range []string{"has space", "line\nbreak", `quote"d`, strings.Repeat("x", maxRequestIDLength+1)} {
			rec := serve(newTestRouter(&fakeSearcher{}), http.MethodGet, "/health", http.Header{RequestIDHeader: {bad}})
			if got := rec.Header().Get(RequestIDHeader); got == bad || uuid.Validate(got) != nil {
				t.Errorf("supplied %q: X-Request-ID = %q, want a generated UUID", bad, got)
			}
		}
	})

	t.Run("present on error responses", func(t *testing.T) {
		rec := serve(newTestRouter(&fakeSearcher{}), http.MethodGet, "/nope", nil)
		if rec.Header().Get(RequestIDHeader) == "" {
			t.Error("404 response has no X-Request-ID")
		}
	})
}

func TestAccessLog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewTextHandler(&buf, nil)))
	s := &fakeSearcher{results: []hybrid.Result{{ID: "a"}}}
	h := NewRouter(NewSearchHandler(s, time.Second), NewReadinessHandler(logger), logger)

	rec := serve(h, http.MethodGet, "/api/v1/search?q=patient+record+4411", http.Header{RequestIDHeader: {"req-1"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	logged := buf.String()
	for _, want := range []string{"http request", "request_id=req-1", "method=GET", "path=/api/v1/search", "status=200", "duration="} {
		if !strings.Contains(logged, want) {
			t.Errorf("access log is missing %q: %s", want, logged)
		}
	}
	if strings.Contains(logged, "patient") || strings.Contains(logged, "4411") {
		t.Errorf("access log contains the search query: %s", logged)
	}
}

func TestPanicRecovery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewTextHandler(&buf, nil)))
	s := &fakeSearcher{block: func(context.Context) error { panic("nil map write") }}
	h := NewRouter(NewSearchHandler(s, time.Second), NewReadinessHandler(logger), logger)

	rec := serve(h, http.MethodGet, "/api/v1/search?q=golang", http.Header{RequestIDHeader: {"req-panic"}})

	body := expectError(t, rec, http.StatusInternalServerError, CodeInternal)
	if strings.Contains(rec.Body.String(), "nil map write") || strings.Contains(rec.Body.String(), "goroutine") {
		t.Errorf("response leaks the panic: %s", rec.Body.String())
	}
	if body.Message != "internal error" {
		t.Errorf("message = %q", body.Message)
	}
	logged := buf.String()
	for _, want := range []string{"panic while handling request", "nil map write", "request_id=req-panic", "status=500"} {
		if !strings.Contains(logged, want) {
			t.Errorf("log is missing %q: %s", want, logged)
		}
	}
}

func TestAbortHandlerPanicPropagates(t *testing.T) {
	h := withRecovery(discardLogger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer func() {
		if p := recover(); p != http.ErrAbortHandler {
			t.Errorf("recovered %v, want http.ErrAbortHandler to propagate", p)
		}
	}()
	serve(h, http.MethodGet, "/", nil)
}

func TestLogHandlerAddsRequestID(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewTextHandler(&buf, nil))).With("component", "search")

	ctx := context.WithValue(context.Background(), requestIDKey{}, "req-9")
	logger.InfoContext(ctx, "hybrid search failed")
	logger.Info("startup") // no request context, no ID

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if !strings.Contains(lines[0], "request_id=req-9") || !strings.Contains(lines[0], "component=search") {
		t.Errorf("first line = %s, want request_id and the logger's attributes", lines[0])
	}
	if strings.Contains(lines[1], "request_id") {
		t.Errorf("line logged without a request carries a request ID: %s", lines[1])
	}
}
