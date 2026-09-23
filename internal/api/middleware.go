package api

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

// RequestIDHeader carries the request ID in both directions.
const RequestIDHeader = "X-Request-ID"

// maxRequestIDLength caps a client-supplied ID, which ends up in every log
// line for the request.
const maxRequestIDLength = 128

type requestIDKey struct{}

// RequestIDFromContext returns the request ID stored by the request ID
// middleware, or "" outside a request.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// withRequestID gives every request an ID: the client's X-Request-ID if it is
// well formed, otherwise a new UUID. The ID goes into the request context and
// back to the client in the X-Request-ID response header.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !validRequestID(id) {
			id = uuid.NewString()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

// validRequestID accepts 1 to 128 letters, digits and - _ . : only, so a
// client-supplied ID cannot smuggle spaces, quotes or line breaks into logs.
func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLength {
		return false
	}
	for _, c := range []byte(id) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == ':':
		default:
			return false
		}
	}
	return true
}

// withLogging writes one access log line per request. It logs the path but
// never the query string, because a search query can hold personal data.
func withLogging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		level := slog.LevelInfo
		if rec.status >= http.StatusInternalServerError {
			level = slog.LevelError
		}
		logger.LogAttrs(r.Context(), level, "http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.statusOrOK()),
			slog.Int("bytes", rec.bytes),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

// withRecovery turns a panic in a handler into a 500 response and an error
// log with the stack trace, instead of a dropped connection. The stack trace
// goes to the log only, never to the client.
func withRecovery(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			p := recover()
			if p == nil {
				return
			}
			// ErrAbortHandler is net/http's way to abort a response on
			// purpose; it must keep propagating.
			if p == http.ErrAbortHandler {
				panic(p)
			}
			logger.LogAttrs(r.Context(), slog.LevelError, "panic while handling request",
				slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
			if !rec.wroteHeader {
				writeError(rec, http.StatusInternalServerError, CodeInternal, "internal error")
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

// statusRecorder remembers the status code and body size a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int
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
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// statusOrOK reports 200 for a handler that wrote nothing, as net/http does.
func (r *statusRecorder) statusOrOK() int {
	if !r.wroteHeader {
		return http.StatusOK
	}
	return r.status
}

// NewLogHandler wraps h so that every record logged with a request's context
// carries that request's ID as request_id. Wrap the application logger's
// handler with it, and every layer that logs with the request context, the
// search service included, is correlated with the access log without passing
// the ID around.
func NewLogHandler(h slog.Handler) slog.Handler {
	return requestIDLogHandler{h}
}

type requestIDLogHandler struct {
	slog.Handler
}

func (h requestIDLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestIDFromContext(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h requestIDLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return requestIDLogHandler{h.Handler.WithAttrs(attrs)}
}

func (h requestIDLogHandler) WithGroup(name string) slog.Handler {
	return requestIDLogHandler{h.Handler.WithGroup(name)}
}
