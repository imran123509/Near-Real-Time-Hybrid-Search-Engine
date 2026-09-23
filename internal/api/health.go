package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// defaultReadinessTimeout bounds all readiness checks together, so a hung
// dependency makes the probe fail instead of hang.
const defaultReadinessTimeout = 2 * time.Second

// Health handles GET /health. It reports that the process is up and checks
// nothing else, so it stays cheap enough for frequent liveness probes.
func Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

// ReadinessCheck is one dependency the API needs before it can serve
// searches. Check must be cheap, such as a ping, and must honour ctx.
type ReadinessCheck struct {
	// Name identifies the dependency in logs. It never appears in responses.
	Name  string
	Check func(ctx context.Context) error
}

// ReadinessHandler serves GET /ready.
type ReadinessHandler struct {
	checks  []ReadinessCheck
	timeout time.Duration
	logger  *slog.Logger
}

// NewReadinessHandler returns a handler that runs checks on every request.
func NewReadinessHandler(logger *slog.Logger, checks ...ReadinessCheck) *ReadinessHandler {
	return &ReadinessHandler{checks: checks, timeout: defaultReadinessTimeout, logger: logger}
}

// Ready runs every check at once and answers 200 when all pass or 503 when
// any fails. The body is only {"status": ...}: which dependency failed, and
// why, goes to the log rather than to whoever is probing.
func (h *ReadinessHandler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	errs := make([]error, len(h.checks))
	var wg sync.WaitGroup
	for i, c := range h.checks {
		wg.Go(func() { errs[i] = c.Check(ctx) })
	}
	wg.Wait()

	ready := true
	for i, err := range errs {
		if err != nil {
			ready = false
			h.logger.LogAttrs(r.Context(), slog.LevelWarn, "readiness check failed",
				slog.String("check", h.checks[i].Name), slog.Any("error", err))
		}
	}
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, statusResponse{Status: "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, statusResponse{Status: "ready"})
}
