// Package startup waits for a service's dependencies while it starts.
//
// In Docker Compose or Kubernetes, dependencies come up in no particular
// order, and a container that has started is not yet a database that accepts
// connections. Retry lets a service wait for them for a bounded time instead
// of failing on the first refused connection, or waiting forever.
package startup

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Backoff between attempts: 500ms, 1s, 2s, 4s, then every 5s.
const (
	initialDelay = 500 * time.Millisecond
	maxDelay     = 5 * time.Second
	// attemptTimeout bounds one attempt, so a connection that hangs is
	// abandoned and retried rather than using up the whole startup deadline.
	attemptTimeout = 10 * time.Second
)

// Retry calls attempt until it succeeds or ctx ends, and returns the last
// error if ctx ends first. Give ctx the startup deadline: that deadline, not a
// count of attempts, is what stops the retries.
//
// Every failed attempt is logged at warning level straight away, so a
// misconfiguration such as a wrong password is visible in the log long before
// the deadline passes.
func Retry(ctx context.Context, logger *slog.Logger, dependency string, attempt func(ctx context.Context) error) error {
	delay := initialDelay
	for n := 1; ; n++ {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := attempt(attemptCtx)
		cancel()
		if err == nil {
			if n > 1 {
				logger.InfoContext(ctx, "dependency ready", "dependency", dependency, "attempts", n)
			}
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s not ready before the startup deadline after %d attempts: %w", dependency, n, err)
		}

		logger.WarnContext(ctx, "waiting for dependency",
			"dependency", dependency, "attempt", n, "retry_in", delay, "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not ready before the startup deadline after %d attempts: %w", dependency, n, err)
		case <-time.After(delay):
		}
		delay = min(delay*2, maxDelay)
	}
}
