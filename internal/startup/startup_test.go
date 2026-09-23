package startup

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

var discard = slog.New(slog.DiscardHandler)

func TestRetrySucceedsOnceTheDependencyIsUp(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), discard, "postgres", func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if calls != 3 {
		t.Errorf("attempts = %d, want 3", calls)
	}
}

func TestRetryStopsAtTheDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	start := time.Now()
	calls := 0
	err := Retry(ctx, discard, "opensearch", func(context.Context) error {
		calls++
		return errors.New("connection refused")
	})

	if err == nil || !strings.Contains(err.Error(), "opensearch not ready before the startup deadline") ||
		!strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err = %v, want the deadline error wrapping the last failure", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Retry ran for %s past a 700ms deadline", elapsed)
	}
	if calls < 2 {
		t.Errorf("attempts = %d, want at least 2 within the deadline", calls)
	}
}

func TestRetryBoundsEachAttempt(t *testing.T) {
	var deadline time.Time
	err := Retry(context.Background(), discard, "qdrant", func(ctx context.Context) error {
		deadline, _ = ctx.Deadline()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if deadline.IsZero() || time.Until(deadline) > attemptTimeout {
		t.Errorf("attempt deadline = %v, want one within %s", deadline, attemptTimeout)
	}
}

func TestRetryLogsEachFailure(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	calls := 0
	_ = Retry(context.Background(), logger, "kafka", func(context.Context) error {
		calls++
		if calls == 1 {
			return errors.New("unknown topic")
		}
		return nil
	})
	logged := buf.String()
	for _, want := range []string{"waiting for dependency", "dependency=kafka", "unknown topic", "dependency ready"} {
		if !strings.Contains(logged, want) {
			t.Errorf("log is missing %q: %s", want, logged)
		}
	}
}
