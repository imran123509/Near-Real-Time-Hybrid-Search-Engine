package indexing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/kafka"
)

func newTestPool(t *testing.T, workers, queueSize int, h Handler, onFatal func(error)) *WorkerPool {
	t.Helper()
	if onFatal == nil {
		onFatal = func(err error) { t.Errorf("unexpected fatal error: %v", err) }
	}
	p, err := NewWorkerPool(workers, queueSize, h, onFatal, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	p.Start(context.Background())
	return p
}

func submit(t *testing.T, p *WorkerPool, key string, offset int64) {
	t.Helper()
	if err := p.Submit(context.Background(), kafka.Message{Key: []byte(key), Offset: offset}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
}

func TestWorkerPoolBoundsConcurrency(t *testing.T) {
	const workers, messages = 3, 30
	var active, peak atomic.Int32
	var finished sync.WaitGroup
	finished.Add(messages)

	p := newTestPool(t, workers, 1, func(context.Context, kafka.Message) error {
		defer finished.Done()
		n := active.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		return nil
	}, nil)

	for i := range messages {
		submit(t, p, fmt.Sprintf("doc-%d", i), int64(i))
	}
	finished.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := peak.Load(); got > workers {
		t.Fatalf("peak concurrency = %d, want at most %d", got, workers)
	}
}

func TestWorkerPoolKeepsOrderPerKey(t *testing.T) {
	const perKey = 50
	keys := []string{"doc-a", "doc-b", "doc-c"}
	var mu sync.Mutex
	seen := map[string][]int64{}
	var finished sync.WaitGroup
	finished.Add(perKey * len(keys))

	p := newTestPool(t, 4, 2, func(_ context.Context, msg kafka.Message) error {
		defer finished.Done()
		mu.Lock()
		seen[string(msg.Key)] = append(seen[string(msg.Key)], msg.Offset)
		mu.Unlock()
		return nil
	}, nil)

	for i := range perKey {
		for _, k := range keys {
			submit(t, p, k, int64(i))
		}
	}
	finished.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		for i, off := range seen[k] {
			if off != int64(i) {
				t.Fatalf("key %s processed out of order: %v", k, seen[k])
			}
		}
	}
}

func TestWorkerPoolShutdownFinishesInFlightAndSkipsQueued(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32

	p := newTestPool(t, 1, 3, func(context.Context, kafka.Message) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}, nil)

	submit(t, p, "doc", 0)
	<-started
	for off := int64(1); off <= 3; off++ {
		submit(t, p, "doc", off) // fills the queue
	}

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- p.Shutdown(context.Background()) }()
	waitUntilStopping(t, p)

	select {
	case err := <-shutdownErr:
		t.Fatalf("Shutdown returned %v before the in-flight message finished", err)
	default:
	}
	close(release)
	if err := <-shutdownErr; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1 (queued messages must not start during shutdown)", got)
	}
}

func TestWorkerPoolShutdownDeadlineCancelsHandlers(t *testing.T) {
	started := make(chan struct{})
	p := newTestPool(t, 1, 1, func(ctx context.Context, _ kafka.Message) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, nil)

	submit(t, p, "doc", 0)
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
	}
}

func TestWorkerPoolReportsFatalErrors(t *testing.T) {
	want := errors.New("dead-letter publish failed")
	fatal := make(chan error, 1)

	p := newTestPool(t, 2, 1, func(context.Context, kafka.Message) error {
		return want
	}, func(err error) { fatal <- err })

	submit(t, p, "doc", 0)
	select {
	case got := <-fatal:
		if !errors.Is(got, want) {
			t.Fatalf("onFatal got %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("onFatal was not called")
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// waitUntilStopping blocks until Shutdown has begun. The single worker's queue
// is full, so Submit only returns early once the pool is shutting down.
func waitUntilStopping(t *testing.T, p *WorkerPool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		err := p.Submit(ctx, kafka.Message{Key: []byte("doc")})
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if err == nil {
			t.Fatal("Submit succeeded on a full queue")
		}
	}
	t.Fatal("pool did not start shutting down")
}
