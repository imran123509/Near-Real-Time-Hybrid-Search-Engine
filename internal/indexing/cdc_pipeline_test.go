package indexing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/kafka"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
)

// These tests drive the whole change-data-capture path the way the consumer
// does: raw Debezium bytes go into a worker pool and end up in stand-ins for
// OpenSearch and Qdrant. Nothing here needs Kafka, Debezium, PostgreSQL,
// OpenSearch or Qdrant to be running.

const cdcTestID = "7f1c9b2e-4a3d-4e8b-9c1a-2b3c4d5e6f70"

// stores is a minimal in-memory pair of search indexes plus an embedder.
type stores struct {
	mu        sync.Mutex
	documents map[string]opensearch.Document
	points    map[string]qdrant.Point

	// upsertFailures is how many more vector writes must fail with
	// upsertErr; a negative value fails every one of them.
	upsertFailures int
	upsertErr      error
}

func newStores() *stores {
	return &stores{documents: map[string]opensearch.Document{}, points: map[string]qdrant.Point{}}
}

func (s *stores) IndexDocument(_ context.Context, doc opensearch.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.documents[doc.ID] = doc
	return nil
}

func (s *stores) DeleteDocument(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.documents, id)
	return nil
}

func (s *stores) Upsert(_ context.Context, p qdrant.Point) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upsertFailures != 0 {
		if s.upsertFailures > 0 {
			s.upsertFailures--
		}
		return s.upsertErr
	}
	s.points[p.ID] = p
	return nil
}

func (s *stores) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.points, id)
	return nil
}

func (s *stores) Embed(context.Context, string) ([]float32, error) {
	return []float32{0.5, 0.5}, nil
}

func (s *stores) document(id string) opensearch.Document {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.documents[id]
}

func (s *stores) counts() (documents, points int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.documents), len(s.points)
}

// failUpserts makes the next n vector writes fail with err. A negative n
// fails every write from now on.
func (s *stores) failUpserts(n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upsertFailures, s.upsertErr = n, err
}

// newCDCTestPipeline returns a pipeline over st with backoff removed, so the
// tests do not spend real time sleeping between retries.
func newCDCTestPipeline(t *testing.T, st *stores, dlq DeadLetterPublisher, maxAttempts int) *Pipeline[cdc.ChangeEvent] {
	t.Helper()
	service, err := cdc.NewService(st, st, cdc.EmbedderFunc(st.Embed), cdc.DefaultMapping())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	p := NewCDCPipeline(service, dlq, maxAttempts, slog.New(slog.DiscardHandler))
	p.backoff = func(int) time.Duration { return 0 }
	return p
}

// changeMessage builds the Kafka message Debezium would publish for one row
// change. The key is the document ID, which is what keeps every event for a
// document on the same worker.
func changeMessage(op, id string, version int, offset int64) kafka.Message {
	row := fmt.Sprintf(
		`{"id": %q, "title": "Title %s", "body": "Body for %s.", "version": %d, "updated_at": "2024-05-01T10:00:00Z"}`,
		id, id, id, version)
	before, after := "null", row
	if op == "d" {
		before, after = row, "null"
	}
	value := fmt.Sprintf(
		`{"before": %s, "after": %s, "op": %q, "ts_ms": 1714557600987,
		  "source": {"schema": "public", "table": "documents", "ts_ms": 1714557600000}}`,
		before, after, op)

	return kafka.Message{
		Topic:     "pg.public.documents",
		Partition: 0,
		Offset:    offset,
		Key:       []byte(id),
		Value:     []byte(value),
	}
}

// tombstoneMessage is the null-valued message Debezium writes after a delete
// so that log compaction can drop the key.
func tombstoneMessage(id string, offset int64) kafka.Message {
	return kafka.Message{Topic: "pg.public.documents", Offset: offset, Key: []byte(id), Value: nil}
}

func TestCDCPipelineIndexesChanges(t *testing.T) {
	st := newStores()
	dlq := &fakeDeadLetters{}
	pipeline := newCDCTestPipeline(t, st, dlq, 3)
	ctx := context.Background()

	if err := pipeline.Process(ctx, changeMessage("c", cdcTestID, 1, 0)); err != nil {
		t.Fatalf("create: %v", err)
	}
	documents, points := st.counts()
	if documents != 1 || points != 1 {
		t.Fatalf("after create: %d documents, %d points; want 1 and 1", documents, points)
	}

	if err := pipeline.Process(ctx, changeMessage("u", cdcTestID, 2, 1)); err != nil {
		t.Fatalf("update: %v", err)
	}
	documents, points = st.counts()
	if documents != 1 || points != 1 {
		t.Fatalf("after update: %d documents, %d points; want the same document updated", documents, points)
	}
	if got := st.document(cdcTestID).Version; got != 2 {
		t.Errorf("indexed version = %d, want 2", got)
	}

	if err := pipeline.Process(ctx, changeMessage("d", cdcTestID, 2, 2)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if documents, points = st.counts(); documents != 0 || points != 0 {
		t.Fatalf("after delete: %d documents, %d points; want both stores empty", documents, points)
	}

	if len(dlq.causes) != 0 {
		t.Errorf("dead-lettered %d valid messages: %v", len(dlq.causes), dlq.causes)
	}
}

// A tombstone is normal traffic after a delete. It must be finished quietly,
// not dead-lettered, or the dead-letter topic would fill with routine messages.
func TestCDCPipelineSkipsTombstones(t *testing.T) {
	st := newStores()
	dlq := &fakeDeadLetters{}
	pipeline := newCDCTestPipeline(t, st, dlq, 3)

	if err := pipeline.Process(context.Background(), tombstoneMessage(cdcTestID, 0)); err != nil {
		t.Fatalf("Process returned %v, want nil so the offset is committed", err)
	}
	if len(dlq.causes) != 0 {
		t.Errorf("a tombstone was dead-lettered: %v", dlq.causes)
	}
	if documents, points := st.counts(); documents != 0 || points != 0 {
		t.Errorf("a tombstone wrote to the stores: %d documents, %d points", documents, points)
	}
}

// Anything a retry cannot fix goes straight to the dead-letter topic, without
// spending attempts on it first.
func TestCDCPipelineDeadLettersUnfixableMessages(t *testing.T) {
	broken := func(value string) kafka.Message {
		return kafka.Message{Topic: "pg.public.documents", Key: []byte(cdcTestID), Value: []byte(value)}
	}

	tests := []struct {
		name string
		msg  kafka.Message
	}{
		{"invalid json", broken(`{"op": "c", "after":`)},
		{"unknown operation", broken(`{"op": "x", "after": {"id": "1", "body": "b"}, "source": {}}`)},
		{"truncate", broken(`{"op": "t", "after": null, "source": {}}`)},
		{"missing primary key", broken(`{"op": "c", "after": {"title": "t", "body": "b"}, "source": {}}`)},
		{"nothing to embed", broken(`{"op": "c", "after": {"id": "1", "title": "", "body": ""}, "source": {}}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newStores()
			dlq := &fakeDeadLetters{}

			if err := newCDCTestPipeline(t, st, dlq, 3).Process(context.Background(), tt.msg); err != nil {
				t.Fatalf("Process returned %v, want nil (message finished)", err)
			}
			if len(dlq.causes) != 1 {
				t.Fatalf("dead-lettered %d messages, want 1", len(dlq.causes))
			}
			if documents, points := st.counts(); documents != 0 || points != 0 {
				t.Errorf("an unusable message reached the stores: %d documents, %d points", documents, points)
			}
		})
	}
}

// A store that is briefly unavailable is retried, because the same event will
// succeed once it comes back.
func TestCDCPipelineRetriesTemporaryStoreFailures(t *testing.T) {
	st := newStores()
	dlq := &fakeDeadLetters{}
	// The vector store is unavailable for two attempts, then recovers.
	st.failUpserts(2, errors.New("qdrant unavailable"))

	pipeline := newCDCTestPipeline(t, st, dlq, 5)
	if err := pipeline.Process(context.Background(), changeMessage("c", cdcTestID, 1, 0)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if documents, points := st.counts(); documents != 1 || points != 1 {
		t.Fatalf("after the retry: %d documents, %d points; want 1 and 1", documents, points)
	}
	if len(dlq.causes) != 0 {
		t.Errorf("a recoverable failure was dead-lettered: %v", dlq.causes)
	}
}

// A store that never recovers must not be retried forever: after the
// configured number of attempts the message is dead-lettered and consumption
// carries on.
func TestCDCPipelineDeadLettersAfterRetriesAreExhausted(t *testing.T) {
	st := newStores()
	dlq := &fakeDeadLetters{}
	want := errors.New("qdrant unavailable")
	st.failUpserts(-1, want)

	if err := newCDCTestPipeline(t, st, dlq, 3).Process(context.Background(), changeMessage("c", cdcTestID, 1, 0)); err != nil {
		t.Fatalf("Process returned %v, want nil (message finished)", err)
	}
	if len(dlq.causes) != 1 || !errors.Is(dlq.causes[0], want) {
		t.Fatalf("dead-letter causes = %v, want one wrapping %v", dlq.causes, want)
	}
}

// The whole path, driven the way the consumer drives it: a bounded pool of
// workers reading a bounded queue.
func TestCDCWorkerPoolProcessesEveryMessage(t *testing.T) {
	const workers, documents = 4, 40
	st := newStores()
	dlq := &fakeDeadLetters{}
	pipeline := newCDCTestPipeline(t, st, dlq, 3)

	var active, peak atomic.Int32
	var done sync.WaitGroup
	done.Add(documents)

	handler := func(ctx context.Context, msg kafka.Message) error {
		defer done.Done()
		n := active.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		defer active.Add(-1)
		return pipeline.Process(ctx, msg)
	}

	pool, err := NewWorkerPool(workers, 8, handler, func(err error) { t.Errorf("unexpected fatal error: %v", err) },
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewWorkerPool: %v", err)
	}
	pool.Start(context.Background())

	for i := range documents {
		msg := changeMessage("c", fmt.Sprintf("doc-%d", i), 1, int64(i))
		if err := pool.Submit(context.Background(), msg); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	done.Wait()
	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// Every submitted message was indexed exactly once.
	if gotDocs, gotPoints := st.counts(); gotDocs != documents || gotPoints != documents {
		t.Errorf("indexed %d documents and %d points, want %d of each", gotDocs, gotPoints, documents)
	}
	// Concurrency never exceeded the configured worker count, so the number of
	// requests in flight against OpenSearch, Qdrant and the embedder is capped
	// however fast Kafka delivers.
	if got := peak.Load(); got > workers {
		t.Errorf("peak concurrency = %d, want at most the configured %d workers", got, workers)
	}
	if len(dlq.causes) != 0 {
		t.Errorf("dead-lettered %d valid messages: %v", len(dlq.causes), dlq.causes)
	}
}

// Backpressure: once the queue is full, Submit blocks. The consumer calls
// Submit before fetching the next message, so a slow indexing path slows
// consumption instead of building an unbounded backlog in memory.
func TestCDCWorkerPoolAppliesBackpressureWhenTheQueueIsFull(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once

	pool, err := NewWorkerPool(1, 1, func(context.Context, kafka.Message) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	}, func(err error) { t.Errorf("unexpected fatal error: %v", err) }, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewWorkerPool: %v", err)
	}
	pool.Start(context.Background())
	defer func() {
		close(release)
		if err := pool.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()

	// The first message occupies the worker, the second fills the queue.
	if err := pool.Submit(context.Background(), changeMessage("c", "doc-0", 1, 0)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-started
	if err := pool.Submit(context.Background(), changeMessage("c", "doc-1", 1, 1)); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// The third has nowhere to go, so Submit blocks rather than starting
	// another goroutine or growing the queue.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := pool.Submit(ctx, changeMessage("c", "doc-2", 1, 2)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Submit on a full queue returned %v, want it to block until the deadline", err)
	}
}

// Shutdown must stop the workers and leave no goroutines behind, however many
// pools have come and gone.
func TestCDCWorkerPoolShutdownLeavesNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	for range 5 {
		st := newStores()
		pipeline := newCDCTestPipeline(t, st, &fakeDeadLetters{}, 3)

		pool, err := NewWorkerPool(8, 16, pipeline.Process, func(err error) { t.Errorf("unexpected fatal error: %v", err) },
			slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatalf("NewWorkerPool: %v", err)
		}
		pool.Start(context.Background())

		for i := range 16 {
			if err := pool.Submit(context.Background(), changeMessage("c", fmt.Sprintf("doc-%d", i), 1, int64(i))); err != nil {
				t.Fatalf("Submit: %v", err)
			}
		}
		if err := pool.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}

	// Goroutines are torn down asynchronously, so allow a moment to settle
	// before deciding that one has leaked.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines went from %d to %d across five pools; workers are leaking", before, after)
	}
}

// Cancelling the context workers run under stops them without their offsets
// being committed, so the messages are delivered again after a restart.
func TestCDCWorkerPoolStopsOnContextCancellation(t *testing.T) {
	st := newStores()
	pipeline := newCDCTestPipeline(t, st, &fakeDeadLetters{}, 100)
	// Keep the indexing path failing so the message is still being retried
	// when the context is cancelled.
	st.failUpserts(-1, errors.New("qdrant unavailable"))

	ctx, cancel := context.WithCancel(context.Background())
	handling := make(chan struct{})
	var once sync.Once

	pool, err := NewWorkerPool(2, 2, func(ctx context.Context, msg kafka.Message) error {
		once.Do(func() { close(handling) })
		return pipeline.Process(ctx, msg)
	}, func(error) {}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewWorkerPool: %v", err)
	}
	pool.Start(ctx)

	if err := pool.Submit(context.Background(), changeMessage("c", cdcTestID, 1, 0)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-handling
	cancel()

	// Shutdown returns once the workers have noticed, rather than hanging or
	// being killed mid-write.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := pool.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestClassifyChange(t *testing.T) {
	temporary := &opensearch.RequestError{StatusCode: 503, Err: errors.New("unavailable")}
	rejected := &opensearch.RequestError{StatusCode: 400, Err: errors.New("mapping conflict")}

	tests := []struct {
		name          string
		err           error
		wantPermanent bool
	}{
		{"no error", nil, false},
		{"malformed event", fmt.Errorf("parse: %w", cdc.ErrMalformedEvent), true},
		{"unknown operation", cdc.ErrUnknownOperation, true},
		{"missing id", cdc.ErrMissingID, true},
		{"store unavailable", &cdc.StoreError{Store: cdc.StoreKeyword, Err: temporary}, false},
		{"store rejected the request", &cdc.StoreError{Store: cdc.StoreKeyword, Err: rejected}, true},
		{"invalid document", &cdc.StoreError{Store: cdc.StoreKeyword, Err: opensearch.ErrInvalidDocument}, true},
		{"invalid vector", &cdc.StoreError{Store: cdc.StoreVector, Err: qdrant.ErrInvalidVector}, true},
		{"unclassified error", errors.New("something else"), false},
		{
			name: "delete where both stores rejected the request",
			err: errors.Join(
				&cdc.StoreError{Store: cdc.StoreKeyword, Err: opensearch.ErrInvalidDocument},
				&cdc.StoreError{Store: cdc.StoreVector, Err: qdrant.ErrInvalidPoint},
			),
			wantPermanent: true,
		},
		{
			// One store may still recover, so the message is worth retrying
			// even though the other failure is permanent.
			name: "delete where only one store rejected the request",
			err: errors.Join(
				&cdc.StoreError{Store: cdc.StoreKeyword, Err: opensearch.ErrInvalidDocument},
				&cdc.StoreError{Store: cdc.StoreVector, Err: temporary},
			),
			wantPermanent: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyChange(tt.err)
			if isPermanent(got) != tt.wantPermanent {
				t.Fatalf("classifyChange(%v) permanent = %v, want %v", tt.err, isPermanent(got), tt.wantPermanent)
			}
			if tt.err != nil && !errors.Is(got, tt.err) && got != tt.err {
				t.Errorf("classifyChange lost the original error: %v", got)
			}
		})
	}
}
