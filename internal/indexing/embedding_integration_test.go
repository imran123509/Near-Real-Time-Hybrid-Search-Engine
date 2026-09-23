package indexing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"near-real-time-hybrid-search-engine/internal/embedding"
	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/retry"
)

// These tests check how the indexing pipeline uses an embedding provider: when
// it asks for a vector, and what it does with each way a provider can fail.
// FakeEmbedder stands in for the provider, so no embedding API is needed.

// FakeEmbedder is an embedding.Provider that returns Vector, or Err when it is
// set, and records the text of every call.
type FakeEmbedder struct {
	Vector []float32
	Err    error

	mu    sync.Mutex
	texts []string
}

var _ embedding.Provider = (*FakeEmbedder)(nil)

func (f *FakeEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.texts = append(f.texts, text)
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Vector, nil
}

func (f *FakeEmbedder) Dimension() int { return len(f.Vector) }
func (f *FakeEmbedder) Name() string   { return "fake" }

// Calls returns the text of every Embed call so far.
func (f *FakeEmbedder) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.texts...)
}

func newFakeEmbedder() *FakeEmbedder {
	return &FakeEmbedder{Vector: []float32{0.1, 0.2, 0.3}}
}

// Creates, updates and snapshot reads need a vector; a delete does not, and
// must not spend a request on one.
func TestEmbedderIsCalledOnlyForUpserts(t *testing.T) {
	tests := []struct {
		op        string
		wantCalls int
	}{
		{"c", 1}, // CREATE
		{"u", 1}, // UPDATE
		{"r", 1}, // snapshot READ
		{"d", 0}, // DELETE
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			st := newStores()
			embedder := newFakeEmbedder()
			deadLetters := &fakeDeadLetters{}

			msg := changeMessage(tt.op, cdcTestID, 1, 0)
			if err := newCDCTestPipelineWith(t, st, embedder, deadLetters, attemptsPolicy(3)).Process(context.Background(), msg); err != nil {
				t.Fatalf("Process: %v", err)
			}

			calls := embedder.Calls()
			if len(calls) != tt.wantCalls {
				t.Fatalf("embed calls = %d, want %d", len(calls), tt.wantCalls)
			}
			if tt.wantCalls == 0 {
				return
			}
			// The pipeline decides the text; the provider only embeds it.
			if want := fmt.Sprintf("title: Title %s | text: Body for %s.", cdcTestID, cdcTestID); calls[0] != want {
				t.Errorf("embedded text = %q, want %q", calls[0], want)
			}
			if _, points := st.counts(); points != 1 {
				t.Errorf("points = %d, want the vector stored", points)
			}
			if deadLetters.count() != 0 {
				t.Errorf("dead-lettered: %v", deadLetters.all())
			}
		})
	}
}

// A provider that is briefly unavailable, or rate limiting, is retried by the
// pipeline, and never reported as success. Keyword indexing has already
// happened, so the vector index is the part that lags.
func TestEmbeddingFailureIsReturnedAndRetried(t *testing.T) {
	st := newStores()
	embedder := newFakeEmbedder()
	embedder.Err = &embedding.APIError{Provider: "fake", StatusCode: http.StatusTooManyRequests, Message: "quota"}
	deadLetters := &fakeDeadLetters{}

	const attempts = 3
	if err := newCDCTestPipelineWith(t, st, embedder, deadLetters, attemptsPolicy(attempts)).Process(context.Background(), changeMessage("c", cdcTestID, 1, 0)); err != nil {
		t.Fatalf("Process returned %v, want nil once the message is dead-lettered", err)
	}

	if got := len(embedder.Calls()); got != attempts {
		t.Errorf("embed calls = %d, want %d: a rate limit is worth retrying", got, attempts)
	}
	letter := deadLetters.only(t)
	if letter.ErrorType != retry.Retryable.String() || letter.Attempts != attempts {
		t.Fatalf("dead-letter message = type %q after %d attempts; want %q after %d",
			letter.ErrorType, letter.Attempts, retry.Retryable, attempts)
	}
	if !strings.Contains(letter.Error, "status 429") {
		t.Errorf("dead-letter error = %q, want it to report the rate limit", letter.Error)
	}
	documents, points := st.counts()
	if documents != 1 || points != 0 {
		t.Errorf("stores hold %d documents and %d points; want the keyword write kept and no vector", documents, points)
	}
}

// A vector of the wrong length means the model and the configuration disagree.
// Retrying cannot fix that, so the message is dead-lettered after one try and
// nothing reaches Qdrant.
func TestDimensionMismatchIsReturnedAndNotRetried(t *testing.T) {
	st := newStores()
	embedder := newFakeEmbedder()
	embedder.Err = fmt.Errorf("fake: %w: expected 768, got 3", embedding.ErrDimensionMismatch)
	deadLetters := &fakeDeadLetters{}

	if err := newCDCTestPipelineWith(t, st, embedder, deadLetters, attemptsPolicy(5)).Process(context.Background(), changeMessage("u", cdcTestID, 1, 0)); err != nil {
		t.Fatalf("Process returned %v, want nil once the message is dead-lettered", err)
	}

	if got := len(embedder.Calls()); got != 1 {
		t.Errorf("embed calls = %d, want 1: a dimension mismatch cannot succeed on retry", got)
	}
	letter := deadLetters.only(t)
	if letter.ErrorType != retry.NonRetryable.String() || letter.Attempts != 1 {
		t.Fatalf("dead-letter message = type %q after %d attempts; want %q after 1",
			letter.ErrorType, letter.Attempts, retry.NonRetryable)
	}
	if !strings.Contains(letter.Error, embedding.ErrDimensionMismatch.Error()) {
		t.Errorf("dead-letter error = %q, want it to report the dimension mismatch", letter.Error)
	}
	if _, points := st.counts(); points != 0 {
		t.Errorf("points = %d, want none: a wrong-sized vector must never be stored", points)
	}
}

// The context the worker runs under reaches the provider, so a shutdown stops
// an embedding request in flight, and the message is left for redelivery
// instead of being dead-lettered.
func TestEmbeddingStopsWhenTheWorkerIsCancelled(t *testing.T) {
	st := newStores()
	deadLetters := &fakeDeadLetters{}
	started := make(chan struct{})

	blocking := cdc.EmbedderFunc(func(ctx context.Context, _ string) ([]float32, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	err := newCDCTestPipelineWith(t, st, blocking, deadLetters, attemptsPolicy(3)).Process(ctx, changeMessage("c", cdcTestID, 1, 0))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Process = %v, want context.Canceled so the offset is not committed", err)
	}
	if deadLetters.count() != 0 {
		t.Errorf("a cancelled message was dead-lettered: %v", deadLetters.all())
	}
}

func TestClassifyEmbedding(t *testing.T) {
	apiErr := func(status int) error {
		return fmt.Errorf("gemini: %w", &embedding.APIError{Provider: "gemini", StatusCode: status})
	}

	tests := []struct {
		name          string
		err           error
		wantPermanent bool
	}{
		{"empty text", embedding.ErrEmptyText, true},
		{"dimension mismatch", fmt.Errorf("gemini: %w: expected 768, got 3", embedding.ErrDimensionMismatch), true},
		{"bad request", apiErr(http.StatusBadRequest), true},
		{"unauthenticated", apiErr(http.StatusUnauthorized), true},
		{"forbidden", apiErr(http.StatusForbidden), true},
		{"rate limited", apiErr(http.StatusTooManyRequests), false},
		{"server error", apiErr(http.StatusInternalServerError), false},
		{"unavailable", apiErr(http.StatusServiceUnavailable), false},
		{"status unknown", apiErr(0), false},
		{"transport failure", fmt.Errorf("gemini: %w: connection refused", embedding.ErrUnavailable), false},
		{"provider timeout", fmt.Errorf("gemini: %w: %w", embedding.ErrUnavailable, context.DeadlineExceeded), false},
		{"unusable response", fmt.Errorf("gemini: %w: want 1 embedding, got 0", embedding.ErrInvalidResponse), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyEmbedding(tt.err)
			if isPermanent(got) != tt.wantPermanent {
				t.Fatalf("permanent = %v, want %v", isPermanent(got), tt.wantPermanent)
			}
			if !errors.Is(got, tt.err) {
				t.Errorf("classifyEmbedding lost the original error: %v", got)
			}
		})
	}
}
