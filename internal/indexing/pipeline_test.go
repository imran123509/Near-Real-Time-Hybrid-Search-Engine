package indexing

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/kafka"
)

type fakeIndexer struct {
	errs  []error // returned in order; nil once exhausted
	calls int
	hook  func()
}

func (f *fakeIndexer) Index(context.Context, Event) error {
	f.calls++
	if f.hook != nil {
		f.hook()
	}
	if f.calls <= len(f.errs) {
		return f.errs[f.calls-1]
	}
	return nil
}

type fakeDeadLetters struct {
	causes []error
	err    error
}

func (f *fakeDeadLetters) Publish(_ context.Context, _ kafka.Message, cause error) error {
	f.causes = append(f.causes, cause)
	return f.err
}

func newTestPipeline(ix EventIndexer, dlq DeadLetterPublisher, maxAttempts int) *Pipeline {
	p := NewPipeline(ix, dlq, maxAttempts, slog.New(slog.DiscardHandler))
	p.backoff = func(int) time.Duration { return 0 }
	return p
}

func validMessage() kafka.Message {
	return kafka.Message{
		Key:   []byte(testDocumentID),
		Value: []byte(`{"event_id":"e1","document_id":"` + testDocumentID + `","operation":"upsert","version":1}`),
	}
}

func TestPipelineProcess(t *testing.T) {
	temporary := errors.New("opensearch timeout")
	rejected := permanent(errors.New("mapping error"))

	tests := []struct {
		name         string
		msg          kafka.Message
		indexErrs    []error
		maxAttempts  int
		wantCalls    int
		wantDLQ      bool
		wantDLQCause error
	}{
		{name: "indexed first try", msg: validMessage(), maxAttempts: 3, wantCalls: 1},
		{name: "temporary errors then success", msg: validMessage(), indexErrs: []error{temporary, temporary}, maxAttempts: 3, wantCalls: 3},
		{name: "retries exhausted", msg: validMessage(), indexErrs: []error{temporary, temporary, temporary}, maxAttempts: 3, wantCalls: 3, wantDLQ: true, wantDLQCause: temporary},
		{name: "permanent error is not retried", msg: validMessage(), indexErrs: []error{rejected}, maxAttempts: 3, wantCalls: 1, wantDLQ: true, wantDLQCause: rejected},
		{name: "malformed event is not indexed", msg: kafka.Message{Value: []byte("not json")}, maxAttempts: 3, wantCalls: 0, wantDLQ: true, wantDLQCause: ErrMalformedEvent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ix := &fakeIndexer{errs: tt.indexErrs}
			dlq := &fakeDeadLetters{}

			if err := newTestPipeline(ix, dlq, tt.maxAttempts).Process(context.Background(), tt.msg); err != nil {
				t.Fatalf("Process returned %v, want nil (message finished)", err)
			}
			if ix.calls != tt.wantCalls {
				t.Errorf("Index calls = %d, want %d", ix.calls, tt.wantCalls)
			}
			if got := len(dlq.causes) == 1; got != tt.wantDLQ {
				t.Fatalf("dead-lettered = %v, want %v", got, tt.wantDLQ)
			}
			if tt.wantDLQ && !errors.Is(dlq.causes[0], tt.wantDLQCause) {
				t.Errorf("dead-letter cause = %v, want %v", dlq.causes[0], tt.wantDLQCause)
			}
		})
	}
}

func TestPipelineReturnsErrorWhenDeadLetterFails(t *testing.T) {
	dlq := &fakeDeadLetters{err: errors.New("kafka unavailable")}
	ix := &fakeIndexer{errs: []error{permanent(errors.New("rejected"))}}

	err := newTestPipeline(ix, dlq, 3).Process(context.Background(), validMessage())
	if err == nil {
		t.Fatal("Process returned nil, so the offset would be committed and the message lost")
	}
}

func TestPipelineStopsOnCancellationWithoutDeadLettering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dlq := &fakeDeadLetters{}
	ix := &fakeIndexer{errs: []error{context.Canceled}, hook: cancel}

	err := newTestPipeline(ix, dlq, 5).Process(ctx, validMessage())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if ix.calls != 1 || len(dlq.causes) != 0 {
		t.Fatalf("calls = %d, dead letters = %d; want 1 and 0", ix.calls, len(dlq.causes))
	}
}

func TestBackoffStaysInBounds(t *testing.T) {
	for attempt := 1; attempt <= 100; attempt++ {
		d := backoff(attempt)
		if d <= 0 || d > maxRetryDelay {
			t.Fatalf("backoff(%d) = %v, want within (0, %v]", attempt, d, maxRetryDelay)
		}
	}
}
