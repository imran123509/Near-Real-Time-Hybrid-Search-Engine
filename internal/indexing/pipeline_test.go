package indexing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/dlq"
	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/kafka"
	"near-real-time-hybrid-search-engine/internal/retry"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
)

type fakeIndexer struct {
	errs   []error    // returned in order; nil once exhausted
	effect cdc.Effect // reported when the call succeeds
	calls  int
	hook   func()
}

func (f *fakeIndexer) Index(context.Context, Event) (cdc.Effect, error) {
	f.calls++
	if f.hook != nil {
		f.hook()
	}
	if f.calls <= len(f.errs) {
		if err := f.errs[f.calls-1]; err != nil {
			return cdc.EffectNone, err
		}
	}
	if f.effect != cdc.EffectNone {
		return f.effect, nil
	}
	return cdc.EffectIndexed, nil
}

// fakeDeadLetters stands in for the dead-letter topic. It is safe for
// concurrent use because the worker-pool tests share one between workers.
type fakeDeadLetters struct {
	mu       sync.Mutex
	messages []dlq.Message
	err      error
}

func (f *fakeDeadLetters) Publish(_ context.Context, msg dlq.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, msg)
	return nil
}

func (f *fakeDeadLetters) all() []dlq.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]dlq.Message(nil), f.messages...)
}

func (f *fakeDeadLetters) count() int { return len(f.all()) }

// only returns the single dead-lettered message, failing the test when the
// count is anything else.
func (f *fakeDeadLetters) only(t *testing.T) dlq.Message {
	t.Helper()
	msgs := f.all()
	if len(msgs) != 1 {
		t.Fatalf("dead-lettered %d messages, want 1", len(msgs))
	}
	return msgs[0]
}

// fastPolicy retries three times with waits short enough that the tests do
// not spend real time on them.
func fastPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, Multiplier: 2}
}

// slowPolicy retries with waits long enough that a test notices if one
// happens when it should not.
func slowPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 5, InitialBackoff: 30 * time.Second, MaxBackoff: time.Minute, Multiplier: 2}
}

func newTestPipeline(t *testing.T, ix EventIndexer[Event], deadLetters dlq.Publisher, policy retry.Policy) *Pipeline[Event] {
	t.Helper()
	p, err := NewPipeline(DecodeEvent, ix, deadLetters, policy, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	return p
}

func validMessage() kafka.Message {
	return kafka.Message{
		Topic:     "document-events",
		Partition: 3,
		Offset:    4711,
		Key:       []byte(testDocumentID),
		Value:     []byte(`{"event_id":"e1","document_id":"` + testDocumentID + `","operation":"upsert","version":1}`),
	}
}

// temporaryError is an error that says for itself whether it is worth
// retrying, the way the store and embedding errors do.
type temporaryError struct{ temporary bool }

func (e temporaryError) Error() string   { return "store unavailable" }
func (e temporaryError) Temporary() bool { return e.temporary }

// 1. The ordinary case: one attempt, nothing retried, nothing dead-lettered,
// and the offset may be committed.
func TestPipelineIndexesOnTheFirstAttempt(t *testing.T) {
	ix := &fakeIndexer{}
	deadLetters := &fakeDeadLetters{}

	if err := newTestPipeline(t, ix, deadLetters, fastPolicy()).Process(context.Background(), validMessage()); err != nil {
		t.Fatalf("Process returned %v, want nil so the offset is committed", err)
	}
	if ix.calls != 1 {
		t.Errorf("Index calls = %d, want 1", ix.calls)
	}
	if deadLetters.count() != 0 {
		t.Errorf("dead-lettered a message that succeeded: %v", deadLetters.all())
	}
}

// 2. A store that is briefly unavailable is retried, and the message is
// committed once it succeeds.
func TestPipelineRetriesThenSucceeds(t *testing.T) {
	ix := &fakeIndexer{errs: []error{temporaryError{temporary: true}, temporaryError{temporary: true}}}
	deadLetters := &fakeDeadLetters{}

	if err := newTestPipeline(t, ix, deadLetters, fastPolicy()).Process(context.Background(), validMessage()); err != nil {
		t.Fatalf("Process returned %v, want nil after the retry succeeded", err)
	}
	if ix.calls != 3 {
		t.Errorf("Index calls = %d, want 3 (two failures then a success)", ix.calls)
	}
	if deadLetters.count() != 0 {
		t.Errorf("dead-lettered a message that eventually succeeded: %v", deadLetters.all())
	}
}

// 3. A failure that never clears is retried exactly MaxAttempts times and then
// dead-lettered, so one bad message cannot stall the partition forever.
func TestPipelineDeadLettersWhenRetriesAreExhausted(t *testing.T) {
	failure := temporaryError{temporary: true}
	ix := &fakeIndexer{errs: []error{failure, failure, failure, failure, failure}}
	deadLetters := &fakeDeadLetters{}

	if err := newTestPipeline(t, ix, deadLetters, fastPolicy()).Process(context.Background(), validMessage()); err != nil {
		t.Fatalf("Process returned %v, want nil once the message is safely dead-lettered", err)
	}
	if ix.calls != 3 {
		t.Errorf("Index calls = %d, want the configured 3 attempts", ix.calls)
	}
	msg := deadLetters.only(t)
	if msg.Attempts != 3 || msg.ErrorType != retry.Retryable.String() {
		t.Errorf("dead-letter message = %d attempts, type %q; want 3 and %q", msg.Attempts, msg.ErrorType, retry.Retryable)
	}
	if !strings.Contains(msg.Error, failure.Error()) {
		t.Errorf("dead-letter error = %q, want it to mention %q", msg.Error, failure)
	}
}

// 4. A failure that retrying cannot fix goes to the dead-letter topic at once,
// without spending attempts, and without waiting out any backoff.
func TestPipelineDoesNotRetryNonRetryableFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"rejected by the store", permanent(errors.New("mapping conflict"))},
		{"the error says it is not temporary", temporaryError{temporary: false}},
		{"the store refused the request", &opensearch.RequestError{StatusCode: 400, Err: errors.New("mapping conflict")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ix := &fakeIndexer{errs: []error{tt.err, tt.err, tt.err}}
			deadLetters := &fakeDeadLetters{}

			start := time.Now()
			if err := newTestPipeline(t, ix, deadLetters, slowPolicy()).Process(context.Background(), validMessage()); err != nil {
				t.Fatalf("Process returned %v, want nil once the message is dead-lettered", err)
			}
			if ix.calls != 1 {
				t.Errorf("Index calls = %d, want 1: an unfixable failure must not be retried", ix.calls)
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Errorf("Process took %s, so it waited out a backoff it should have skipped", elapsed)
			}
			if got := deadLetters.only(t).ErrorType; got != retry.NonRetryable.String() {
				t.Errorf("error_type = %q, want %q", got, retry.NonRetryable)
			}
		})
	}
}

// 5. The invariant: a message that reached neither the indexes nor the
// dead-letter topic must not be committed, or it would be lost.
func TestPipelineDoesNotCommitWhenTheDeadLetterPublishFails(t *testing.T) {
	cause := permanent(errors.New("mapping conflict"))
	unavailable := errors.New("kafka unavailable")
	deadLetters := &fakeDeadLetters{err: unavailable}
	ix := &fakeIndexer{errs: []error{cause}}

	err := newTestPipeline(t, ix, deadLetters, fastPolicy()).Process(context.Background(), validMessage())
	if err == nil {
		t.Fatal("Process returned nil, so the offset would be committed and the message lost")
	}
	// Both the reason the message failed and the reason it could not be stored
	// are reported, because either could be what needs fixing.
	if !errors.Is(err, unavailable) || !errors.Is(err, cause) {
		t.Errorf("err = %v, want it to carry both %v and %v", err, unavailable, cause)
	}
}

// 6. A shutdown stops the work rather than dead-lettering a message that was
// never given its full chance; the offset stays uncommitted and Kafka delivers
// it again.
func TestPipelineStopsOnShutdownWithoutDeadLettering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	deadLetters := &fakeDeadLetters{}
	ix := &fakeIndexer{errs: []error{temporaryError{temporary: true}}, hook: cancel}

	start := time.Now()
	err := newTestPipeline(t, ix, deadLetters, slowPolicy()).Process(ctx, validMessage())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled so the offset is left uncommitted", err)
	}
	if ix.calls != 1 || deadLetters.count() != 0 {
		t.Fatalf("calls = %d, dead letters = %d; want 1 and 0", ix.calls, deadLetters.count())
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Process took %s, want an immediate return on cancellation", elapsed)
	}
}

// A dead-letter publish that fails because the consumer is shutting down is
// not a lost message: the offset is left uncommitted either way, and the
// message is delivered again after the restart.
func TestPipelineTreatsACancelledDeadLetterPublishAsShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	deadLetters := &fakeDeadLetters{err: context.Canceled}
	ix := &fakeIndexer{}
	// The decode step fails, so the message is dead-lettered without the
	// indexer being asked to run under a context that has already ended.
	msg := kafka.Message{Topic: "document-events", Offset: 7, Value: []byte("not json")}

	err := newTestPipeline(t, ix, deadLetters, fastPolicy()).Process(ctx, msg)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled so the offset is left uncommitted", err)
	}
}

// 7. The waits between attempts follow the configured backoff. The exact
// sequence is checked in internal/retry; what matters here is that the
// pipeline applies the policy it was given instead of retrying in a tight
// loop.
func TestPipelineWaitsBetweenAttempts(t *testing.T) {
	policy := retry.Policy{MaxAttempts: 3, InitialBackoff: 20 * time.Millisecond, MaxBackoff: time.Second, Multiplier: 2}
	failure := temporaryError{temporary: true}
	ix := &fakeIndexer{errs: []error{failure, failure, failure}}

	start := time.Now()
	if err := newTestPipeline(t, ix, &fakeDeadLetters{}, policy).Process(context.Background(), validMessage()); err != nil {
		t.Fatalf("Process: %v", err)
	}
	// 20ms before the second attempt and 40ms before the third.
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Errorf("three attempts took %s, want at least the configured 60ms of backoff", elapsed)
	}
}

// 8. An unusable retry configuration is refused at startup, where it is
// obvious, rather than producing surprising behaviour per message.
func TestNewPipelineRejectsInvalidRetryPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy retry.Policy
	}{
		{"no attempts", retry.Policy{MaxAttempts: 0, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2}},
		{"no initial backoff", retry.Policy{MaxAttempts: 3, InitialBackoff: 0, MaxBackoff: time.Minute, Multiplier: 2}},
		{"cap below the first wait", retry.Policy{MaxAttempts: 3, InitialBackoff: time.Minute, MaxBackoff: time.Second, Multiplier: 2}},
		{"shrinking multiplier", retry.Policy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 0.5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewPipeline(DecodeEvent, &fakeIndexer{}, &fakeDeadLetters{}, tt.policy, slog.New(slog.DiscardHandler)); err == nil {
				t.Fatal("NewPipeline accepted an unusable retry policy")
			}
		})
	}

	t.Run("missing dependencies", func(t *testing.T) {
		if _, err := NewPipeline(DecodeEvent, &fakeIndexer{}, nil, fastPolicy(), slog.New(slog.DiscardHandler)); err == nil {
			t.Fatal("NewPipeline accepted a nil dead-letter publisher")
		}
	})
}

// 9. What ends up in the dead-letter topic has to be enough to find the
// original message again and to know why it failed.
func TestPipelineDeadLetterCarriesMessageMetadata(t *testing.T) {
	deadLetters := &fakeDeadLetters{}
	ix := &fakeIndexer{errs: []error{permanent(errors.New("mapping conflict"))}}
	msg := validMessage()

	if err := newTestPipeline(t, ix, deadLetters, fastPolicy()).Process(context.Background(), msg); err != nil {
		t.Fatalf("Process: %v", err)
	}

	letter := deadLetters.only(t)
	if letter.OriginalTopic != msg.Topic || letter.OriginalPartition != msg.Partition || letter.OriginalOffset != msg.Offset {
		t.Errorf("origin = %s/%d/%d, want %s/%d/%d",
			letter.OriginalTopic, letter.OriginalPartition, letter.OriginalOffset,
			msg.Topic, msg.Partition, msg.Offset)
	}
	if letter.EventKey != string(msg.Key) {
		t.Errorf("event key = %q, want %q", letter.EventKey, msg.Key)
	}
	if letter.Attempts != 1 || letter.ErrorType != retry.NonRetryable.String() || letter.Error == "" {
		t.Errorf("failure description = %d attempts, type %q, error %q", letter.Attempts, letter.ErrorType, letter.Error)
	}
	if letter.FailedAt.IsZero() {
		t.Error("the message does not say when it failed")
	}
}

// 10. The payload is stored exactly as it arrived, including a payload that
// could not be parsed at all, because replaying it is the whole point.
func TestPipelineDeadLetterKeepsThePayloadUnchanged(t *testing.T) {
	tests := []struct {
		name string
		msg  kafka.Message
	}{
		{"a valid event the indexer rejected", validMessage()},
		{"a payload that is not JSON", kafka.Message{Topic: "document-events", Offset: 12, Value: []byte("not json")}},
		{"a payload that is not text", kafka.Message{Topic: "document-events", Offset: 13, Value: []byte{0x00, 0xff, 0xfe}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deadLetters := &fakeDeadLetters{}
			ix := &fakeIndexer{errs: []error{permanent(errors.New("rejected"))}}

			if err := newTestPipeline(t, ix, deadLetters, fastPolicy()).Process(context.Background(), tt.msg); err != nil {
				t.Fatalf("Process: %v", err)
			}
			if got := deadLetters.only(t).Payload; !bytes.Equal(got, tt.msg.Value) {
				t.Errorf("stored payload = %q, want the original %q", got, tt.msg.Value)
			}
		})
	}
}

// A message that cannot be parsed is dead-lettered without being retried: it
// will not parse next time either.
func TestPipelineDeadLettersMalformedMessages(t *testing.T) {
	deadLetters := &fakeDeadLetters{}
	ix := &fakeIndexer{}

	msg := kafka.Message{Topic: "document-events", Offset: 9, Value: []byte("not json")}
	if err := newTestPipeline(t, ix, deadLetters, slowPolicy()).Process(context.Background(), msg); err != nil {
		t.Fatalf("Process returned %v, want nil once the message is dead-lettered", err)
	}
	if ix.calls != 0 {
		t.Errorf("Index calls = %d, want 0: an unparseable message never reaches the stores", ix.calls)
	}
	if got := deadLetters.only(t).ErrorType; got != retry.NonRetryable.String() {
		t.Errorf("error_type = %q, want %q", got, retry.NonRetryable)
	}
}

// A message with nothing to index is finished quietly, so routine traffic such
// as Debezium's tombstones does not fill the dead-letter topic.
func TestPipelineSkipsMessagesWithNothingToIndex(t *testing.T) {
	deadLetters := &fakeDeadLetters{}
	ix := &fakeIndexer{}
	skip := func([]byte) (Event, error) { return Event{}, ErrSkipMessage }

	p, err := NewPipeline(skip, ix, deadLetters, fastPolicy(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	if err := p.Process(context.Background(), validMessage()); err != nil {
		t.Fatalf("Process returned %v, want nil so the offset is committed", err)
	}
	if ix.calls != 0 || deadLetters.count() != 0 {
		t.Errorf("a skipped message reached the indexer (%d calls) or the dead-letter topic (%d messages)",
			ix.calls, deadLetters.count())
	}
}

// Every message reports exactly one outcome, which is what a metrics exporter
// will count. Without this the counters could disagree with the logs.
func TestPipelineReportsEveryOutcome(t *testing.T) {
	failure := temporaryError{temporary: true}

	tests := []struct {
		name        string
		msg         kafka.Message
		indexErrs   []error
		publishErr  error
		want        Outcome
		wantAttempt int
	}{
		{name: "indexed", msg: validMessage(), want: OutcomeIndexed, wantAttempt: 1},
		{name: "retried then indexed", msg: validMessage(), indexErrs: []error{failure}, want: OutcomeIndexed, wantAttempt: 2},
		{name: "dead-lettered", msg: validMessage(), indexErrs: []error{failure, failure, failure}, want: OutcomeDeadLettered, wantAttempt: 3},
		{name: "not stored anywhere", msg: validMessage(), indexErrs: []error{failure, failure, failure},
			publishErr: errors.New("kafka unavailable"), want: OutcomeFailed, wantAttempt: 3},
		{name: "unparseable", msg: kafka.Message{Value: []byte("not json")}, want: OutcomeDeadLettered},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestPipeline(t, &fakeIndexer{errs: tt.indexErrs}, &fakeDeadLetters{err: tt.publishErr}, fastPolicy())

			var reports []Report
			p.Observe(func(r Report) { reports = append(reports, r) })
			_ = p.Process(context.Background(), tt.msg)

			if len(reports) != 1 {
				t.Fatalf("%d outcomes reported, want exactly 1", len(reports))
			}
			if reports[0].Outcome != tt.want || reports[0].Attempts != tt.wantAttempt {
				t.Errorf("outcome = %s after %d attempts, want %s after %d",
					reports[0].Outcome, reports[0].Attempts, tt.want, tt.wantAttempt)
			}
		})
	}
}

// Classification reads the errors' types, never their text, so a reworded
// message from a dependency cannot turn a permanent failure into an endless
// retry or the other way round.
func TestClassifyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want retry.Kind
	}{
		{"no error", nil, retry.Unknown},
		{"marked permanent", permanent(errors.New("mapping conflict")), retry.NonRetryable},
		{"wrapped permanent", fmt.Errorf("index in opensearch: %w", permanent(errors.New("x"))), retry.NonRetryable},
		{"the store is unavailable", &opensearch.RequestError{StatusCode: 503, Err: errors.New("unavailable")}, retry.Retryable},
		{"the store refused the request", &opensearch.RequestError{StatusCode: 400, Err: errors.New("bad mapping")}, retry.NonRetryable},
		{"rate limited", &opensearch.RequestError{StatusCode: 429, Err: errors.New("too many requests")}, retry.Retryable},
		{"unrecognised", errors.New("service unavailable, permanent, do not retry"), retry.Unknown},
		{"cancelled", context.Canceled, retry.Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != tt.want {
				t.Errorf("ClassifyError(%v) = %s, want %s", tt.err, got, tt.want)
			}
		})
	}
}
