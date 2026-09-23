package retry

import (
	"context"
	"errors"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"
)

// testPolicy retries quickly, so tests do not spend their time waiting.
func testPolicy() Policy {
	return Policy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2}
}

func newRunner(t *testing.T, policy Policy, classify Classifier) (*Runner, *[]Attempt) {
	t.Helper()
	var retries []Attempt
	r, err := New(policy, classify, func(a Attempt) { retries = append(retries, a) })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r, &retries
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	// The documented example: 500ms, 1s, 2s, ... capped at 30s.
	policy := Policy{MaxAttempts: 10, InitialBackoff: 500 * time.Millisecond, MaxBackoff: 30 * time.Second, Multiplier: 2}

	want := []time.Duration{
		500 * time.Millisecond, // attempt 1
		time.Second,            // attempt 2
		2 * time.Second,        // attempt 3
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second, // 32s would exceed the cap
		30 * time.Second,
	}
	for i, expected := range want {
		if got := policy.Backoff(i + 1); got != expected {
			t.Errorf("Backoff(%d) = %s, want %s", i+1, got, expected)
		}
	}

	// A multiplier of 1 keeps the delay constant.
	constant := Policy{MaxAttempts: 5, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 1}
	for attempt := 1; attempt <= 5; attempt++ {
		if got := constant.Backoff(attempt); got != time.Second {
			t.Errorf("Backoff(%d) = %s, want 1s", attempt, got)
		}
	}
}

func TestBackoffNeverOverflows(t *testing.T) {
	policy := Policy{MaxAttempts: math.MaxInt, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 10}
	for _, attempt := range []int{0, 1, 50, 500, math.MaxInt32, math.MaxInt} {
		got := policy.Backoff(attempt)
		if got <= 0 || got > policy.MaxBackoff {
			t.Errorf("Backoff(%d) = %s, want a positive delay no larger than %s", attempt, got, policy.MaxBackoff)
		}
	}
}

func TestPolicyValidate(t *testing.T) {
	tests := []struct {
		name        string
		change      func(*Policy)
		wantInError string
	}{
		{"valid", func(*Policy) {}, ""},
		{"no retries is allowed", func(p *Policy) { p.MaxAttempts = 1 }, ""},
		{"zero attempts", func(p *Policy) { p.MaxAttempts = 0 }, "max attempts"},
		{"negative attempts", func(p *Policy) { p.MaxAttempts = -1 }, "max attempts"},
		{"zero initial backoff", func(p *Policy) { p.InitialBackoff = 0 }, "initial backoff"},
		{"negative initial backoff", func(p *Policy) { p.InitialBackoff = -time.Second }, "initial backoff"},
		{"max below initial", func(p *Policy) { p.MaxBackoff = p.InitialBackoff - 1 }, "max backoff"},
		{"zero multiplier", func(p *Policy) { p.Multiplier = 0 }, "multiplier"},
		{"negative multiplier", func(p *Policy) { p.Multiplier = -2 }, "multiplier"},
		{"shrinking multiplier", func(p *Policy) { p.Multiplier = 0.5 }, "multiplier"},
		{"infinite multiplier", func(p *Policy) { p.Multiplier = math.Inf(1) }, "multiplier"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := testPolicy()
			tt.change(&policy)

			err := policy.Validate()
			if tt.wantInError == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantInError) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.wantInError)
			}
			// New refuses the same policies.
			if _, err := New(policy, nil, nil); err == nil {
				t.Error("New accepted an invalid policy")
			}
		})
	}
}

func TestDoSucceedsOnFirstAttempt(t *testing.T) {
	r, retries := newRunner(t, testPolicy(), alwaysRetryable)

	calls := 0
	result := r.Do(context.Background(), func(context.Context) error {
		calls++
		return nil
	})

	if !result.Succeeded() || result.Attempts != 1 || calls != 1 {
		t.Fatalf("result = %+v after %d calls, want one successful attempt", result, calls)
	}
	if len(*retries) != 0 {
		t.Errorf("%d retries reported, want none", len(*retries))
	}
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	r, retries := newRunner(t, testPolicy(), alwaysRetryable)

	calls := 0
	result := r.Do(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("opensearch unavailable")
		}
		return nil
	})

	if !result.Succeeded() || result.Attempts != 3 {
		t.Fatalf("result = %+v, want success on the third attempt", result)
	}
	if len(*retries) != 2 {
		t.Fatalf("%d retries reported, want 2", len(*retries))
	}
	for i, attempt := range *retries {
		if attempt.Number != i+1 || attempt.Kind != Retryable || attempt.Err == nil {
			t.Errorf("retry %d = %+v", i, attempt)
		}
	}
	// The delays follow the policy: 1ms then 2ms.
	if (*retries)[0].Delay != time.Millisecond || (*retries)[1].Delay != 2*time.Millisecond {
		t.Errorf("delays = %s, %s; want 1ms, 2ms", (*retries)[0].Delay, (*retries)[1].Delay)
	}
}

func TestDoStopsWhenAttemptsRunOut(t *testing.T) {
	r, retries := newRunner(t, testPolicy(), alwaysRetryable)
	boom := errors.New("qdrant unavailable")

	calls := 0
	result := r.Do(context.Background(), func(context.Context) error {
		calls++
		return boom
	})

	if calls != 3 || result.Attempts != 3 {
		t.Fatalf("%d calls, result %+v; want 3 attempts", calls, result)
	}
	if !errors.Is(result.Err, boom) || result.Kind != Retryable {
		t.Errorf("result = %+v, want the last error and its kind", result)
	}
	if len(*retries) != 2 {
		t.Errorf("%d retries reported, want 2 (waits happen between attempts)", len(*retries))
	}
}

func TestDoDoesNotRetryNonRetryableErrors(t *testing.T) {
	r, retries := newRunner(t, testPolicy(), func(error) Kind { return NonRetryable })
	invalid := errors.New("invalid document")

	calls := 0
	result := r.Do(context.Background(), func(context.Context) error {
		calls++
		return invalid
	})

	if calls != 1 || result.Attempts != 1 {
		t.Fatalf("%d calls, want 1: a non-retryable error must not be retried", calls)
	}
	if !errors.Is(result.Err, invalid) || result.Kind != NonRetryable {
		t.Errorf("result = %+v", result)
	}
	if len(*retries) != 0 {
		t.Errorf("%d retries reported, want none", len(*retries))
	}
}

func TestDoRetriesUnknownErrors(t *testing.T) {
	// An unrecognised failure is more often passing than permanent, so it is
	// retried, but still bounded by the policy.
	r, _ := newRunner(t, testPolicy(), func(error) Kind { return Unknown })

	calls := 0
	result := r.Do(context.Background(), func(context.Context) error {
		calls++
		return errors.New("something new")
	})
	if calls != 3 || result.Kind != Unknown {
		t.Errorf("%d calls, result %+v; want 3 attempts and Unknown", calls, result)
	}
}

func TestDoStopsImmediatelyOnCancellation(t *testing.T) {
	// A long backoff that the test must not wait for.
	policy := Policy{MaxAttempts: 5, InitialBackoff: 30 * time.Second, MaxBackoff: time.Minute, Multiplier: 2}
	r, _ := newRunner(t, policy, alwaysRetryable)

	ctx, cancel := context.WithCancel(context.Background())
	before := runtime.NumGoroutine()

	start := time.Now()
	calls := 0
	result := r.Do(ctx, func(context.Context) error {
		calls++
		cancel() // the shutdown arrives while the operation is failing
		return errors.New("opensearch unavailable")
	})
	elapsed := time.Since(start)

	if !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("result = %+v, want context.Canceled", result)
	}
	if calls != 1 {
		t.Errorf("%d calls, want 1: a cancelled run must not try again", calls)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Do took %s; it waited out the backoff instead of returning", elapsed)
	}
	if after := runtime.NumGoroutine(); after > before+1 {
		t.Errorf("goroutines went from %d to %d", before, after)
	}
}

func TestWaitHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := Wait(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Wait took %s, want an immediate return", elapsed)
	}

	if err := Wait(context.Background(), time.Millisecond); err != nil {
		t.Errorf("Wait: %v", err)
	}
}

func TestKind(t *testing.T) {
	tests := []struct {
		kind        Kind
		name        string
		shouldRetry bool
	}{
		{Unknown, "unknown", true},
		{Retryable, "retryable", true},
		{NonRetryable, "non_retryable", false},
	}
	for _, tt := range tests {
		if tt.kind.String() != tt.name || tt.kind.ShouldRetry() != tt.shouldRetry {
			t.Errorf("%v: name %q, retry %v; want %q, %v", tt.kind, tt.kind.String(), tt.kind.ShouldRetry(), tt.name, tt.shouldRetry)
		}
	}
}

func TestClassifyWithoutAClassifier(t *testing.T) {
	r, err := New(testPolicy(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Classify(errors.New("x")); got != Unknown {
		t.Errorf("Classify = %v, want Unknown", got)
	}
	if got := r.Classify(nil); got != Unknown {
		t.Errorf("Classify(nil) = %v, want Unknown", got)
	}
}

func TestIsCancellation(t *testing.T) {
	if !IsCancellation(context.Canceled) || !IsCancellation(context.DeadlineExceeded) {
		t.Error("context errors are cancellations")
	}
	if IsCancellation(errors.New("opensearch unavailable")) {
		t.Error("an ordinary failure is not a cancellation")
	}
}

func alwaysRetryable(error) Kind { return Retryable }
