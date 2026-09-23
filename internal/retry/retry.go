// Package retry runs an operation again after a failure, with exponential
// backoff and a bounded number of attempts.
//
// It decides how often and how long to wait, never what a failure means: a
// Classifier supplied by the caller says whether an error is worth retrying,
// so the rules stay with the code that produces those errors. The package
// knows nothing about Kafka, indexing or HTTP.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// Kind says whether retrying an error can help.
type Kind int

const (
	// Unknown is an error the classifier did not recognise. It is retried,
	// bounded by the policy, rather than discarded: an unrecognised failure
	// is more often a passing fault than a permanent one.
	Unknown Kind = iota
	// Retryable is a failure that may succeed later, such as a timeout, a
	// rate limit or a dependency that is down.
	Retryable
	// NonRetryable is a failure that will happen again in the same way, such
	// as a malformed message or input a dependency rejected.
	NonRetryable
)

func (k Kind) String() string {
	switch k {
	case Retryable:
		return "retryable"
	case NonRetryable:
		return "non_retryable"
	default:
		return "unknown"
	}
}

// ShouldRetry reports whether an error of this kind is worth another attempt.
func (k Kind) ShouldRetry() bool { return k != NonRetryable }

// Classifier decides what kind of failure err is. It is called only with a
// non-nil error.
type Classifier func(err error) Kind

// Policy is how often and how long to wait between attempts.
//
//	delay(attempt) = min(InitialBackoff × Multiplier^(attempt-1), MaxBackoff)
type Policy struct {
	// MaxAttempts is the total number of tries, including the first, so 1
	// means no retries.
	MaxAttempts int
	// InitialBackoff is the wait after the first failure.
	InitialBackoff time.Duration
	// MaxBackoff caps the wait however many attempts have failed.
	MaxBackoff time.Duration
	// Multiplier grows the wait after each failure. 1 keeps it constant.
	Multiplier float64
}

// Validate reports whether the policy can be used. It rejects values that
// would produce no retries, waits that shrink, or a cap below the first wait,
// so a mistake in configuration fails at startup instead of changing how the
// system behaves under load.
func (p Policy) Validate() error {
	switch {
	case p.MaxAttempts < 1:
		return fmt.Errorf("max attempts must be at least 1, got %d", p.MaxAttempts)
	case p.InitialBackoff <= 0:
		return fmt.Errorf("initial backoff must be positive, got %s", p.InitialBackoff)
	case p.MaxBackoff < p.InitialBackoff:
		return fmt.Errorf("max backoff (%s) must not be below the initial backoff (%s)", p.MaxBackoff, p.InitialBackoff)
	case p.Multiplier < 1:
		return fmt.Errorf("multiplier must be at least 1, got %v", p.Multiplier)
	case math.IsInf(p.Multiplier, 0) || math.IsNaN(p.Multiplier):
		return fmt.Errorf("multiplier must be a number, got %v", p.Multiplier)
	}
	return nil
}

// Backoff returns how long to wait after the given attempt, counting from 1.
//
// The delay never exceeds MaxBackoff and never goes negative: the growth is
// computed in floating point and capped before it becomes a Duration, so a
// large attempt number cannot overflow into a negative wait.
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	growth := math.Pow(p.Multiplier, float64(attempt-1))
	delay := float64(p.InitialBackoff) * growth
	if math.IsInf(delay, 0) || delay > float64(p.MaxBackoff) {
		return p.MaxBackoff
	}
	return time.Duration(delay)
}

// Attempt describes one failed try, for logging and metrics.
type Attempt struct {
	// Number is the attempt that failed, counting from 1.
	Number int
	// Delay is how long the runner waits before the next attempt.
	Delay time.Duration
	Kind  Kind
	Err   error
}

// Result is how a run ended.
type Result struct {
	// Attempts is how many tries were made, including the one that succeeded.
	Attempts int
	// Kind describes the final error; it is Unknown when Err is nil.
	Kind Kind
	// Err is the last error, or nil on success.
	Err error
}

// Succeeded reports whether the operation ended without an error.
func (r Result) Succeeded() bool { return r.Err == nil }

// Runner retries operations under one policy.
type Runner struct {
	policy   Policy
	classify Classifier
	onRetry  func(Attempt)
}

// New returns a Runner, or an error if the policy is unusable.
//
// classify decides which failures are worth retrying; a nil classifier treats
// every error as Unknown, which is retried. onRetry, if set, is called before
// each wait, which is where logging and retry metrics belong.
func New(policy Policy, classify Classifier, onRetry func(Attempt)) (*Runner, error) {
	if err := policy.Validate(); err != nil {
		return nil, fmt.Errorf("invalid retry policy: %w", err)
	}
	if classify == nil {
		classify = func(error) Kind { return Unknown }
	}
	return &Runner{policy: policy, classify: classify, onRetry: onRetry}, nil
}

// Policy returns the runner's policy.
func (r *Runner) Policy() Policy { return r.policy }

// Classify reports what kind of failure err is.
func (r *Runner) Classify(err error) Kind {
	if err == nil {
		return Unknown
	}
	return r.classify(err)
}

// Do calls op until it succeeds, until an error is not worth retrying, or
// until the attempts run out, waiting between tries.
//
// Waiting honours ctx, so a shutdown stops a run immediately instead of
// sitting out the backoff. If ctx ends, Do returns the context's error, which
// the caller can tell from a failed operation with errors.Is. Do never starts
// a goroutine: the retries happen on the caller's, which keeps a worker pool
// bounded while its downstream is failing.
func (r *Runner) Do(ctx context.Context, op func(ctx context.Context) error) Result {
	for attempt := 1; ; attempt++ {
		err := op(ctx)
		if err == nil {
			return Result{Attempts: attempt}
		}
		// A cancelled run is not a failed operation: the message stays
		// unacknowledged and is delivered again.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Result{Attempts: attempt, Kind: r.classify(err), Err: ctxErr}
		}

		kind := r.classify(err)
		if !kind.ShouldRetry() || attempt >= r.policy.MaxAttempts {
			return Result{Attempts: attempt, Kind: kind, Err: err}
		}

		delay := r.policy.Backoff(attempt)
		if r.onRetry != nil {
			r.onRetry(Attempt{Number: attempt, Delay: delay, Kind: kind, Err: err})
		}
		if err := Wait(ctx, delay); err != nil {
			return Result{Attempts: attempt, Kind: kind, Err: err}
		}
	}
}

// Wait blocks for d or until ctx ends, returning ctx.Err() if it ends first.
// It uses a timer rather than time.Sleep so that a cancelled context does not
// have to wait out the delay.
func Wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsCancellation reports whether err is a context cancellation or deadline,
// which callers treat as "stopped", not as "failed".
func IsCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
