// Package embedding turns text into vectors.
//
// Provider is the contract every embedding backend implements, and the only
// thing the rest of the application depends on. A provider's job is exactly
// text -> vector: it does not know what a search document is or how one
// becomes text. That rule lives in the indexing layer, so the same provider
// can embed documents today and search queries later, and a provider can be
// swapped without touching what gets embedded.
//
// Each provider converts its SDK's errors into the types defined here, so no
// provider SDK leaks past this package.
package embedding

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"

	"near-real-time-hybrid-search-engine/internal/config"
)

// Provider turns text into a vector of a fixed length. Implementations must:
//
//   - reject empty or whitespace-only text with ErrEmptyText, without sending
//     a request;
//   - return exactly Dimension values, never a truncated or padded vector, and
//     report anything else as ErrDimensionMismatch;
//   - honour the caller's cancellation and deadline;
//   - send each request once and return the error, because whether to retry is
//     the caller's decision;
//   - be safe for concurrent use.
type Provider interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	// Dimension is the length of every vector Embed returns.
	Dimension() int
	// Name identifies the provider in logs and errors, such as "gemini".
	Name() string
}

var (
	// ErrEmptyText means there was nothing to embed. No request was sent.
	ErrEmptyText = errors.New("embedding text cannot be empty")

	// ErrDimensionMismatch means the provider returned a vector of a length
	// other than the configured dimension. Storing it would fail in Qdrant or,
	// worse, corrupt similarity scores, so it is rejected instead.
	ErrDimensionMismatch = errors.New("embedding dimension mismatch")

	// ErrInvalidResponse means the provider answered successfully but the
	// answer could not be used, for example because it held no embedding.
	ErrInvalidResponse = errors.New("invalid embedding response")

	// ErrUnauthenticated means the provider rejected the credentials.
	ErrUnauthenticated = errors.New("embedding provider rejected the credentials")

	// ErrRateLimited means the provider refused the request because of a rate
	// limit or quota.
	ErrRateLimited = errors.New("embedding provider rate limit exceeded")

	// ErrUnavailable means the provider could not be reached, timed out, or
	// failed internally. Retrying later may succeed.
	ErrUnavailable = errors.New("embedding provider unavailable")

	// ErrUnknownProvider means the configuration names a provider that does
	// not exist.
	ErrUnknownProvider = errors.New("unknown embedding provider")
)

// maxErrorMessage caps how much of a provider's error body is kept, so an
// unexpectedly large response cannot flood the logs.
const maxErrorMessage = 512

// New returns the provider cfg.Provider names. It is the one place that knows
// which providers exist: adding one means a constructor beside the others and
// a case here, and nothing that uses a Provider changes.
func New(ctx context.Context, cfg config.EmbeddingConfig) (Provider, error) {
	switch cfg.Provider {
	case ProviderGemini:
		return NewGeminiProvider(ctx, cfg)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownProvider, cfg.Provider)
	}
}

// APIError is an error response from an embedding provider, reduced to what a
// caller needs to decide whether to retry. It matches ErrUnauthenticated,
// ErrRateLimited and ErrUnavailable with errors.Is according to its status.
type APIError struct {
	Provider string
	// StatusCode is the HTTP status, or 0 when the provider's error did not
	// say, as happens when a proxy answers with an error body of its own.
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s api: status %d: %s", e.Provider, e.StatusCode, e.Message)
}

// Temporary reports whether retrying the request may succeed: rate limits,
// timeouts and server errors. An unknown status is treated as temporary too,
// so an unexplained failure is retried a bounded number of times rather than
// dead-lettered at once. Any other status means the request itself was
// refused and will be refused again.
func (e *APIError) Temporary() bool {
	return e.StatusCode == 0 ||
		e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode == http.StatusRequestTimeout ||
		e.StatusCode >= http.StatusInternalServerError
}

// Is reports whether the error belongs to one of the kinds above, so callers
// can write errors.Is(err, embedding.ErrRateLimited) without knowing codes.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrUnauthenticated:
		return e.StatusCode == http.StatusUnauthorized || e.StatusCode == http.StatusForbidden
	case ErrRateLimited:
		return e.StatusCode == http.StatusTooManyRequests
	case ErrUnavailable:
		return e.StatusCode == http.StatusRequestTimeout || e.StatusCode >= http.StatusInternalServerError
	default:
		return false
	}
}

// newAPIError builds an APIError, trimming the message to maxErrorMessage.
func newAPIError(provider string, status int, message string) *APIError {
	if len(message) > maxErrorMessage {
		message = message[:maxErrorMessage] + "..."
	}
	return &APIError{Provider: provider, StatusCode: status, Message: message}
}

// validateDimension checks a configured dimension before any request uses it.
func validateDimension(dimension int) error {
	if dimension <= 0 || dimension > math.MaxInt32 {
		return fmt.Errorf("embedding dimension must be between 1 and %d, got %d", math.MaxInt32, dimension)
	}
	return nil
}

// checkVector rejects a vector that must not reach the vector store: one of
// the wrong length, or one holding values that cannot be compared. It never
// truncates or pads a vector to make it fit.
func checkVector(v []float32, dimension int) error {
	if len(v) != dimension {
		return fmt.Errorf("%w: expected %d, got %d", ErrDimensionMismatch, dimension, len(v))
	}
	for i, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return fmt.Errorf("%w: value at index %d is %v", ErrInvalidResponse, i, x)
		}
	}
	return nil
}
