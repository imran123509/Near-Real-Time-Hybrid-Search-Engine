// Package qdrant stores document vectors in Qdrant and runs similarity search
// over them.
//
// It is the only package that uses the Qdrant SDK. Callers work with the
// Client, Point and SearchResult types defined here and pass in vectors that
// were produced elsewhere; this package never generates embeddings.
package qdrant

import (
	"context"
	"errors"
	"fmt"
	"strings"

	qd "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"near-real-time-hybrid-search-engine/internal/config"
)

// maxVectorSize is Qdrant's limit on the dimension of a dense vector.
const maxVectorSize = 65536

// Client reads and writes points in one Qdrant collection over gRPC. It is safe
// for concurrent use and meant to be shared for the life of the process.
type Client struct {
	api        *qd.Client
	collection string
	vectorSize int
}

// New validates cfg and creates a client for cfg.Collection. It does not
// contact Qdrant; call Ping to check the server is reachable. The caller owns
// the client and must call Close.
//
// cfg.Host, cfg.Port and cfg.UseTLS are the gRPC endpoint that config.Load
// derives from QDRANT_URL.
func New(cfg config.QdrantConfig) (*Client, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid qdrant config: %w", err)
	}

	api, err := qd.NewClient(&qd.Config{
		Host:   cfg.Host,
		Port:   cfg.Port,
		APIKey: cfg.APIKey,
		UseTLS: cfg.UseTLS,
		// The built-in version check blocks for up to a minute and ignores the
		// caller's context. Ping checks reachability within the caller's deadline.
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		return nil, fmt.Errorf("create qdrant client: %w", err)
	}
	return &Client{api: api, collection: cfg.Collection, vectorSize: cfg.VectorSize}, nil
}

// Ping checks that Qdrant is reachable and accepts the API key, if one is set.
func (c *Client) Ping(ctx context.Context) error {
	if _, err := c.api.HealthCheck(ctx); err != nil {
		return fmt.Errorf("ping qdrant: %w", newRequestError(err))
	}
	return nil
}

// Close closes the client's gRPC connections.
func (c *Client) Close() error {
	return c.api.Close()
}

// RequestError is a Qdrant request that failed.
type RequestError struct {
	code codes.Code
	Err  error
}

func (e *RequestError) Error() string { return e.Err.Error() }
func (e *RequestError) Unwrap() error { return e.Err }

// Temporary reports whether retrying the request may succeed: the server was
// unreachable, overloaded, timed out or failed internally. A request Qdrant
// rejected as invalid, unauthorised or aimed at a missing collection will
// fail again.
func (e *RequestError) Temporary() bool {
	switch e.code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted,
		codes.Aborted, codes.Internal, codes.Unknown, codes.Canceled:
		return true
	default:
		return false
	}
}

func newRequestError(err error) *RequestError {
	return &RequestError{code: status.Code(err), Err: err}
}

func validateConfig(cfg config.QdrantConfig) error {
	switch {
	case cfg.Host == "":
		return errors.New("host is required")
	case cfg.Port < 1 || cfg.Port > 65535:
		return fmt.Errorf("port must be between 1 and 65535, got %d", cfg.Port)
	case cfg.VectorSize < 1 || cfg.VectorSize > maxVectorSize:
		return fmt.Errorf("vector size must be between 1 and %d, got %d", maxVectorSize, cfg.VectorSize)
	}
	return validateCollectionName(cfg.Collection)
}

// validateCollectionName applies Qdrant's naming rules, so a bad name fails at
// startup instead of on the first request.
func validateCollectionName(name string) error {
	switch {
	case strings.TrimSpace(name) == "":
		return errors.New("collection name is required")
	case len(name) > 255:
		return errors.New("collection name must be at most 255 bytes")
	case strings.ContainsAny(name, "<>:\"/\\|?*\x00"):
		return fmt.Errorf(`collection name %q must not contain < > : " / \ | ? or *`, name)
	}
	return nil
}
