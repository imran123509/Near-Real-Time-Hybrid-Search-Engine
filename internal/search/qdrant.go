package search

import (
	"context"
	"fmt"

	"github.com/qdrant/go-client/qdrant"

	"near-real-time-hybrid-search-engine/internal/config"
)

// NewQdrantClient creates a Qdrant gRPC client and verifies the server is
// reachable. The caller owns the client and must call Close.
func NewQdrantClient(ctx context.Context, cfg config.QdrantConfig) (*qdrant.Client, error) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   cfg.Host,
		Port:   cfg.Port,
		APIKey: cfg.APIKey,
		UseTLS: cfg.UseTLS,
		// The built-in version check blocks for up to a minute and ignores ctx;
		// the health check below respects the caller's deadline instead.
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	if _, err := client.HealthCheck(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("health check: %w", err)
	}
	return client, nil
}
