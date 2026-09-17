package search

import (
	"context"
	"fmt"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"near-real-time-hybrid-search-engine/internal/config"
)

// NewOpenSearchClient creates an OpenSearch client and verifies the cluster is
// reachable. The caller owns the client and must call Close.
func NewOpenSearchClient(ctx context.Context, cfg config.OpenSearchConfig) (*opensearchapi.Client, error) {
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: cfg.Addresses,
			Username:  cfg.Username,
			Password:  cfg.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	if _, err := client.Info(ctx, nil); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return client, nil
}
