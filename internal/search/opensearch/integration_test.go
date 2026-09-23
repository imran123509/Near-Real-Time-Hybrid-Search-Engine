//go:build integration

// These tests need a running OpenSearch cluster and are excluded from
// `go test ./...` by the integration build tag. Each run uses its own
// throwaway index and deletes it afterwards. Run them with:
//
//	OPENSEARCH_TEST_URL=http://localhost:9200 \
//	    go test -tags=integration ./internal/search/opensearch/
//
// Set OPENSEARCH_TEST_USERNAME and OPENSEARCH_TEST_PASSWORD if the cluster
// requires authentication.
package opensearch

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"near-real-time-hybrid-search-engine/internal/config"
)

func newIntegrationClient(t *testing.T) (*Client, context.Context) {
	t.Helper()
	url := os.Getenv("OPENSEARCH_TEST_URL")
	if url == "" {
		t.Skip("set OPENSEARCH_TEST_URL to run integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	c, err := New(config.OpenSearchConfig{
		URL:      url,
		Username: os.Getenv("OPENSEARCH_TEST_USERNAME"),
		Password: os.Getenv("OPENSEARCH_TEST_PASSWORD"),
		Index:    fmt.Sprintf("test-documents-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_, _ = c.api.Indices.Delete(context.Background(), opensearchapi.IndicesDeleteReq{Indices: []string{c.index}})
		_ = c.Close()
	})

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := c.EnsureIndex(ctx); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	return c, ctx
}

// refresh makes recent writes searchable now instead of within about a second.
func refresh(t *testing.T, ctx context.Context, c *Client) {
	t.Helper()
	if _, err := c.api.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{Index: []string{c.index}}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
}

func TestIntegrationIndexSearchDelete(t *testing.T) {
	c, ctx := newIntegrationClient(t)

	// A second call must leave the existing index alone.
	if err := c.EnsureIndex(ctx); err != nil {
		t.Fatalf("EnsureIndex on existing index: %v", err)
	}

	docs := []Document{
		{ID: "doc-1", Title: "Go channels", Content: "Channels let goroutines communicate.", UpdatedAt: time.Now().UTC()},
		{ID: "doc-2", Title: "Postgres indexes", Content: "B-tree indexes speed up lookups in Go services.", UpdatedAt: time.Now().UTC()},
	}
	for _, d := range docs {
		if _, err := c.IndexDocument(ctx, d); err != nil {
			t.Fatalf("IndexDocument %s: %v", d.ID, err)
		}
	}
	// Indexing the same ID again replaces the document instead of duplicating it.
	if _, err := c.IndexDocument(ctx, docs[0]); err != nil {
		t.Fatalf("re-index: %v", err)
	}
	refresh(t, ctx, c)

	results, err := c.Search(ctx, "go channels", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2 (no duplicates): %+v", len(results), results)
	}
	if results[0].ID != "doc-1" || results[0].Score <= results[1].Score {
		t.Errorf("ranking = %s (%.3f), %s (%.3f); want doc-1 first", results[0].ID, results[0].Score, results[1].ID, results[1].Score)
	}

	if err := c.DeleteDocument(ctx, "doc-1"); err != nil {
		t.Fatalf("DeleteDocument: %v", err)
	}
	if err := c.DeleteDocument(ctx, "doc-1"); err != nil {
		t.Fatalf("second DeleteDocument should succeed: %v", err)
	}
	refresh(t, ctx, c)

	results, err = c.Search(ctx, "channels", 10)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	for _, r := range results {
		if r.ID == "doc-1" {
			t.Fatal("deleted document is still searchable")
		}
	}
}

func TestIntegrationStaleVersionIsIgnored(t *testing.T) {
	c, ctx := newIntegrationClient(t)

	newer := Document{ID: "doc-v", Title: "new title", Content: "current", Version: 5}
	older := Document{ID: "doc-v", Title: "old title", Content: "stale", Version: 4}

	if result, err := c.IndexDocument(ctx, newer); err != nil || result != WriteApplied {
		t.Fatalf("index v5 = %s, %v; want applied", result, err)
	}
	// An event the index has moved past: it must be refused, and the caller
	// must be told, so it does not go on to write the stale vector.
	if result, err := c.IndexDocument(ctx, older); err != nil || result != WriteStale {
		t.Fatalf("index v4 = %s, %v; want stale", result, err)
	}
	// The same event again is a duplicate, not stale: an earlier attempt may
	// have failed after this write, so the caller must finish its work.
	if result, err := c.IndexDocument(ctx, newer); err != nil || result != WriteDuplicate {
		t.Fatalf("re-index v5 = %s, %v; want duplicate", result, err)
	}
	if version, found, err := c.DocumentVersion(ctx, newer.ID); err != nil || !found || version != 5 {
		t.Fatalf("DocumentVersion = %d, %v, %v; want 5, true, nil", version, found, err)
	}
	refresh(t, ctx, c)

	results, err := c.Search(ctx, "title", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 || results[0].Document.Title != "new title" {
		t.Fatalf("results = %+v, want only the version 5 document", results)
	}
}
