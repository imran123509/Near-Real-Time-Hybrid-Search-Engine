//go:build integration

// These tests need a running Qdrant and are excluded from `go test ./...` by
// the integration build tag. Each run uses its own throwaway collection and
// deletes it afterwards. Run them with:
//
//	QDRANT_TEST_URL=http://localhost:6334 \
//	    go test -tags=integration ./internal/search/qdrant/
//
// QDRANT_TEST_URL is the gRPC endpoint (port 6334 by default). Set
// QDRANT_TEST_API_KEY if the server requires one.
package qdrant

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
)

func integrationConfig(t *testing.T, vectorSize int) config.QdrantConfig {
	t.Helper()
	raw := os.Getenv("QDRANT_TEST_URL")
	if raw == "" {
		t.Skip("set QDRANT_TEST_URL to run integration tests")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("QDRANT_TEST_URL: %v", err)
	}
	port := 6334
	if p := u.Port(); p != "" {
		if port, err = strconv.Atoi(p); err != nil {
			t.Fatalf("QDRANT_TEST_URL port: %v", err)
		}
	}
	return config.QdrantConfig{
		Host:       u.Hostname(),
		Port:       port,
		UseTLS:     u.Scheme == "https",
		APIKey:     os.Getenv("QDRANT_TEST_API_KEY"),
		Collection: fmt.Sprintf("test-documents-%d", time.Now().UnixNano()),
		VectorSize: vectorSize,
	}
}

func newIntegrationClient(t *testing.T, cfg config.QdrantConfig) (*Client, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = c.api.DeleteCollection(context.Background(), cfg.Collection)
		_ = c.Close()
	})
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := c.EnsureCollection(ctx); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}
	return c, ctx
}

func TestIntegrationUpsertSearchDelete(t *testing.T) {
	c, ctx := newIntegrationClient(t, integrationConfig(t, 4))

	// A second call must leave the existing collection alone.
	if err := c.EnsureCollection(ctx); err != nil {
		t.Fatalf("EnsureCollection on existing collection: %v", err)
	}

	points := []Point{
		{ID: "doc-go", Vector: []float32{0.9, 0.1, 0, 0}, Payload: map[string]any{"title": "Go channels"}},
		{ID: "doc-sql", Vector: []float32{0, 0, 0.9, 0.1}, Payload: map[string]any{"title": "Postgres indexes"}},
	}
	for _, p := range points {
		if err := c.Upsert(ctx, p); err != nil {
			t.Fatalf("Upsert %s: %v", p.ID, err)
		}
	}
	// Upserting the same ID again replaces the point instead of adding one.
	updated := points[0]
	updated.Payload = map[string]any{"title": "Go channels, revised"}
	if err := c.Upsert(ctx, updated); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}

	results, err := c.Search(ctx, []float32{1, 0, 0, 0}, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2 (no duplicates): %+v", len(results), results)
	}
	if results[0].ID != "doc-go" || results[0].Payload["title"] != "Go channels, revised" {
		t.Errorf("top result = %+v, want the revised doc-go", results[0])
	}
	if results[0].Score <= results[1].Score {
		t.Errorf("scores not in descending order: %v, %v", results[0].Score, results[1].Score)
	}

	if err := c.Delete(ctx, "doc-go"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := c.Delete(ctx, "doc-go"); err != nil {
		t.Fatalf("second Delete should succeed: %v", err)
	}
	results, err = c.Search(ctx, []float32{1, 0, 0, 0}, 10)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-sql" {
		t.Errorf("results after delete = %+v, want only doc-sql", results)
	}
}

func TestIntegrationRejectsMismatchedCollection(t *testing.T) {
	cfg := integrationConfig(t, 4)
	newIntegrationClient(t, cfg) // creates the collection with 4 dimensions

	cfg.VectorSize = 8
	other, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer other.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := other.EnsureCollection(ctx); err == nil {
		t.Fatal("EnsureCollection accepted a collection with a different vector size")
	}
}
