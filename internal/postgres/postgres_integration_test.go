//go:build integration

// These tests need a running PostgreSQL server and are excluded from
// `go test ./...` by the integration build tag. Run them with:
//
//	TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/searchdb \
//	    go test -tags=integration ./internal/postgres/
package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
)

func integrationConfig(t *testing.T) config.PostgreSQLConfig {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run integration tests")
	}
	return config.PostgreSQLConfig{
		URL:               url,
		MaxConns:          4,
		MinConns:          1,
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   30 * time.Minute,
		HealthCheckPeriod: time.Minute,
		ConnectTimeout:    5 * time.Second,
	}
}

func TestIntegrationNewPingsAndCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := New(ctx, integrationConfig(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := pool.Config().MaxConns; got != 4 {
		t.Errorf("MaxConns = %d, want 4", got)
	}
}

func TestIntegrationPoolHandlesConcurrentQueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := New(ctx, integrationConfig(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pool.Close()

	const queries = 20
	var wg sync.WaitGroup
	errs := make(chan error, queries)
	for range queries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n int
			if err := pool.QueryRow(ctx, "SELECT 1").Scan(&n); err != nil {
				errs <- err
				return
			}
			if n != 1 {
				errs <- errors.New("SELECT 1 did not return 1")
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("query failed: %v", err)
	}
	// The pool must not open more connections than it was configured for,
	// however many callers use it at once.
	if total := pool.Stat().TotalConns(); total > 4 {
		t.Errorf("pool opened %d connections, want at most 4", total)
	}
}
