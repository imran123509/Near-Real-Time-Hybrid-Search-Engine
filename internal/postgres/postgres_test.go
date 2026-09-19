package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
)

// testConfig is a valid pool configuration; tests change one field at a time.
func testConfig() config.PostgreSQLConfig {
	return config.PostgreSQLConfig{
		URL:               "postgres://user:pass@localhost:5432/searchdb",
		MaxConns:          25,
		MinConns:          2,
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   30 * time.Minute,
		HealthCheckPeriod: time.Minute,
		ConnectTimeout:    5 * time.Second,
	}
}

func TestPoolConfigAppliesSettings(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConns = 12
	cfg.MinConns = 3

	got, err := poolConfig(cfg)
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}

	if got.MaxConns != 12 || got.MinConns != 3 {
		t.Errorf("MaxConns = %d, MinConns = %d, want 12 and 3", got.MaxConns, got.MinConns)
	}
	if got.MaxConnLifetime != time.Hour || got.MaxConnIdleTime != 30*time.Minute {
		t.Errorf("lifetime = %s, idle = %s", got.MaxConnLifetime, got.MaxConnIdleTime)
	}
	if got.MaxConnLifetimeJitter != 6*time.Minute {
		t.Errorf("MaxConnLifetimeJitter = %s, want a tenth of the lifetime", got.MaxConnLifetimeJitter)
	}
	if got.HealthCheckPeriod != time.Minute {
		t.Errorf("HealthCheckPeriod = %s", got.HealthCheckPeriod)
	}
	if got.ConnConfig.ConnectTimeout != 5*time.Second {
		t.Errorf("ConnectTimeout = %s", got.ConnConfig.ConnectTimeout)
	}
	if got.ConnConfig.Database != "searchdb" {
		t.Errorf("Database = %q, want the value from the URL", got.ConnConfig.Database)
	}
}

func TestPoolConfigRejectsInvalidSettings(t *testing.T) {
	tests := []struct {
		name        string
		change      func(*config.PostgreSQLConfig)
		wantInError string
	}{
		{"no url", func(c *config.PostgreSQLConfig) { c.URL = "" }, "postgres url is required"},
		{"zero max conns", func(c *config.PostgreSQLConfig) { c.MaxConns = 0 }, "max conns must be between"},
		{"negative max conns", func(c *config.PostgreSQLConfig) { c.MaxConns = -1 }, "max conns must be between"},
		{"too many max conns", func(c *config.PostgreSQLConfig) { c.MaxConns = maxPoolConns + 1 }, "max conns must be between"},
		{"negative min conns", func(c *config.PostgreSQLConfig) { c.MinConns = -1 }, "min conns must not be negative"},
		{"min above max", func(c *config.PostgreSQLConfig) { c.MinConns = 26 }, "must not exceed max conns"},
		{"zero lifetime", func(c *config.PostgreSQLConfig) { c.MaxConnLifetime = 0 }, "conn lifetime must be positive"},
		{"zero idle time", func(c *config.PostgreSQLConfig) { c.MaxConnIdleTime = 0 }, "conn idle time must be positive"},
		{"zero health check", func(c *config.PostgreSQLConfig) { c.HealthCheckPeriod = 0 }, "health check period must be positive"},
		{"zero connect timeout", func(c *config.PostgreSQLConfig) { c.ConnectTimeout = -time.Second }, "connect timeout must be positive"},
		{"unparsable url", func(c *config.PostgreSQLConfig) { c.URL = "://nope" }, "parse postgres url"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.change(&cfg)

			if _, err := poolConfig(cfg); err == nil {
				t.Fatal("expected an error")
			} else if !strings.Contains(err.Error(), tt.wantInError) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantInError)
			}
		})
	}
}

// TestPoolConfigErrorHidesConnectionString checks that a rejected URL never
// reaches the error message, so logging the error cannot leak credentials.
func TestPoolConfigErrorHidesConnectionString(t *testing.T) {
	tests := []struct {
		name, url, wantReason string
	}{
		{"bad port", "postgres://user:sup3r-secret@localhost:not-a-port/searchdb", "invalid port"},
		{"bad sslmode", "postgres://user:sup3r-secret@localhost:5432/db?sslmode=bogus", "failed to configure TLS"},
		{"keyword value form", "host=localhost password=sup3r-secret sslmode=bogus", "failed to configure TLS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.URL = tt.url

			_, err := poolConfig(cfg)
			if err == nil {
				t.Fatal("expected an error")
			}
			if strings.Contains(err.Error(), "sup3r-secret") {
				t.Errorf("error leaks the password: %q", err)
			}
			if strings.Contains(err.Error(), "localhost") {
				t.Errorf("error repeats the connection string: %q", err)
			}
			if !strings.Contains(err.Error(), tt.wantReason) {
				t.Errorf("error = %q, want it to explain %q", err, tt.wantReason)
			}
		})
	}
}

func TestNewFailsWhenDatabaseIsUnavailable(t *testing.T) {
	cfg := testConfig()
	// Nothing listens here, so connecting is refused immediately. No server,
	// container or network access is involved.
	cfg.URL = "postgres://user:pass@127.0.0.1:59999/searchdb"
	cfg.ConnectTimeout = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := New(ctx, cfg)
	if err == nil {
		pool.Close()
		t.Fatal("New succeeded although PostgreSQL is unavailable")
	}
	if !strings.Contains(err.Error(), "ping postgres") {
		t.Errorf("error = %q, want it to report the failed ping", err)
	}
}

func TestNewFailsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pool, err := New(ctx, testConfig())
	if err == nil {
		pool.Close()
		t.Fatal("New succeeded with a cancelled context")
	}
}

func TestNewRejectsInvalidConfigBeforeConnecting(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConns = 0

	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("expected an error")
	}
}

// TestPoolConfigUsesLoadedEnvironment checks that the pool settings loaded from
// the environment reach the pool.
func TestPoolConfigUsesLoadedEnvironment(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/searchdb")
	t.Setenv("DATABASE_MAX_CONNS", "7")
	t.Setenv("DATABASE_MIN_CONNS", "3")
	t.Setenv("DATABASE_MAX_CONN_LIFETIME", "45m")
	t.Setenv("DATABASE_CONNECT_TIMEOUT", "3s")
	// Required by config.Load, but unrelated to the pool.
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC", "document-events")
	t.Setenv("KAFKA_CONSUMER_GROUP", "near-realtime-search")
	t.Setenv("OPENSEARCH_URL", "http://localhost:9200")
	t.Setenv("QDRANT_URL", "http://localhost:6334")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	got, err := poolConfig(cfg.PostgreSQL)
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}

	if got.MaxConns != 7 || got.MinConns != 3 {
		t.Errorf("MaxConns = %d, MinConns = %d, want 7 and 3", got.MaxConns, got.MinConns)
	}
	if got.MaxConnLifetime != 45*time.Minute {
		t.Errorf("MaxConnLifetime = %s, want 45m", got.MaxConnLifetime)
	}
	if got.ConnConfig.ConnectTimeout != 3*time.Second {
		t.Errorf("ConnectTimeout = %s, want 3s", got.ConnConfig.ConnectTimeout)
	}
}
