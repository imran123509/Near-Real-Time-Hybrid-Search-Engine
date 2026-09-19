package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// requiredVars are the variables Load refuses to run without.
var requiredVars = map[string]string{
	"DATABASE_URL":         "postgres://user:pass@localhost:5432/searchdb",
	"KAFKA_BROKERS":        "localhost:9092",
	"KAFKA_TOPIC":          "document-events",
	"KAFKA_CONSUMER_GROUP": "near-realtime-search",
	"OPENSEARCH_URL":       "http://localhost:9200",
	"QDRANT_URL":           "http://localhost:6334",
}

// optionalVars is every other variable Load reads. Tests set them all so that
// results never depend on the developer's own environment.
var optionalVars = []string{
	"APP_ENV", "APP_NAME",
	"HTTP_HOST", "HTTP_PORT",
	"HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "HTTP_IDLE_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT",
	"DATABASE_MAX_CONNS", "DATABASE_MIN_CONNS", "DATABASE_MAX_CONN_LIFETIME",
	"DATABASE_MAX_CONN_IDLE_TIME", "DATABASE_HEALTH_CHECK_PERIOD", "DATABASE_CONNECT_TIMEOUT",
	"KAFKA_DLQ_TOPIC", "KAFKA_WORKERS",
	"OPENSEARCH_USERNAME", "OPENSEARCH_PASSWORD", "OPENSEARCH_INDEX",
	"QDRANT_API_KEY", "QDRANT_COLLECTION", "QDRANT_VECTOR_SIZE",
	"INDEXING_WORKERS", "INDEXING_QUEUE_SIZE", "INDEXING_BATCH_SIZE", "INDEXING_RETRY_ATTEMPTS",
	"GEMINI_API_KEY", "GEMINI_EMBEDDING_MODEL", "GEMINI_EMBEDDING_DIMENSIONS",
}

// setEnv gives every variable a known value: the required ones get valid
// values, the optional ones are emptied so defaults apply, then overrides win.
func setEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	for key, value := range requiredVars {
		t.Setenv(key, value)
	}
	for _, key := range optionalVars {
		t.Setenv(key, "")
	}
	for key, value := range overrides {
		t.Setenv(key, value)
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	setEnv(t, nil)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"App.Env", cfg.App.Env, "development"},
		{"App.Name", cfg.App.Name, "near-realtime-search"},
		{"Server.Addr", cfg.Server.Addr(), "0.0.0.0:8080"},
		{"Server.Port", cfg.Server.Port, 8080},
		{"Server.ReadTimeout", cfg.Server.ReadTimeout, 10 * time.Second},
		{"Server.WriteTimeout", cfg.Server.WriteTimeout, 10 * time.Second},
		{"Server.IdleTimeout", cfg.Server.IdleTimeout, 60 * time.Second},
		{"Server.ShutdownTimeout", cfg.Server.ShutdownTimeout, 10 * time.Second},
		{"PostgreSQL.MaxConns", cfg.PostgreSQL.MaxConns, 25},
		{"PostgreSQL.MinConns", cfg.PostgreSQL.MinConns, 2},
		{"PostgreSQL.MaxConnLifetime", cfg.PostgreSQL.MaxConnLifetime, time.Hour},
		{"PostgreSQL.MaxConnIdleTime", cfg.PostgreSQL.MaxConnIdleTime, 30 * time.Minute},
		{"PostgreSQL.HealthCheckPeriod", cfg.PostgreSQL.HealthCheckPeriod, time.Minute},
		{"PostgreSQL.ConnectTimeout", cfg.PostgreSQL.ConnectTimeout, 5 * time.Second},
		{"Kafka.DLQTopic", cfg.Kafka.DLQTopic, "document-events.dlq"},
		{"Kafka.Workers", cfg.Kafka.Workers, 10},
		{"OpenSearch.Index", cfg.OpenSearch.Index, "documents"},
		{"Qdrant.Collection", cfg.Qdrant.Collection, "documents"},
		{"Qdrant.Host", cfg.Qdrant.Host, "localhost"},
		{"Qdrant.Port", cfg.Qdrant.Port, 6334},
		{"Qdrant.UseTLS", cfg.Qdrant.UseTLS, false},
		{"Qdrant.VectorSize", cfg.Qdrant.VectorSize, 768},
		{"Indexing.Workers", cfg.Indexing.Workers, 10},
		{"Indexing.QueueSize", cfg.Indexing.QueueSize, 1000},
		{"Indexing.BatchSize", cfg.Indexing.BatchSize, 100},
		{"Indexing.RetryAttempts", cfg.Indexing.RetryAttempts, 3},
		{"Gemini.Model", cfg.Gemini.Model, "gemini-embedding-2"},
		{"Gemini.Dimensions", cfg.Gemini.Dimensions, 768},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadSuccess(t *testing.T) {
	setEnv(t, map[string]string{
		"APP_ENV":                     "production",
		"APP_NAME":                    "search",
		"HTTP_HOST":                   "127.0.0.1",
		"HTTP_PORT":                   "9090",
		"HTTP_READ_TIMEOUT":           "5s",
		"HTTP_WRITE_TIMEOUT":          "15s",
		"HTTP_IDLE_TIMEOUT":           "1m30s",
		"HTTP_SHUTDOWN_TIMEOUT":       "20s",
		"KAFKA_DLQ_TOPIC":             "document-events.failed",
		"KAFKA_WORKERS":               "6",
		"OPENSEARCH_URL":              "https://opensearch.internal:9200",
		"OPENSEARCH_USERNAME":         "search",
		"OPENSEARCH_PASSWORD":         "not-a-real-password",
		"OPENSEARCH_INDEX":            "docs-v2",
		"QDRANT_URL":                  "https://qdrant.internal:6334",
		"QDRANT_API_KEY":              "not-a-real-key",
		"QDRANT_COLLECTION":           "docs-v2",
		"DATABASE_MAX_CONNS":          "40",
		"DATABASE_MIN_CONNS":          "5",
		"DATABASE_MAX_CONN_LIFETIME":  "2h",
		"DATABASE_CONNECT_TIMEOUT":    "3s",
		"INDEXING_WORKERS":            "4",
		"INDEXING_QUEUE_SIZE":         "200",
		"INDEXING_BATCH_SIZE":         "50",
		"INDEXING_RETRY_ATTEMPTS":     "5",
		"GEMINI_API_KEY":              "not-a-real-key",
		"GEMINI_EMBEDDING_MODEL":      "gemini-embedding-001",
		"GEMINI_EMBEDDING_DIMENSIONS": "1536",
		"QDRANT_VECTOR_SIZE":          "1536",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Server.Addr() != "127.0.0.1:9090" || cfg.Server.IdleTimeout != 90*time.Second {
		t.Errorf("Server = %+v", cfg.Server)
	}
	if cfg.PostgreSQL.URL != requiredVars["DATABASE_URL"] {
		t.Errorf("PostgreSQL.URL = %q", cfg.PostgreSQL.URL)
	}
	if cfg.PostgreSQL.MaxConns != 40 || cfg.PostgreSQL.MinConns != 5 ||
		cfg.PostgreSQL.MaxConnLifetime != 2*time.Hour || cfg.PostgreSQL.ConnectTimeout != 3*time.Second {
		t.Errorf("PostgreSQL pool settings = %+v", cfg.PostgreSQL)
	}
	if cfg.Kafka.ConsumerGroup != "near-realtime-search" || cfg.Kafka.DLQTopic != "document-events.failed" || cfg.Kafka.Workers != 6 {
		t.Errorf("Kafka = %+v", cfg.Kafka)
	}
	if cfg.OpenSearch.Username != "search" || cfg.OpenSearch.Password != "not-a-real-password" || cfg.OpenSearch.Index != "docs-v2" {
		t.Errorf("OpenSearch = %+v", cfg.OpenSearch)
	}
	if cfg.Qdrant.Host != "qdrant.internal" || cfg.Qdrant.Port != 6334 || !cfg.Qdrant.UseTLS ||
		cfg.Qdrant.APIKey != "not-a-real-key" || cfg.Qdrant.VectorSize != 1536 {
		t.Errorf("Qdrant = %+v", cfg.Qdrant)
	}
	want := IndexingConfig{Workers: 4, QueueSize: 200, BatchSize: 50, RetryAttempts: 5}
	if cfg.Indexing != want {
		t.Errorf("Indexing = %+v, want %+v", cfg.Indexing, want)
	}
	if cfg.Gemini.Model != "gemini-embedding-001" || cfg.Gemini.Dimensions != 1536 {
		t.Errorf("Gemini = %+v", cfg.Gemini)
	}
}

func TestLoadRequiresVariables(t *testing.T) {
	for key := range requiredVars {
		t.Run(key, func(t *testing.T) {
			setEnv(t, map[string]string{key: ""})

			_, err := Load()
			if err == nil {
				t.Fatalf("Load succeeded without %s", key)
			}
			if !strings.Contains(err.Error(), key) {
				t.Errorf("error %q does not mention %s", err, key)
			}
		})
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name, key, value, wantInError string
	}{
		{"integer", "KAFKA_WORKERS", "abc", "invalid KAFKA_WORKERS"},
		{"integer with unit", "INDEXING_QUEUE_SIZE", "1k", "invalid INDEXING_QUEUE_SIZE"},
		{"duration", "HTTP_READ_TIMEOUT", "10 seconds", "invalid HTTP_READ_TIMEOUT"},
		{"duration unit missing", "HTTP_SHUTDOWN_TIMEOUT", "30", "invalid HTTP_SHUTDOWN_TIMEOUT"},
		{"zero workers", "INDEXING_WORKERS", "0", "INDEXING_WORKERS must be positive"},
		{"negative workers", "INDEXING_WORKERS", "-4", "INDEXING_WORKERS must be positive"},
		{"zero kafka workers", "KAFKA_WORKERS", "0", "KAFKA_WORKERS must be positive"},
		{"zero db max conns", "DATABASE_MAX_CONNS", "0", "DATABASE_MAX_CONNS must be positive"},
		{"negative db min conns", "DATABASE_MIN_CONNS", "-1", "DATABASE_MIN_CONNS must not be negative"},
		{"db min above max", "DATABASE_MIN_CONNS", "50", "must not exceed DATABASE_MAX_CONNS"},
		{"invalid db lifetime", "DATABASE_MAX_CONN_LIFETIME", "1 hour", "invalid DATABASE_MAX_CONN_LIFETIME"},
		{"zero db connect timeout", "DATABASE_CONNECT_TIMEOUT", "0s", "DATABASE_CONNECT_TIMEOUT must be positive"},
		{"invalid vector size", "QDRANT_VECTOR_SIZE", "big", "invalid QDRANT_VECTOR_SIZE"},
		{"zero vector size", "QDRANT_VECTOR_SIZE", "0", "QDRANT_VECTOR_SIZE must be positive"},
		{"vector size differs from embedding model", "QDRANT_VECTOR_SIZE", "384", "must match GEMINI_EMBEDDING_DIMENSIONS"},
		{"db min conns may be zero", "DATABASE_MIN_CONNS", "0", ""},
		{"zero queue size", "INDEXING_QUEUE_SIZE", "0", "INDEXING_QUEUE_SIZE must be positive"},
		{"zero retry attempts", "INDEXING_RETRY_ATTEMPTS", "0", "INDEXING_RETRY_ATTEMPTS must be positive"},
		{"negative timeout", "HTTP_IDLE_TIMEOUT", "-5s", "HTTP_IDLE_TIMEOUT must be positive"},
		{"port out of range", "HTTP_PORT", "70000", "HTTP_PORT must be between"},
		{"opensearch url without scheme", "OPENSEARCH_URL", "localhost:9200", "invalid OPENSEARCH_URL"},
		{"qdrant url without scheme", "QDRANT_URL", "localhost:6334", "invalid QDRANT_URL"},
		{"qdrant url with bad scheme", "QDRANT_URL", "ftp://localhost:6334", "invalid QDRANT_URL"},
		{"empty index name is a default, not an error", "OPENSEARCH_INDEX", "documents", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, map[string]string{tt.key: tt.value})

			_, err := Load()
			if tt.wantInError == "" {
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Load succeeded with %s=%q", tt.key, tt.value)
			}
			if !strings.Contains(err.Error(), tt.wantInError) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantInError)
			}
		})
	}
}

func TestLoadParsesMultipleKafkaBrokers(t *testing.T) {
	setEnv(t, map[string]string{
		"KAFKA_BROKERS": " broker-1:9092, broker-2:9092 ,,broker-3:9092 ",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"broker-1:9092", "broker-2:9092", "broker-3:9092"}
	if !slices.Equal(cfg.Kafka.Brokers, want) {
		t.Errorf("Brokers = %q, want %q", cfg.Kafka.Brokers, want)
	}
}

func TestLoadReportsEveryProblem(t *testing.T) {
	setEnv(t, map[string]string{
		"DATABASE_URL":      "",
		"KAFKA_TOPIC":       "",
		"KAFKA_WORKERS":     "abc",
		"HTTP_READ_TIMEOUT": "nope",
	})

	_, err := Load()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"DATABASE_URL", "KAFKA_TOPIC", "KAFKA_WORKERS", "HTTP_READ_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestQdrantURLWithoutPortUsesGRPCDefault(t *testing.T) {
	tests := []struct {
		url      string
		wantHost string
		wantPort int
		wantTLS  bool
	}{
		{"http://localhost", "localhost", 6334, false},
		{"https://qdrant.example.com", "qdrant.example.com", 6334, true},
		{"grpcs://qdrant.example.com:7000", "qdrant.example.com", 7000, true},
		{"grpc://127.0.0.1:6334", "127.0.0.1", 6334, false},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			setEnv(t, map[string]string{"QDRANT_URL": tt.url})

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Qdrant.Host != tt.wantHost || cfg.Qdrant.Port != tt.wantPort || cfg.Qdrant.UseTLS != tt.wantTLS {
				t.Errorf("got %s:%d tls=%v, want %s:%d tls=%v",
					cfg.Qdrant.Host, cfg.Qdrant.Port, cfg.Qdrant.UseTLS, tt.wantHost, tt.wantPort, tt.wantTLS)
			}
		})
	}
}

func TestQdrantVectorSizeFollowsEmbeddingModel(t *testing.T) {
	setEnv(t, map[string]string{"GEMINI_EMBEDDING_DIMENSIONS": "1024"})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Qdrant.VectorSize != 1024 {
		t.Errorf("Qdrant.VectorSize = %d, want 1024 from GEMINI_EMBEDDING_DIMENSIONS", cfg.Qdrant.VectorSize)
	}
}
