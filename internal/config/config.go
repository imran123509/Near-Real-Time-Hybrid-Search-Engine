// Package config loads and validates application settings from environment
// variables. It is shared by the API and the consumer, and depends on nothing
// but the standard library.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full set of settings for both services. Each service reads
// only the groups it needs: the API uses App, Server, PostgreSQL, OpenSearch,
// Qdrant, Embedding and Search; the consumer uses App, Kafka, Indexing,
// PostgreSQL, OpenSearch, Qdrant and Embedding.
type Config struct {
	App        AppConfig
	Server     ServerConfig
	PostgreSQL PostgreSQLConfig
	Kafka      KafkaConfig
	OpenSearch OpenSearchConfig
	Qdrant     QdrantConfig
	Indexing   IndexingConfig
	Metrics    MetricsConfig
	Embedding  EmbeddingConfig
	Search     SearchConfig
}

// AppConfig identifies the running service.
type AppConfig struct {
	Env  string // APP_ENV
	Name string // APP_NAME
	// StartupTimeout is how long a service waits for its dependencies to
	// accept connections before giving up. Each is retried with backoff until
	// then, so services tolerate starting before their dependencies.
	StartupTimeout time.Duration // APP_STARTUP_TIMEOUT
	// LogLevel is the lowest level logged: debug, info, warn or error.
	LogLevel slog.Level // LOG_LEVEL
}

// ServerConfig holds HTTP server settings for the API.
type ServerConfig struct {
	Host            string        // HTTP_HOST
	Port            int           // HTTP_PORT
	ReadTimeout     time.Duration // HTTP_READ_TIMEOUT
	WriteTimeout    time.Duration // HTTP_WRITE_TIMEOUT
	IdleTimeout     time.Duration // HTTP_IDLE_TIMEOUT
	ShutdownTimeout time.Duration // HTTP_SHUTDOWN_TIMEOUT
}

// Addr returns the host:port the HTTP server listens on.
func (c ServerConfig) Addr() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// PostgreSQLConfig holds the database connection string and pool settings.
// URL contains credentials, so never log it.
type PostgreSQLConfig struct {
	URL string // DATABASE_URL

	MaxConns int // DATABASE_MAX_CONNS
	MinConns int // DATABASE_MIN_CONNS, warm connections kept open
	// MaxConnLifetime is how long a connection may be reused before it is
	// replaced, which also lets connections move to restarted database nodes.
	MaxConnLifetime   time.Duration // DATABASE_MAX_CONN_LIFETIME
	MaxConnIdleTime   time.Duration // DATABASE_MAX_CONN_IDLE_TIME
	HealthCheckPeriod time.Duration // DATABASE_HEALTH_CHECK_PERIOD
	ConnectTimeout    time.Duration // DATABASE_CONNECT_TIMEOUT
}

// KafkaConfig holds consumer settings.
type KafkaConfig struct {
	Brokers       []string // KAFKA_BROKERS, comma-separated
	Topic         string   // KAFKA_TOPIC
	ConsumerGroup string   // KAFKA_CONSUMER_GROUP
	DLQTopic      string   // KAFKA_DLQ_TOPIC, defaults to Topic + ".dlq"
	Workers       int      // KAFKA_WORKERS
	Retry         RetryConfig
}

// RetryConfig decides how often and how far apart a failed message is retried
// before it is sent to the dead-letter topic.
//
// The delay before attempt n is InitialBackoff × Multiplier^(n-1), never more
// than MaxBackoff. With the defaults that is 500ms, 1s, 2s, ... up to 30s.
//
// The values trade recovery against lag: a store that is restarting usually
// comes back within seconds, so a few attempts spread over tens of seconds
// recover most failures, while the worker handling the message is blocked for
// that whole time and its partition falls further behind. Raising MaxAttempts
// or MaxBackoff buys patience with lag.
type RetryConfig struct {
	// MaxAttempts is the total number of tries per message, the first one
	// included, so 1 disables retrying.
	MaxAttempts int // KAFKA_RETRY_MAX_ATTEMPTS
	// InitialBackoff is the wait before the second attempt.
	InitialBackoff time.Duration // KAFKA_RETRY_INITIAL_BACKOFF
	// MaxBackoff caps the wait however many attempts have failed.
	MaxBackoff time.Duration // KAFKA_RETRY_MAX_BACKOFF
	// Multiplier is how much each wait grows; 1 keeps it constant.
	Multiplier float64 // KAFKA_RETRY_MULTIPLIER
}

// OpenSearchConfig holds OpenSearch connection settings. Password is a secret,
// so never log it.
type OpenSearchConfig struct {
	URL      string // OPENSEARCH_URL
	Username string // OPENSEARCH_USERNAME
	Password string // OPENSEARCH_PASSWORD
	Index    string // OPENSEARCH_INDEX
}

// QdrantConfig holds Qdrant connection settings. APIKey is a secret, so never
// log it.
//
// URL must point at Qdrant's gRPC endpoint, which listens on port 6334 by
// default, not the REST endpoint on 6333, because the client speaks gRPC.
// Host, Port and UseTLS are derived from URL by Load.
type QdrantConfig struct {
	URL        string // QDRANT_URL
	APIKey     string // QDRANT_API_KEY
	Collection string // QDRANT_COLLECTION
	// VectorSize is the dimension of every stored vector. It must equal the
	// embedding model's output size, so it defaults to EMBEDDING_DIMENSION
	// and Load rejects a mismatch.
	VectorSize int // QDRANT_VECTOR_SIZE

	Host   string
	Port   int
	UseTLS bool
}

// IndexingConfig holds worker pool settings for the consumer. How a failed
// message is retried is KafkaConfig.Retry.
type IndexingConfig struct {
	Workers   int // INDEXING_WORKERS
	QueueSize int // INDEXING_QUEUE_SIZE, buffered messages across all workers
	BatchSize int // INDEXING_BATCH_SIZE
}

// MetricsConfig holds the Prometheus endpoint's settings. Metrics are on by
// default: a service that has to be reconfigured before it can be observed
// tends to be observed only after something has already gone wrong.
type MetricsConfig struct {
	Enabled bool   // METRICS_ENABLED
	Path    string // METRICS_PATH
	// Addr is the address the consumer serves metrics on. The API serves them
	// from its own HTTP server and ignores this.
	Addr string // METRICS_ADDR
}

// EmbeddingConfig selects and configures the provider that turns text into
// vectors. It is provider-neutral: switching providers means changing these
// values, not adding a new group. APIKey is a secret, so never log it.
type EmbeddingConfig struct {
	// Provider names the implementation to use, such as "gemini". It is
	// trimmed and lowercased; the embedding package rejects unknown names.
	Provider string // EMBEDDING_PROVIDER
	APIKey   string // EMBEDDING_API_KEY
	Model    string // EMBEDDING_MODEL
	// Dimension is the length of every vector. The provider is asked for
	// exactly this many values and rejects a response of any other length,
	// so that a wrong-sized vector never reaches Qdrant. Changing the model
	// may change it, and then every stored vector has to be re-embedded.
	Dimension int // EMBEDDING_DIMENSION
	// Timeout bounds a single embedding request, network round trip
	// included. It is applied inside the caller's context, so a shorter
	// caller deadline or a cancellation still wins.
	Timeout time.Duration // EMBEDDING_TIMEOUT
}

// SearchConfig holds hybrid search settings for the API.
type SearchConfig struct {
	// DefaultLimit is the number of results returned when a request does not
	// ask for a specific number.
	DefaultLimit int // SEARCH_DEFAULT_LIMIT
	// MaxLimit is the most results one request may ask for.
	MaxLimit int // SEARCH_MAX_LIMIT
	// CandidateLimit is how many results are fetched from each retriever
	// before fusion. Fusing only the final page from each would drop documents
	// that rank moderately in both lists, which is exactly what RRF rewards.
	CandidateLimit int // SEARCH_CANDIDATE_LIMIT
	// RRFK is the Reciprocal Rank Fusion constant; the default is rrf.DefaultK.
	RRFK int // RRF_K
	// Timeout bounds one search request, all dependency calls included. It
	// must be shorter than HTTP_WRITE_TIMEOUT so that a timed-out search can
	// still be answered with an error instead of a dropped connection.
	Timeout time.Duration // SEARCH_TIMEOUT
}

// Load reads the environment, applies defaults, parses values and validates
// them. It reports every problem it finds rather than only the first, and
// never logs secrets.
//
// Required: DATABASE_URL, KAFKA_BROKERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP,
// OPENSEARCH_URL, QDRANT_URL. Both services also need EMBEDDING_API_KEY: the
// consumer embeds documents and the API embeds queries. The embedding provider
// checks it when it is created.
func Load() (Config, error) {
	var e env

	cfg := Config{
		App: AppConfig{
			Env:            getEnv("APP_ENV", "development"),
			Name:           getEnv("APP_NAME", "near-realtime-search"),
			StartupTimeout: e.duration("APP_STARTUP_TIMEOUT", 60*time.Second),
			LogLevel:       e.logLevel("LOG_LEVEL", slog.LevelInfo),
		},
		Server: ServerConfig{
			Host:            getEnv("HTTP_HOST", "0.0.0.0"),
			Port:            e.int("HTTP_PORT", 8080),
			ReadTimeout:     e.duration("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:    e.duration("HTTP_WRITE_TIMEOUT", 10*time.Second),
			IdleTimeout:     e.duration("HTTP_IDLE_TIMEOUT", 60*time.Second),
			ShutdownTimeout: e.duration("HTTP_SHUTDOWN_TIMEOUT", 10*time.Second),
		},
		PostgreSQL: PostgreSQLConfig{
			URL:               e.required("DATABASE_URL"),
			MaxConns:          e.int("DATABASE_MAX_CONNS", 25),
			MinConns:          e.int("DATABASE_MIN_CONNS", 2),
			MaxConnLifetime:   e.duration("DATABASE_MAX_CONN_LIFETIME", time.Hour),
			MaxConnIdleTime:   e.duration("DATABASE_MAX_CONN_IDLE_TIME", 30*time.Minute),
			HealthCheckPeriod: e.duration("DATABASE_HEALTH_CHECK_PERIOD", time.Minute),
			ConnectTimeout:    e.duration("DATABASE_CONNECT_TIMEOUT", 5*time.Second),
		},
		Kafka: KafkaConfig{
			Brokers:       e.requiredList("KAFKA_BROKERS"),
			Topic:         e.required("KAFKA_TOPIC"),
			ConsumerGroup: e.required("KAFKA_CONSUMER_GROUP"),
			Workers:       e.int("KAFKA_WORKERS", 10),
			Retry: RetryConfig{
				MaxAttempts:    e.int("KAFKA_RETRY_MAX_ATTEMPTS", 5),
				InitialBackoff: e.duration("KAFKA_RETRY_INITIAL_BACKOFF", 500*time.Millisecond),
				MaxBackoff:     e.duration("KAFKA_RETRY_MAX_BACKOFF", 30*time.Second),
				Multiplier:     e.float("KAFKA_RETRY_MULTIPLIER", 2),
			},
		},
		OpenSearch: OpenSearchConfig{
			URL:      e.required("OPENSEARCH_URL"),
			Username: os.Getenv("OPENSEARCH_USERNAME"),
			Password: os.Getenv("OPENSEARCH_PASSWORD"),
			Index:    getEnv("OPENSEARCH_INDEX", "documents"),
		},
		Qdrant: QdrantConfig{
			URL:        e.required("QDRANT_URL"),
			APIKey:     os.Getenv("QDRANT_API_KEY"),
			Collection: getEnv("QDRANT_COLLECTION", "documents"),
		},
		Indexing: IndexingConfig{
			Workers:   e.int("INDEXING_WORKERS", 10),
			QueueSize: e.int("INDEXING_QUEUE_SIZE", 1000),
			BatchSize: e.int("INDEXING_BATCH_SIZE", 100),
		},
		Metrics: MetricsConfig{
			Enabled: e.bool("METRICS_ENABLED", true),
			Path:    getEnv("METRICS_PATH", "/metrics"),
			Addr:    getEnv("METRICS_ADDR", ":9091"),
		},
		Embedding: EmbeddingConfig{
			Provider:  strings.ToLower(getEnv("EMBEDDING_PROVIDER", "gemini")),
			APIKey:    strings.TrimSpace(os.Getenv("EMBEDDING_API_KEY")),
			Model:     getEnv("EMBEDDING_MODEL", "gemini-embedding-2"),
			Dimension: e.int("EMBEDDING_DIMENSION", 768),
			Timeout:   e.duration("EMBEDDING_TIMEOUT", 10*time.Second),
		},
		Search: SearchConfig{
			DefaultLimit:   e.int("SEARCH_DEFAULT_LIMIT", 10),
			MaxLimit:       e.int("SEARCH_MAX_LIMIT", 50),
			CandidateLimit: e.int("SEARCH_CANDIDATE_LIMIT", 50),
			RRFK:           e.int("RRF_K", 60),
			Timeout:        e.duration("SEARCH_TIMEOUT", 5*time.Second),
		},
	}
	cfg.Kafka.DLQTopic = getEnv("KAFKA_DLQ_TOPIC", cfg.Kafka.Topic+".dlq")
	cfg.Qdrant.VectorSize = e.int("QDRANT_VECTOR_SIZE", cfg.Embedding.Dimension)

	if err := e.err(); err != nil {
		return Config{}, err
	}

	host, port, useTLS, err := splitEndpoint(cfg.Qdrant.URL, 6334)
	if err != nil {
		return Config{}, fmt.Errorf("invalid QDRANT_URL: %w", err)
	}
	cfg.Qdrant.Host, cfg.Qdrant.Port, cfg.Qdrant.UseTLS = host, port, useTLS

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate checks value ranges that parsing alone cannot catch.
func (c Config) validate() error {
	var errs []error
	add := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if c.Server.Port < 1 || c.Server.Port > 65535 {
		add(fmt.Errorf("HTTP_PORT must be between 1 and 65535, got %d", c.Server.Port))
	}
	for _, d := range []struct {
		key   string
		value time.Duration
	}{
		{"APP_STARTUP_TIMEOUT", c.App.StartupTimeout},
		{"HTTP_READ_TIMEOUT", c.Server.ReadTimeout},
		{"HTTP_WRITE_TIMEOUT", c.Server.WriteTimeout},
		{"HTTP_IDLE_TIMEOUT", c.Server.IdleTimeout},
		{"HTTP_SHUTDOWN_TIMEOUT", c.Server.ShutdownTimeout},
		{"DATABASE_MAX_CONN_LIFETIME", c.PostgreSQL.MaxConnLifetime},
		{"DATABASE_MAX_CONN_IDLE_TIME", c.PostgreSQL.MaxConnIdleTime},
		{"DATABASE_HEALTH_CHECK_PERIOD", c.PostgreSQL.HealthCheckPeriod},
		{"DATABASE_CONNECT_TIMEOUT", c.PostgreSQL.ConnectTimeout},
		{"EMBEDDING_TIMEOUT", c.Embedding.Timeout},
		{"SEARCH_TIMEOUT", c.Search.Timeout},
		{"KAFKA_RETRY_INITIAL_BACKOFF", c.Kafka.Retry.InitialBackoff},
		{"KAFKA_RETRY_MAX_BACKOFF", c.Kafka.Retry.MaxBackoff},
	} {
		if d.value <= 0 {
			add(fmt.Errorf("%s must be positive, got %s", d.key, d.value))
		}
	}
	for _, n := range []struct {
		key   string
		value int
	}{
		{"DATABASE_MAX_CONNS", c.PostgreSQL.MaxConns},
		{"KAFKA_WORKERS", c.Kafka.Workers},
		{"INDEXING_WORKERS", c.Indexing.Workers},
		{"INDEXING_QUEUE_SIZE", c.Indexing.QueueSize},
		{"INDEXING_BATCH_SIZE", c.Indexing.BatchSize},
		{"KAFKA_RETRY_MAX_ATTEMPTS", c.Kafka.Retry.MaxAttempts},
		{"EMBEDDING_DIMENSION", c.Embedding.Dimension},
		{"QDRANT_VECTOR_SIZE", c.Qdrant.VectorSize},
		{"SEARCH_DEFAULT_LIMIT", c.Search.DefaultLimit},
		{"SEARCH_MAX_LIMIT", c.Search.MaxLimit},
		{"SEARCH_CANDIDATE_LIMIT", c.Search.CandidateLimit},
		{"RRF_K", c.Search.RRFK},
	} {
		if n.value <= 0 {
			add(fmt.Errorf("%s must be positive, got %d", n.key, n.value))
		}
	}
	if c.PostgreSQL.MinConns < 0 {
		add(fmt.Errorf("DATABASE_MIN_CONNS must not be negative, got %d", c.PostgreSQL.MinConns))
	}
	if c.PostgreSQL.MinConns > c.PostgreSQL.MaxConns {
		add(fmt.Errorf("DATABASE_MIN_CONNS (%d) must not exceed DATABASE_MAX_CONNS (%d)",
			c.PostgreSQL.MinConns, c.PostgreSQL.MaxConns))
	}
	// A cap below the first wait, or a multiplier that shrinks the wait, would
	// silently turn exponential backoff into something else, so both are
	// refused rather than corrected.
	if c.Kafka.Retry.MaxBackoff > 0 && c.Kafka.Retry.MaxBackoff < c.Kafka.Retry.InitialBackoff {
		add(fmt.Errorf("KAFKA_RETRY_MAX_BACKOFF (%s) must not be shorter than KAFKA_RETRY_INITIAL_BACKOFF (%s)",
			c.Kafka.Retry.MaxBackoff, c.Kafka.Retry.InitialBackoff))
	}
	if m := c.Kafka.Retry.Multiplier; m < 1 || math.IsInf(m, 0) || math.IsNaN(m) {
		add(fmt.Errorf("KAFKA_RETRY_MULTIPLIER must be a finite number of at least 1, got %v", m))
	}
	if c.Metrics.Enabled {
		if !strings.HasPrefix(c.Metrics.Path, "/") {
			add(fmt.Errorf("METRICS_PATH must start with /, got %q", c.Metrics.Path))
		}
		if c.Metrics.Addr == "" {
			add(errors.New("METRICS_ADDR must not be empty"))
		}
	}
	add(checkURL("OPENSEARCH_URL", c.OpenSearch.URL))
	if c.OpenSearch.Index == "" {
		add(errors.New("OPENSEARCH_INDEX must not be empty"))
	}
	if c.Qdrant.Collection == "" {
		add(errors.New("QDRANT_COLLECTION must not be empty"))
	}
	// Qdrant rejects every vector whose length differs from the collection's.
	if c.Qdrant.VectorSize != c.Embedding.Dimension {
		add(fmt.Errorf("QDRANT_VECTOR_SIZE (%d) must match EMBEDDING_DIMENSION (%d)",
			c.Qdrant.VectorSize, c.Embedding.Dimension))
	}
	if c.Embedding.Model == "" {
		add(errors.New("EMBEDDING_MODEL must not be empty"))
	}
	if c.Search.DefaultLimit > c.Search.MaxLimit {
		add(fmt.Errorf("SEARCH_DEFAULT_LIMIT (%d) must not exceed SEARCH_MAX_LIMIT (%d)",
			c.Search.DefaultLimit, c.Search.MaxLimit))
	}
	if c.Search.Timeout >= c.Server.WriteTimeout {
		add(fmt.Errorf("SEARCH_TIMEOUT (%s) must be shorter than HTTP_WRITE_TIMEOUT (%s)",
			c.Search.Timeout, c.Server.WriteTimeout))
	}
	// Each retriever must supply at least a full page, or the fused page could
	// come up short even when both retrievers have plenty of matches.
	if c.Search.MaxLimit > c.Search.CandidateLimit {
		add(fmt.Errorf("SEARCH_MAX_LIMIT (%d) must not exceed SEARCH_CANDIDATE_LIMIT (%d)",
			c.Search.MaxLimit, c.Search.CandidateLimit))
	}

	return errors.Join(errs...)
}

// env records the errors from a series of lookups so that Load can report
// every problem at once instead of only the first.
type env struct {
	errs []error
}

func (e *env) required(key string) string {
	v, err := getRequiredEnv(key)
	e.add(err)
	return v
}

func (e *env) requiredList(key string) []string {
	v, err := getRequiredList(key)
	e.add(err)
	return v
}

func (e *env) int(key string, fallback int) int {
	v, err := getEnvInt(key, fallback)
	e.add(err)
	return v
}

func (e *env) duration(key string, fallback time.Duration) time.Duration {
	v, err := getEnvDuration(key, fallback)
	e.add(err)
	return v
}

// bool parses a flag. Only the forms strconv.ParseBool accepts are allowed,
// so a misspelled "yes" is a startup error rather than a silent false.
func (e *env) bool(key string, fallback bool) bool {
	raw := getEnv(key, "")
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		e.add(fmt.Errorf("invalid %s: want true or false, got %q", key, raw))
		return fallback
	}
	return v
}

func (e *env) float(key string, fallback float64) float64 {
	v, err := getEnvFloat(key, fallback)
	e.add(err)
	return v
}

func (e *env) logLevel(key string, fallback slog.Level) slog.Level {
	raw := getEnv(key, "")
	if raw == "" {
		return fallback
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		e.add(fmt.Errorf("invalid %s: want debug, info, warn or error, got %q", key, raw))
		return fallback
	}
	return level
}

func (e *env) add(err error) {
	if err != nil {
		e.errs = append(e.errs, err)
	}
}

// err returns all collected errors joined together, or nil when there are none.
func (e *env) err() error {
	return errors.Join(e.errs...)
}

// getEnv returns the value of key, or fallback when it is unset or empty.
func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// getRequiredEnv returns the value of key, or an error when it is unset or empty.
func getRequiredEnv(key string) (string, error) {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("%s is required", key)
}

// getEnvInt parses key as an integer. Range checks belong in validate.
func getEnvInt(key string, fallback int) (int, error) {
	raw := getEnv(key, "")
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}

// getEnvFloat parses key as a decimal number. Range checks belong in validate.
func getEnvFloat(key string, fallback float64) (float64, error) {
	raw := getEnv(key, "")
	if raw == "" {
		return fallback, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return f, nil
}

// getEnvDuration parses key as a Go duration such as "10s" or "1m30s".
func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := getEnv(key, "")
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return d, nil
}

// getRequiredList parses key as a comma-separated list with at least one item.
func getRequiredList(key string) ([]string, error) {
	var items []string
	for _, item := range strings.Split(os.Getenv(key), ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("%s is required", key)
	}
	return items, nil
}

func checkURL(key, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	if u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("invalid %s: want http(s)://host[:port], got %q", key, raw)
	}
	return nil
}

// splitEndpoint breaks a URL into host, port and whether TLS is used, applying
// defaultPort when the URL leaves the port out.
func splitEndpoint(raw string, defaultPort int) (host string, port int, useTLS bool, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", 0, false, err
	}
	if u.Host == "" {
		return "", 0, false, fmt.Errorf("want scheme://host[:port], got %q", raw)
	}

	host = u.Hostname()
	port = defaultPort
	if p := u.Port(); p != "" {
		if port, err = strconv.Atoi(p); err != nil {
			return "", 0, false, fmt.Errorf("invalid port in %q: %w", raw, err)
		}
	}
	switch u.Scheme {
	case "https", "grpcs":
		useTLS = true
	case "http", "grpc":
		useTLS = false
	default:
		return "", 0, false, fmt.Errorf("unsupported scheme %q in %q", u.Scheme, raw)
	}
	return host, port, useTLS, nil
}
