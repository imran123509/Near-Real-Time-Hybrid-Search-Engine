// Package config loads application settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds the settings for the API and consumer services. Settings a
// service does not use may be left unset; each constructor validates the
// fields it needs.
type Config struct {
	HTTPAddr    string
	PostgresURL string
	OpenSearch  OpenSearchConfig
	Qdrant      QdrantConfig
	Kafka       KafkaConfig
	Gemini      GeminiConfig
}

// OpenSearchConfig holds OpenSearch connection settings.
type OpenSearchConfig struct {
	Addresses []string
	Username  string
	Password  string
	Index     string
}

// QdrantConfig holds Qdrant gRPC connection settings.
type QdrantConfig struct {
	Host       string
	Port       int
	APIKey     string
	UseTLS     bool
	Collection string
}

// KafkaConfig holds consumer settings.
type KafkaConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	DLQTopic string
	// Workers is the number of goroutines indexing messages concurrently.
	Workers int
	// QueueSize is the number of messages buffered per worker.
	QueueSize int
	// MaxAttempts is how many times a message is tried before it is
	// published to the dead-letter topic.
	MaxAttempts int
}

// GeminiConfig holds Gemini embedding API settings.
type GeminiConfig struct {
	APIKey     string
	Model      string
	Dimensions int
}

// Load reads configuration from the environment.
//
//	HTTP_ADDR                    default ":8080"
//	POSTGRES_URL                 required
//	OPENSEARCH_ADDRESSES         comma-separated, default "http://localhost:9200"
//	OPENSEARCH_USERNAME          optional
//	OPENSEARCH_PASSWORD          optional
//	OPENSEARCH_INDEX             default "documents"
//	QDRANT_HOST                  default "localhost"
//	QDRANT_PORT                  default 6334
//	QDRANT_API_KEY               optional
//	QDRANT_USE_TLS               default false
//	QDRANT_COLLECTION            default "documents"
//	KAFKA_BROKERS                comma-separated, required by the consumer
//	KAFKA_TOPIC                  default "document-events"
//	KAFKA_GROUP_ID               default "search-indexer"
//	KAFKA_DLQ_TOPIC              default KAFKA_TOPIC + ".dlq"
//	KAFKA_WORKERS                default 10
//	KAFKA_QUEUE_SIZE             default 8 (per worker)
//	KAFKA_MAX_ATTEMPTS           default 5
//	GEMINI_API_KEY               required by the consumer
//	GEMINI_EMBEDDING_MODEL       default "gemini-embedding-2"
//	GEMINI_EMBEDDING_DIMENSIONS  default 768
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		PostgresURL: os.Getenv("POSTGRES_URL"),
		OpenSearch: OpenSearchConfig{
			Addresses: splitList(getEnv("OPENSEARCH_ADDRESSES", "http://localhost:9200")),
			Username:  os.Getenv("OPENSEARCH_USERNAME"),
			Password:  os.Getenv("OPENSEARCH_PASSWORD"),
			Index:     getEnv("OPENSEARCH_INDEX", "documents"),
		},
		Qdrant: QdrantConfig{
			Host:       getEnv("QDRANT_HOST", "localhost"),
			APIKey:     os.Getenv("QDRANT_API_KEY"),
			Collection: getEnv("QDRANT_COLLECTION", "documents"),
		},
		Kafka: KafkaConfig{
			Brokers: splitList(os.Getenv("KAFKA_BROKERS")),
			Topic:   getEnv("KAFKA_TOPIC", "document-events"),
			GroupID: getEnv("KAFKA_GROUP_ID", "search-indexer"),
		},
		Gemini: GeminiConfig{
			APIKey: os.Getenv("GEMINI_API_KEY"),
			Model:  getEnv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2"),
		},
	}
	cfg.Kafka.DLQTopic = getEnv("KAFKA_DLQ_TOPIC", cfg.Kafka.Topic+".dlq")

	if cfg.PostgresURL == "" {
		return Config{}, errors.New("POSTGRES_URL is required")
	}

	var err error
	if cfg.Qdrant.Port, err = getEnvInt("QDRANT_PORT", 6334); err != nil {
		return Config{}, err
	}
	if cfg.Qdrant.UseTLS, err = strconv.ParseBool(getEnv("QDRANT_USE_TLS", "false")); err != nil {
		return Config{}, fmt.Errorf("parse QDRANT_USE_TLS: %w", err)
	}
	if cfg.Kafka.Workers, err = getEnvInt("KAFKA_WORKERS", 10); err != nil {
		return Config{}, err
	}
	if cfg.Kafka.QueueSize, err = getEnvInt("KAFKA_QUEUE_SIZE", 8); err != nil {
		return Config{}, err
	}
	if cfg.Kafka.MaxAttempts, err = getEnvInt("KAFKA_MAX_ATTEMPTS", 5); err != nil {
		return Config{}, err
	}
	if cfg.Gemini.Dimensions, err = getEnvInt("GEMINI_EMBEDDING_DIMENSIONS", 768); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// getEnvInt parses key as a positive integer.
func getEnvInt(key string, fallback int) (int, error) {
	v := getEnv(key, strconv.Itoa(fallback))
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", key, n)
	}
	return n, nil
}

func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
