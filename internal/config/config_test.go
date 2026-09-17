package config

import (
	"slices"
	"testing"
)

func TestLoadKafkaSettings(t *testing.T) {
	t.Setenv("POSTGRES_URL", "postgres://example")
	t.Setenv("KAFKA_BROKERS", " broker-1:9092, ,broker-2:9092 ")
	t.Setenv("KAFKA_TOPIC", "events")
	t.Setenv("KAFKA_DLQ_TOPIC", "")
	t.Setenv("KAFKA_WORKERS", "4")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"broker-1:9092", "broker-2:9092"}; !slices.Equal(cfg.Kafka.Brokers, want) {
		t.Errorf("Brokers = %q, want %q", cfg.Kafka.Brokers, want)
	}
	if cfg.Kafka.DLQTopic != "events.dlq" {
		t.Errorf("DLQTopic = %q, want %q", cfg.Kafka.DLQTopic, "events.dlq")
	}
	if cfg.Kafka.Workers != 4 {
		t.Errorf("Workers = %d, want 4", cfg.Kafka.Workers)
	}
}

func TestLoadRejectsInvalidNumbers(t *testing.T) {
	for _, value := range []string{"0", "-1", "ten"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("POSTGRES_URL", "postgres://example")
			t.Setenv("KAFKA_WORKERS", value)
			if _, err := Load(); err == nil {
				t.Fatalf("KAFKA_WORKERS=%q: expected an error", value)
			}
		})
	}
}
