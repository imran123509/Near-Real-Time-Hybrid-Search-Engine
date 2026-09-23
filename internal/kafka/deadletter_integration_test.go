//go:build integration

// These tests need a running Kafka broker and are excluded from
// `go test ./...` by the integration build tag. Each run uses its own
// throwaway dead-letter topic and deletes it afterwards. Run them with:
//
//	KAFKA_TEST_BROKERS=localhost:9092 go test -tags=integration ./internal/kafka/
//
// The broker must allow topic creation, which the Compose stack does not: it
// pre-creates its topics instead. Point KAFKA_TEST_BROKERS at a broker with
// auto-creation enabled, or create the topic listed in the skip message first.
package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/dlq"
)

// newIntegrationWriter returns a dead-letter writer for a topic of this test's
// own, created up front and removed afterwards.
func newIntegrationWriter(t *testing.T) (*DeadLetterWriter, config.KafkaConfig) {
	t.Helper()
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		t.Skip("set KAFKA_TEST_BROKERS to run integration tests")
	}

	cfg := config.KafkaConfig{
		Brokers:  strings.Split(brokers, ","),
		Topic:    "test-events",
		DLQTopic: fmt.Sprintf("test-events-dlq-%d", time.Now().UnixNano()),
	}
	createTopic(t, cfg)

	writer, err := NewDeadLetterWriter(cfg)
	if err != nil {
		t.Fatalf("NewDeadLetterWriter: %v", err)
	}
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Errorf("close writer: %v", err)
		}
	})
	return writer, cfg
}

// createTopic creates only the topic this test publishes to, and deletes only
// that one afterwards, so a shared broker keeps everything else.
func createTopic(t *testing.T, cfg config.KafkaConfig) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		t.Skipf("kafka is not reachable at %s: %v", cfg.Brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("find controller: %v", err)
	}
	admin, err := kafkago.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer admin.Close()

	err = admin.CreateTopics(kafkago.TopicConfig{Topic: cfg.DLQTopic, NumPartitions: 1, ReplicationFactor: 1})
	if err != nil {
		t.Skipf("cannot create topic %s, so this broker cannot run the test: %v", cfg.DLQTopic, err)
	}
	t.Cleanup(func() {
		if err := admin.DeleteTopics(cfg.DLQTopic); err != nil {
			t.Logf("delete topic %s: %v", cfg.DLQTopic, err)
		}
	})
}

// A real broker must give back exactly what was published: the envelope with
// its metadata, and the original payload byte for byte, or a replay would
// index something other than what failed.
func TestDeadLetterWriterRoundTripsAMessage(t *testing.T) {
	writer, cfg := newIntegrationWriter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A payload that is neither valid JSON nor valid UTF-8, which is the kind
	// of message most likely to be mangled on the way through.
	payload := append([]byte(`{"op":"c","after":{"title":"Go & concurrency`), 0x00, 0xff, 0xfe)
	want := dlq.New(dlq.Source{
		Topic:     cfg.Topic,
		Partition: 0,
		Offset:    4711,
		Key:       []byte(`{"id":"7f1c9b2e-4a3d-4e8b-9c1a-2b3c4d5e6f70"}`),
		Payload:   payload,
	}, errors.New("opensearch: status 400: mapping conflict"), "non_retryable", 3)

	if err := writer.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   cfg.Brokers,
		Topic:     cfg.DLQTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	record, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read from %s: %v", cfg.DLQTopic, err)
	}

	var got dlq.Message
	if err := json.Unmarshal(record.Value, &got); err != nil {
		t.Fatalf("decode dead-letter message: %v", err)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Errorf("payload = %q, want the original %q", got.Payload, payload)
	}
	if !bytes.Equal(record.Key, []byte(want.EventKey)) {
		t.Errorf("record key = %q, want %q: failures for one document must stay in one partition",
			record.Key, want.EventKey)
	}
	if got.OriginalTopic != want.OriginalTopic || got.OriginalPartition != want.OriginalPartition ||
		got.OriginalOffset != want.OriginalOffset {
		t.Errorf("origin = %s/%d/%d, want %s/%d/%d",
			got.OriginalTopic, got.OriginalPartition, got.OriginalOffset,
			want.OriginalTopic, want.OriginalPartition, want.OriginalOffset)
	}
	if got.Error != want.Error || got.ErrorType != want.ErrorType || got.Attempts != want.Attempts {
		t.Errorf("failure description = %+v, want %+v", got, want)
	}
	if !got.FailedAt.Equal(want.FailedAt) {
		t.Errorf("failed_at = %s, want %s", got.FailedAt, want.FailedAt)
	}
}

// Publishing to a broker that cannot be reached has to fail rather than block
// forever, because the caller is holding an uncommitted message while it
// waits.
func TestDeadLetterWriterFailsWhenTheBrokerIsUnreachable(t *testing.T) {
	if os.Getenv("KAFKA_TEST_BROKERS") == "" {
		t.Skip("set KAFKA_TEST_BROKERS to run integration tests")
	}
	writer, err := NewDeadLetterWriter(config.KafkaConfig{
		Brokers:  []string{"127.0.0.1:1"}, // nothing listens here
		Topic:    "test-events",
		DLQTopic: "test-events-dlq",
	})
	if err != nil {
		t.Fatalf("NewDeadLetterWriter: %v", err)
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := dlq.New(dlq.Source{Topic: "test-events", Payload: []byte("{}")}, errors.New("boom"), "retryable", 3)
	if err := writer.Publish(ctx, msg); err == nil {
		t.Fatal("Publish reported success against an unreachable broker, so the message would be committed and lost")
	}
}
