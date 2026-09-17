package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"near-real-time-hybrid-search-engine/internal/config"
)

// DeadLetterWriter publishes messages that could not be indexed to the
// dead-letter topic, keeping the original key and value so they can be
// replayed. It is safe for concurrent use.
type DeadLetterWriter struct {
	writer *kafkago.Writer
}

// NewDeadLetterWriter returns a writer for cfg.DLQTopic. The caller owns the
// writer and must call Close.
func NewDeadLetterWriter(cfg config.KafkaConfig) (*DeadLetterWriter, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS is required")
	}
	if cfg.DLQTopic == "" {
		return nil, errors.New("kafka dead-letter topic is required")
	}
	return &DeadLetterWriter{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(cfg.Brokers...),
			Topic:        cfg.DLQTopic,
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireAll,
			// Publish each message immediately instead of waiting for a batch.
			BatchSize: 1,
		},
	}, nil
}

// Publish writes msg and the reason it failed to the dead-letter topic.
func (w *DeadLetterWriter) Publish(ctx context.Context, msg Message, cause error) error {
	err := w.writer.WriteMessages(ctx, kafkago.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafkago.Header{
			{Key: "dlq.error", Value: []byte(cause.Error())},
			{Key: "dlq.original_topic", Value: []byte(msg.Topic)},
			{Key: "dlq.original_partition", Value: []byte(strconv.Itoa(msg.Partition))},
			{Key: "dlq.original_offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
			{Key: "dlq.failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	})
	if err != nil {
		return fmt.Errorf("publish to dead-letter topic: %w", err)
	}
	return nil
}

// Close flushes and closes the writer.
func (w *DeadLetterWriter) Close() error {
	return w.writer.Close()
}
