package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/dlq"
)

// DeadLetterWriter publishes messages that could not be indexed to the
// dead-letter topic. It is safe for concurrent use.
//
// The record's value is the dlq.Message envelope as JSON: the original
// payload, kept byte for byte, plus where it came from and why it failed. Its
// key is the original message key, so every failure for one document lands in
// the same partition and keeps its order, and a future replay can read them
// back in the order they failed.
//
// Writes wait for all in-sync replicas to acknowledge them. Reporting a
// message as dead-lettered when it was not would lose it, since the original
// offset is committed straight afterwards.
type DeadLetterWriter struct {
	writer *kafkago.Writer
}

var _ dlq.Publisher = (*DeadLetterWriter)(nil)

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

// Topic returns the dead-letter topic this writer publishes to.
func (w *DeadLetterWriter) Topic() string { return w.writer.Topic }

// Publish writes one dead-letter message. It returns an error when the
// message may not have been stored, and the caller must then leave the
// original message uncommitted so Kafka delivers it again.
func (w *DeadLetterWriter) Publish(ctx context.Context, msg dlq.Message) error {
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode dead-letter message: %w", err)
	}
	record := kafkago.Message{Key: []byte(msg.EventKey), Value: value}

	if err := w.writer.WriteMessages(ctx, record); err != nil {
		return fmt.Errorf("publish to dead-letter topic %s: %w", w.writer.Topic, err)
	}
	return nil
}

// Close flushes and closes the writer.
func (w *DeadLetterWriter) Close() error {
	return w.writer.Close()
}
