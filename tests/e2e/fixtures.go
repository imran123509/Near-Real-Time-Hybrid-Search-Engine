package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"near-real-time-hybrid-search-engine/internal/dlq"
)

// Document is a test document. Every run builds new ones, so tests never
// collide with each other or with what an earlier run left behind.
//
// Token is a word that appears in the title and the body and nowhere else in
// the corpus, so a keyword search for it matches this document alone. The
// table's column is "body"; the search index calls the same field "content".
type Document struct {
	ID    string
	Title string
	Body  string
	URL   string
	Token string
}

// NewDocument builds a document with a unique ID and token.
func NewDocument(subject string) Document {
	id := uuid.NewString()
	token := "e2e" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	return Document{
		ID:    id,
		Title: fmt.Sprintf("E2E %s %s", subject, token),
		Body: fmt.Sprintf(
			"This document validates the change data capture pipeline end to end. "+
				"Marker %s identifies it. Subject: %s.", token, subject),
		URL:   "https://example.com/e2e/" + token,
		Token: token,
	}
}

// Row is a documents row as the database holds it.
type Row struct {
	ID        string
	Title     string
	Body      string
	URL       *string
	Version   int64
	UpdatedAt time.Time
}

// Insert writes the document. version and updated_at are set by the trigger
// from migrations/0002, which is what keeps versions increasing.
func Insert(ctx context.Context, pool *pgxpool.Pool, doc Document) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO documents (id, title, body, url) VALUES ($1, $2, $3, $4)`,
		doc.ID, doc.Title, doc.Body, doc.URL)
	if err != nil {
		return fmt.Errorf("insert document %s: %w", doc.ID, err)
	}
	return nil
}

// Update replaces the title and body, which bumps version and updated_at.
func Update(ctx context.Context, pool *pgxpool.Pool, id, title, body string) error {
	tag, err := pool.Exec(ctx, `UPDATE documents SET title = $2, body = $3 WHERE id = $1`, id, title, body)
	if err != nil {
		return fmt.Errorf("update document %s: %w", id, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("update document %s: %d rows affected", id, tag.RowsAffected())
	}
	return nil
}

// Delete removes the document. A missing row is not an error, so cleanup can
// run whatever the test did.
func Delete(ctx context.Context, pool *pgxpool.Pool, id string) error {
	if _, err := pool.Exec(ctx, `DELETE FROM documents WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete document %s: %w", id, err)
	}
	return nil
}

// ReadRow returns the row as it is now.
func ReadRow(ctx context.Context, pool *pgxpool.Pool, id string) (Row, error) {
	var row Row
	err := pool.QueryRow(ctx,
		`SELECT id::text, title, body, url, version, updated_at FROM documents WHERE id = $1`, id).
		Scan(&row.ID, &row.Title, &row.Body, &row.URL, &row.Version, &row.UpdatedAt)
	if err != nil {
		return Row{}, fmt.Errorf("read document %s: %w", id, err)
	}
	return row, nil
}

// ChangeEvent builds the Debezium message for a row, in the shape the
// connector produces with JsonConverter and schemas disabled: the same
// envelope the consumer parses in production.
//
// Republishing one is how the idempotency test replays a change that the
// consumer has already applied.
func ChangeEvent(row Row, op string) (key, value []byte, err error) {
	after := map[string]any{
		"id":         row.ID,
		"title":      row.Title,
		"body":       row.Body,
		"version":    row.Version,
		"updated_at": row.UpdatedAt.UTC().Format(time.RFC3339Nano),
		"url":        row.URL,
	}
	now := time.Now().UnixMilli()
	envelope := map[string]any{
		"before": nil,
		"after":  after,
		"source": map[string]any{
			"version":   "3.6.3.Final",
			"connector": "postgresql",
			"name":      "search",
			"ts_ms":     row.UpdatedAt.UnixMilli(),
			"snapshot":  "false",
			"db":        "searchdb",
			"schema":    "public",
			"table":     "documents",
		},
		"op":    op,
		"ts_ms": now,
	}

	if key, err = json.Marshal(map[string]any{"id": row.ID}); err != nil {
		return nil, nil, fmt.Errorf("encode key: %w", err)
	}
	if value, err = json.Marshal(envelope); err != nil {
		return nil, nil, fmt.Errorf("encode value: %w", err)
	}
	return key, value, nil
}

// Publish writes one message to the change topic, as Debezium would. The key
// decides the partition, so replayed events stay in order with the real ones
// for that document.
func Publish(ctx context.Context, env Environment, key, value []byte) error {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(env.KafkaBrokers...),
		Topic:        env.Topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		BatchSize:    1,
	}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, kafkago.Message{Key: key, Value: value}); err != nil {
		return fmt.Errorf("publish to %s: %w", env.Topic, err)
	}
	return nil
}

// MalformedEvent builds a message the consumer cannot possibly index: the
// value is not a Debezium envelope at all. The key is a real document ID, so
// the message is routed and ordered like any other event for that document.
func MalformedEvent(id string) (key, value []byte, err error) {
	if key, err = json.Marshal(map[string]any{"id": id}); err != nil {
		return nil, nil, fmt.Errorf("encode key: %w", err)
	}
	return key, []byte(`{"op": "c", "after": {"id": ` + id + `, "title": `), nil
}

// ReadDeadLetters reads the dead-letter topic from the beginning and returns
// the first message that match accepts, or ctx's error if none arrives in
// time. The reader is not part of a consumer group, so it leaves no committed
// offsets behind and repeated runs each see the whole topic.
func ReadDeadLetters(ctx context.Context, env Environment, match func(dlq.Message) bool) (dlq.Message, error) {
	var found dlq.Message
	err := readDeadLetters(ctx, env, func(msg dlq.Message) bool {
		if !match(msg) {
			return true // keep reading
		}
		found = msg
		return false
	})
	if err != nil {
		return dlq.Message{}, err
	}
	return found, nil
}

// ReadAllDeadLetters reads the dead-letter topic until ctx ends and returns
// every message match accepts, oldest first. Running out of time is how it
// finishes, not a failure: the topic never ends.
func ReadAllDeadLetters(ctx context.Context, env Environment, match func(dlq.Message) bool) ([]dlq.Message, error) {
	var found []dlq.Message
	err := readDeadLetters(ctx, env, func(msg dlq.Message) bool {
		if match(msg) {
			found = append(found, msg)
		}
		return true
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return nil, err
	}
	return found, nil
}

// readDeadLetters calls visit for every message on the dead-letter topic until
// visit returns false or ctx ends.
func readDeadLetters(ctx context.Context, env Environment, visit func(dlq.Message) bool) error {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   env.KafkaBrokers,
		Topic:     env.DLQTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	for {
		record, err := reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("read from %s: %w", env.DLQTopic, err)
		}
		var msg dlq.Message
		if err := json.Unmarshal(record.Value, &msg); err != nil {
			return fmt.Errorf("decode dead-letter message at offset %d: %w", record.Offset, err)
		}
		if !visit(msg) {
			return nil
		}
	}
}
