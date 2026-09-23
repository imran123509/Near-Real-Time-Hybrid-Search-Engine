package e2e

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/dlq"
)

// These tests cover what happens when the same change reaches the consumer
// more than once, which Kafka's at-least-once delivery makes normal rather
// than exceptional: a redelivery after a crash, a rebalance or a replay. They
// run against the same stack as the rest of the suite.
//
// What they check is always the same three things: one logical document is one
// OpenSearch document and one Qdrant point, the newest row wins, and the
// consumer carries on afterwards.

// replay publishes the change event for a row as Debezium would, n times over.
// The key is the document ID, so every copy lands on the partition the
// original came from and is processed in order with it.
func replay(t *testing.T, ctx context.Context, row Row, op string, n int) {
	t.Helper()
	key, value, err := ChangeEvent(row, op)
	if err != nil {
		t.Fatal(err)
	}
	for range n {
		if err := Publish(ctx, env, key, value); err != nil {
			t.Fatal(err)
		}
	}
}

// waitForReplayToBeHandled publishes a message that cannot be processed and
// waits for it to reach the dead-letter topic.
//
// It is a barrier, not a test of the dead-letter topic: the message carries
// the document's key, so it is processed after everything published for that
// document before it. When it turns up in the dead-letter topic, the replays
// in front of it are finished, and the indexes can be checked without waiting
// for a change that may never come.
func waitForReplayToBeHandled(t *testing.T, ctx context.Context, id string) {
	t.Helper()
	key, value, err := MalformedEvent(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := Publish(ctx, env, key, value); err != nil {
		t.Fatal(err)
	}

	readCtx, cancel := context.WithTimeout(ctx, env.Timeout)
	defer cancel()
	if _, err := ReadDeadLetters(readCtx, env, func(m dlq.Message) bool { return bytes.Equal(m.Payload, value) }); err != nil {
		t.Fatalf("the barrier message never reached %s, so the replays may still be in flight: %v", env.DLQTopic, err)
	}
}

// assertIndexedOnce checks the two indexes and the public API agree that there
// is exactly one of this document.
func assertIndexedOnce(t *testing.T, ctx context.Context, doc Document) {
	t.Helper()
	if count, err := CountOpenSearchDocs(ctx, env, doc.ID); err != nil || count != 1 {
		t.Errorf("OpenSearch holds %d documents for %s (err %v), want exactly 1", count, doc.ID, err)
	}
	if count, err := CountQdrantPoints(ctx, env, doc.ID); err != nil || count != 1 {
		t.Errorf("Qdrant holds %d points for %s (err %v), want exactly 1", count, doc.ID, err)
	}
	// From outside, a duplicate would show up as the same document twice in
	// one response, whatever its position.
	resp, status, err := Search(ctx, env, doc.Token, 10)
	if err != nil || status != 200 {
		t.Fatalf("search: status %d, err %v", status, err)
	}
	if got := resp.Count(doc.ID); got != 1 {
		t.Errorf("the search API returned document %s %d times, want once: %+v", doc.ID, got, resp.Results)
	}
}

// TestDuplicateCreate replays a CREATE event the consumer has already applied.
func TestDuplicateCreate(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "duplicate-create")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)

	row, err := ReadRow(ctx, pool, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	replay(t, ctx, row, "c", 3)
	waitForReplayToBeHandled(t, ctx, doc.ID)

	assertIndexedOnce(t, ctx, doc)
	indexed := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	if indexed.Version != row.Version {
		t.Errorf("indexed version = %d, want the row version %d", indexed.Version, row.Version)
	}
}

// TestDuplicateUpdate replays an UPDATE, which must leave the latest row
// indexed exactly once rather than adding a second copy of the document.
func TestDuplicateUpdate(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "duplicate-update")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })

	updated := NewDocument("duplicate-update-revised")
	if err := Update(ctx, pool, doc.ID, updated.Title, updated.Body); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error {
		if d.Source.Title != updated.Title {
			return fmt.Errorf("waiting for the update, have %q", d.Source.Title)
		}
		return nil
	})

	row, err := ReadRow(ctx, pool, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	replay(t, ctx, row, "u", 3)
	waitForReplayToBeHandled(t, ctx, doc.ID)

	assertIndexedOnce(t, ctx, doc)
	final, err := GetOpenSearchDoc(ctx, env, doc.ID)
	if err != nil || !final.Found {
		t.Fatalf("GetOpenSearchDoc: %+v, %v", final, err)
	}
	if final.Source.Title != updated.Title || final.Source.Content != updated.Body {
		t.Errorf("indexed document = %+v, want the updated row", final.Source)
	}
	point, found, err := GetQdrantPoint(ctx, env, doc.ID)
	if err != nil || !found {
		t.Fatalf("GetQdrantPoint: %v, found %v", err, found)
	}
	if title, _ := point.Payload["title"].(string); title != updated.Title {
		t.Errorf("point payload title = %q, want the updated title", title)
	}
}

// TestDuplicateDelete replays a DELETE for a document that is already gone.
// The repeats must be accepted, not retried until they are dead-lettered.
func TestDuplicateDelete(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "duplicate-delete")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)

	row, err := ReadRow(ctx, pool, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := Delete(ctx, pool, doc.ID); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearchGone(t, ctx, doc.ID)
	waitForQdrantGone(t, ctx, doc.ID)

	// The same delete two more times, as a redelivery would bring it.
	replay(t, ctx, row, "d", 2)
	waitForReplayToBeHandled(t, ctx, doc.ID)

	if count, err := CountOpenSearchDocs(ctx, env, doc.ID); err != nil || count != 0 {
		t.Errorf("OpenSearch holds %d documents for %s (err %v), want none", count, doc.ID, err)
	}
	if count, err := CountQdrantPoints(ctx, env, doc.ID); err != nil || count != 0 {
		t.Errorf("Qdrant holds %d points for %s (err %v), want none", count, doc.ID, err)
	}
	waitForSearchMiss(t, ctx, doc.Token, doc.ID)
}

// TestStaleEventDoesNotOverwriteNewerData replays an event from before the
// latest update. Both indexes must keep the newer row: the keyword index
// refuses the old version, and the vector index, which has no version check of
// its own, is never asked to store it.
func TestStaleEventDoesNotOverwriteNewerData(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "stale-replay")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	// The row as it was before the update: this is what gets replayed.
	old, err := ReadRow(ctx, pool, doc.ID)
	if err != nil {
		t.Fatal(err)
	}

	updated := NewDocument("stale-replay-current")
	if err := Update(ctx, pool, doc.ID, updated.Title, updated.Body); err != nil {
		t.Fatal(err)
	}
	current := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error {
		if d.Source.Title != updated.Title {
			return fmt.Errorf("waiting for the update, have %q", d.Source.Title)
		}
		return nil
	})

	replay(t, ctx, old, "u", 2)
	waitForReplayToBeHandled(t, ctx, doc.ID)

	final, err := GetOpenSearchDoc(ctx, env, doc.ID)
	if err != nil || !final.Found {
		t.Fatalf("GetOpenSearchDoc: %+v, %v", final, err)
	}
	if final.Source.Title != updated.Title || final.Version != current.Version {
		t.Errorf("indexed document = %+v at version %d, want the version %d row: a stale event overwrote newer data",
			final.Source, final.Version, current.Version)
	}
	point, found, err := GetQdrantPoint(ctx, env, doc.ID)
	if err != nil || !found {
		t.Fatalf("GetQdrantPoint: %v, found %v", err, found)
	}
	// The payload carries the row version the vector was built from, so an
	// older vector in the collection shows up here.
	if version := payloadVersion(point); version != current.Version {
		t.Errorf("point payload version = %d, want %d: the vector index holds the older row",
			version, current.Version)
	}
	if title, _ := point.Payload["title"].(string); title != updated.Title {
		t.Errorf("point payload title = %q, want %q", title, updated.Title)
	}
	assertIndexedOnce(t, ctx, doc)
}

// TestConsumerRestartRecovery stops the consumer mid-stream and starts it
// again. Whatever it had not committed is delivered again, and the changes it
// missed while it was down are still waiting on the topic.
func TestConsumerRestartRecovery(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "restart-recovery")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)

	row, err := ReadRow(ctx, pool, doc.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Restart the consumer. Only that one service is touched.
	restartCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := RestartService(restartCtx, "consumer"); err != nil {
		t.Skipf("cannot restart the consumer with docker compose: %v", err)
	}

	// A change made while it was restarting, which it has to pick up when it
	// comes back, and a replay of the old event, which it must not let undo
	// that change.
	updated := NewDocument("restart-recovery-after")
	if err := Update(ctx, pool, doc.ID, updated.Title, updated.Body); err != nil {
		t.Fatal(err)
	}
	replay(t, ctx, row, "u", 1)

	indexed := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error {
		if d.Source.Title != updated.Title {
			return fmt.Errorf("waiting for the restarted consumer to catch up, have %q", d.Source.Title)
		}
		return nil
	})
	if indexed.Version <= row.Version {
		t.Errorf("indexed version = %d, want it past the pre-restart version %d", indexed.Version, row.Version)
	}
	waitForQdrantPoint(t, ctx, doc.ID)
	assertIndexedOnce(t, ctx, doc)
}

// payloadVersion reads the row version from a point's payload. Qdrant returns
// JSON numbers, which decode as float64.
func payloadVersion(point QdrantPoint) int64 {
	switch v := point.Payload["version"].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}
