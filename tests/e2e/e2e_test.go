package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"near-real-time-hybrid-search-engine/internal/dlq"
)

// seededDocumentID is one of the rows docker/postgres/initdb/30-seed.sql
// inserts before the connector starts, so it reaches the indexes through
// Debezium's initial snapshot.
const seededDocumentID = "0b7d4f6e-3c1a-4e2b-9f5d-1a2b3c4d5e03"

var (
	env  Environment
	pool *pgxpool.Pool
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	if !Enabled() {
		fmt.Println("E2E is not set: skipping the end-to-end suite. See tests/e2e/README.md.")
		return 0
	}

	var err error
	if env, err = LoadEnvironment(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// Generous: OpenSearch and Kafka Connect can take a while to come up, and
	// the connector snapshots the table before it streams.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := waitForStack(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "the stack is not ready: %v\n\nStart it with:\n"+
			"  $env:EMBEDDING_PROVIDER=\"fake\"; docker compose up -d --build\n", err)
		return 1
	}
	defer pool.Close()

	code := m.Run()
	if code != 0 && getEnv("E2E_DUMP_LOGS", "1") != "0" {
		DumpComposeLogs(os.Stderr, 200, "consumer", "api", "debezium")
	}
	return code
}

// waitForStack blocks until every service the tests need is actually serving,
// rather than merely started, and the Debezium connector is running.
func waitForStack(ctx context.Context) error {
	var err error
	if err = eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		pool, err = Connect(ctx, env)
		return err == nil, err
	}); err != nil {
		return fmt.Errorf("postgres at %s: %w", redactURL(env.DatabaseURL), err)
	}

	checks := []struct {
		name string
		url  string
	}{
		{"opensearch", env.OpenSearchURL + "/_cluster/health?wait_for_status=yellow&timeout=5s"},
		{"qdrant", env.QdrantURL + "/readyz"},
		{"kafka connect", env.ConnectURL + "/connectors"},
		{"search api", env.APIURL + "/ready"},
	}
	for _, check := range checks {
		if err := waitForHTTP(ctx, check.url); err != nil {
			return fmt.Errorf("%s at %s: %w", check.name, check.url, err)
		}
	}

	if err := waitForKafkaTopic(ctx); err != nil {
		return fmt.Errorf("kafka topic %s: %w", env.Topic, err)
	}
	// The tests write to PostgreSQL, so the connector must be streaming first.
	return EnsureConnector(ctx, env)
}

func waitForHTTP(ctx context.Context, url string) error {
	return eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		status, err := getJSON(ctx, url, nil)
		if err != nil {
			return false, err
		}
		if status != 200 {
			return false, fmt.Errorf("status %d", status)
		}
		return true, nil
	})
}

func waitForKafkaTopic(ctx context.Context) error {
	return eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		conn, err := kafkago.DialContext(ctx, "tcp", env.KafkaBrokers[0])
		if err != nil {
			return false, err
		}
		defer conn.Close()
		partitions, err := conn.ReadPartitions(env.Topic)
		if err != nil {
			return false, err
		}
		return len(partitions) > 0, nil
	})
}

// redactURL hides the password in a connection string before it reaches the
// test output.
func redactURL(raw string) string {
	at := strings.LastIndex(raw, "@")
	scheme := strings.Index(raw, "://")
	if at < 0 || scheme < 0 {
		return raw
	}
	return raw[:scheme+3] + "***@" + raw[at+1:]
}

// ------------------------------------------------------------------ the tests

// TestDocumentCreateEndToEnd is the whole pipeline: a row written to
// PostgreSQL turns up in both indexes and is returned by the public API.
func TestDocumentCreateEndToEnd(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "create")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}

	indexed := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error {
		return matchesDocument(d, doc)
	})
	if indexed.Version <= 0 {
		t.Errorf("OpenSearch version = %d, want the row version", indexed.Version)
	}

	point := waitForQdrantPoint(t, ctx, doc.ID)
	if len(point.Vector) != env.VectorSize {
		t.Errorf("vector has %d dimensions, want %d", len(point.Vector), env.VectorSize)
	}
	if got, _ := point.Payload["document_id"].(string); got != doc.ID {
		t.Errorf("payload document_id = %q, want %q", got, doc.ID)
	}
	if got, _ := point.Payload["title"].(string); got != doc.Title {
		t.Errorf("payload title = %q, want %q", got, doc.Title)
	}

	// The public contract: the document is findable through the HTTP API.
	resp := waitForSearchHit(t, ctx, doc.Token, doc.ID)
	i, _ := resp.Find(doc.ID)
	result := resp.Results[i]
	if result.Title != doc.Title || result.Content != doc.Body || result.URL != doc.URL {
		t.Errorf("search result = %+v, want the inserted document", result)
	}
	if result.Score <= 0 {
		t.Errorf("score = %v, want a positive fused score", result.Score)
	}
}

// TestDocumentUpdateEndToEnd checks that an UPDATE replaces what is indexed,
// in both stores and in the API's answers.
func TestDocumentUpdateEndToEnd(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "update")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	before := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	beforePoint := waitForQdrantPoint(t, ctx, doc.ID)

	updated := NewDocument("update-revised")
	updated.ID, updated.URL = doc.ID, doc.URL
	if err := Update(ctx, pool, doc.ID, updated.Title, updated.Body); err != nil {
		t.Fatal(err)
	}

	after := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error {
		if d.Source.Title != updated.Title || d.Source.Content != updated.Body {
			return fmt.Errorf("still the old title/content: %q", d.Source.Title)
		}
		return nil
	})
	if after.Version <= before.Version {
		t.Errorf("version %d did not increase from %d", after.Version, before.Version)
	}
	if !after.Source.UpdatedAt.After(before.Source.UpdatedAt) {
		t.Errorf("updated_at %s is not after %s", after.Source.UpdatedAt, before.Source.UpdatedAt)
	}

	// The point is still there and now holds the vector of the new text.
	afterPoint := waitForQdrantPoint(t, ctx, doc.ID)
	if sameVector(beforePoint.Vector, afterPoint.Vector) {
		t.Error("the Qdrant vector did not change with the content")
	}

	waitForSearchHit(t, ctx, updated.Token, doc.ID)
	// The replaced content is no longer what the index holds.
	waitForSearchMiss(t, ctx, doc.Token, doc.ID)
}

// TestDocumentDeleteEndToEnd checks that a DELETE removes the document from
// both indexes and from search results.
func TestDocumentDeleteEndToEnd(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "delete")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)
	waitForSearchHit(t, ctx, doc.Token, doc.ID)

	if err := Delete(ctx, pool, doc.ID); err != nil {
		t.Fatal(err)
	}

	waitForOpenSearchGone(t, ctx, doc.ID)
	waitForQdrantGone(t, ctx, doc.ID)
	waitForSearchMiss(t, ctx, doc.Token, doc.ID)
}

// TestInitialSnapshot checks the rows that existed before the connector
// started, which Debezium reports as "r" events, reached the indexes too.
func TestInitialSnapshot(t *testing.T) {
	ctx := t.Context()

	row, err := ReadRow(ctx, pool, seededDocumentID)
	if err != nil {
		t.Skipf("seed document %s is not in the database, so there is no snapshot to check: %v", seededDocumentID, err)
	}

	waitForOpenSearch(t, ctx, row.ID, func(d OpenSearchDoc) error {
		if d.Source.Title != row.Title {
			return fmt.Errorf("title %q, want %q", d.Source.Title, row.Title)
		}
		return nil
	})
	waitForQdrantPoint(t, ctx, row.ID)
	waitForSearchHit(t, ctx, "goroutines channels", row.ID)
}

// TestDuplicateEventIsIdempotent replays a change the consumer has already
// applied, straight onto the topic, and checks that the document is still
// indexed exactly once and still holds the newest content.
func TestDuplicateEventIsIdempotent(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "idempotency")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	indexed := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)

	row, err := ReadRow(ctx, pool, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	key, value, err := ChangeEvent(row, "u")
	if err != nil {
		t.Fatal(err)
	}
	// The same logical change, delivered twice, as a redelivery would.
	for range 2 {
		if err := Publish(ctx, env, key, value); err != nil {
			t.Fatal(err)
		}
	}

	// A later change to the same document is the barrier: events for one key
	// share a partition and are processed in order, so once this one is
	// visible the replayed events have been handled.
	sentinel := NewDocument("idempotency-sentinel")
	if err := Update(ctx, pool, doc.ID, sentinel.Title, sentinel.Body); err != nil {
		t.Fatal(err)
	}
	final := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error {
		if d.Source.Title != sentinel.Title {
			return fmt.Errorf("waiting for the sentinel update, have %q", d.Source.Title)
		}
		return nil
	})

	// One row in, one document and one point out.
	if count, err := CountOpenSearchDocs(ctx, env, doc.ID); err != nil || count != 1 {
		t.Errorf("OpenSearch holds %d documents for %s (err %v), want exactly 1", count, doc.ID, err)
	}
	if count, err := CountQdrantPoints(ctx, env, doc.ID); err != nil || count != 1 {
		t.Errorf("Qdrant holds %d points for %s (err %v), want exactly 1", count, doc.ID, err)
	}
	// The replay must not have put the older content back.
	if final.Source.Content != sentinel.Body {
		t.Errorf("content = %q, want the sentinel body: a replayed event overwrote newer data", final.Source.Content)
	}
	if final.Version <= indexed.Version {
		t.Errorf("version %d did not advance past %d", final.Version, indexed.Version)
	}
}

// TestUnprocessableEventGoesToTheDeadLetterQueue publishes an event the
// consumer can never index and checks that it is stored in the dead-letter
// topic, with the original payload and enough context to replay it, and that
// consumption carries on afterwards.
func TestUnprocessableEventGoesToTheDeadLetterQueue(t *testing.T) {
	ctx := t.Context()
	doc := newTestDocument(t, "dead-letter")

	key, value, err := MalformedEvent(doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := Publish(ctx, env, key, value); err != nil {
		t.Fatal(err)
	}

	readCtx, cancel := context.WithTimeout(ctx, env.Timeout)
	defer cancel()
	letter, err := ReadDeadLetters(readCtx, env, func(m dlq.Message) bool {
		return bytes.Equal(m.Payload, value)
	})
	if err != nil {
		t.Fatalf("waiting for the message in %s: %v", env.DLQTopic, err)
	}

	// Everything needed to find the original message and replay it.
	if letter.OriginalTopic != env.Topic {
		t.Errorf("original_topic = %q, want %q", letter.OriginalTopic, env.Topic)
	}
	if letter.OriginalOffset < 0 {
		t.Errorf("original_offset = %d, want the offset it came from", letter.OriginalOffset)
	}
	if letter.EventKey != string(key) {
		t.Errorf("event_key = %q, want %q", letter.EventKey, key)
	}
	// A message that cannot be parsed is not worth retrying.
	if letter.ErrorType != "non_retryable" {
		t.Errorf("error_type = %q, want non_retryable", letter.ErrorType)
	}
	if letter.Error == "" {
		t.Error("the dead-letter message does not say why it failed")
	}
	if letter.FailedAt.IsZero() {
		t.Error("the dead-letter message does not say when it failed")
	}

	// One poisoned message must not stop the pipeline: a real row inserted
	// afterwards still reaches both indexes.
	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)
}

// TestRealEmbeddingProvider is a smoke test for the real provider: it finds a
// document by meaning, using words the document does not contain, which only
// works with real embeddings. It needs the stack running with
// EMBEDDING_PROVIDER=gemini and a key, so it is opt-in.
func TestRealEmbeddingProvider(t *testing.T) {
	if getEnv("E2E_REAL_EMBEDDING", "") != "true" {
		t.Skip("set E2E_REAL_EMBEDDING=true, and run the stack with a real embedding provider, to run this")
	}
	ctx := t.Context()

	doc := NewDocument("semantic")
	doc.Title = "Kayaking the upper river"
	doc.Body = "We paddled through the rapids at dawn, the canoe bouncing over white water between the canyon walls."
	t.Cleanup(func() { cleanupDocument(t, doc.ID) })
	t.Cleanup(func() { logStateOnFailure(t, doc.ID) })

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(OpenSearchDoc) error { return nil })
	waitForQdrantPoint(t, ctx, doc.ID)

	// No word here appears in the document, so only vector search can find it.
	waitForSearchHit(t, ctx, "boating adventure on a mountain stream", doc.ID)
}

// --------------------------------------------------------- test-side helpers

// newTestDocument builds a document and registers cleanup and diagnostics.
func newTestDocument(t *testing.T, subject string) Document {
	t.Helper()
	doc := NewDocument(subject)
	t.Logf("document %s, marker %s", doc.ID, doc.Token)
	// Runs after the diagnostics below, since cleanups run in reverse order.
	t.Cleanup(func() { cleanupDocument(t, doc.ID) })
	t.Cleanup(func() { logStateOnFailure(t, doc.ID) })
	return doc
}

// cleanupDocument removes the row, which also removes it from both indexes.
// It is best effort and never fails the test, so a cleanup problem cannot
// hide what the test actually found.
func cleanupDocument(t *testing.T, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 15*time.Second)
	defer cancel()
	if err := Delete(ctx, pool, id); err != nil {
		t.Logf("cleanup: %v", err)
	}
}

func logStateOnFailure(t *testing.T, id string) {
	if !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
	defer cancel()
	t.Log(PipelineState(ctx, env, pool, id))
}

// matchesDocument reports whether the indexed document is the one inserted.
func matchesDocument(d OpenSearchDoc, doc Document) error {
	switch {
	case d.Source.ID != doc.ID:
		return fmt.Errorf("id %q, want %q", d.Source.ID, doc.ID)
	case d.Source.Title != doc.Title:
		return fmt.Errorf("title %q, want %q", d.Source.Title, doc.Title)
	case d.Source.Content != doc.Body:
		return fmt.Errorf("content %q, want %q", d.Source.Content, doc.Body)
	case d.Source.URL != doc.URL:
		return fmt.Errorf("url %q, want %q", d.Source.URL, doc.URL)
	}
	return nil
}

func waitForOpenSearch(t *testing.T, ctx context.Context, id string, check func(OpenSearchDoc) error) OpenSearchDoc {
	t.Helper()
	var doc OpenSearchDoc
	err := eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		got, err := GetOpenSearchDoc(ctx, env, id)
		if err != nil {
			return false, err
		}
		if !got.Found {
			return false, fmt.Errorf("document is not in the index yet")
		}
		if err := check(got); err != nil {
			return false, err
		}
		doc = got
		return true, nil
	})
	if err != nil {
		t.Fatalf("opensearch: document %s did not reach index %q as expected within %s: %v",
			id, env.Index, env.Timeout, err)
	}
	return doc
}

func waitForOpenSearchGone(t *testing.T, ctx context.Context, id string) {
	t.Helper()
	err := eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		got, err := GetOpenSearchDoc(ctx, env, id)
		if err != nil {
			return false, err
		}
		if got.Found {
			return false, fmt.Errorf("document is still indexed")
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("opensearch: document %s was still in index %q after %s: %v", id, env.Index, env.Timeout, err)
	}
}

func waitForQdrantPoint(t *testing.T, ctx context.Context, id string) QdrantPoint {
	t.Helper()
	var point QdrantPoint
	err := eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		got, found, err := GetQdrantPoint(ctx, env, id)
		if err != nil || !found {
			return false, err
		}
		point = got
		return true, nil
	})
	if err != nil {
		t.Fatalf("qdrant: point %s did not appear in collection %q within %s: %v",
			id, env.Collection, env.Timeout, err)
	}
	return point
}

func waitForQdrantGone(t *testing.T, ctx context.Context, id string) {
	t.Helper()
	err := eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		_, found, err := GetQdrantPoint(ctx, env, id)
		if err != nil {
			return false, err
		}
		if found {
			return false, fmt.Errorf("point still exists")
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("qdrant: point %s was still in collection %q after %s: %v", id, env.Collection, env.Timeout, err)
	}
}

// waitForSearchHit waits until the API returns the document for the query. It
// asserts the document is present, not where it ranks: RRF ordering depends on
// the rest of the corpus.
func waitForSearchHit(t *testing.T, ctx context.Context, query, id string) SearchResponse {
	t.Helper()
	var resp SearchResponse
	err := eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		got, status, err := Search(ctx, env, query, 10)
		if err != nil {
			return false, err
		}
		if status != 200 {
			return false, fmt.Errorf("GET /api/v1/search returned %d", status)
		}
		if got.Total != len(got.Results) {
			return false, fmt.Errorf("total %d does not match %d results", got.Total, len(got.Results))
		}
		if _, ok := got.Find(id); !ok {
			return false, fmt.Errorf("%d results, none of them %s", len(got.Results), id)
		}
		resp = got
		return true, nil
	})
	if err != nil {
		t.Fatalf("search api: %q did not return document %s within %s: %v", query, id, env.Timeout, err)
	}
	return resp
}

func waitForSearchMiss(t *testing.T, ctx context.Context, query, id string) {
	t.Helper()
	err := eventually(ctx, env.Timeout, env.PollInterval, func(ctx context.Context) (bool, error) {
		got, status, err := Search(ctx, env, query, 10)
		if err != nil {
			return false, err
		}
		if status != 200 {
			return false, fmt.Errorf("GET /api/v1/search returned %d", status)
		}
		if _, ok := got.Find(id); ok {
			return false, fmt.Errorf("document is still returned")
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("search api: %q still returned document %s after %s: %v", query, id, env.Timeout, err)
	}
}

func sameVector(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
