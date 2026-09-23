package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/dlq"
)

// These tests take one search index away from the running consumer and check
// that the pipeline converges once it comes back. They are opt-in: stopping
// and starting a container takes far longer than the consumer's retry window,
// so a run takes minutes rather than seconds, and while a store is down the
// rest of the stack is degraded.
//
//	$env:E2E = "1"; $env:E2E_FAILURE_INJECTION = "1"
//	go test ./tests/e2e/ -run FailureRecovery -v
//
// Failure injection is done by stopping the real service, never by building
// failure into the application: nothing in cmd/ or internal/ knows these tests
// exist.

// failureInjectionEnabled reports whether the tests that stop services may
// run. They are separate from the rest of the suite because they are slow and
// disruptive, not because they are optional in principle.
func failureInjectionEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_FAILURE_INJECTION") == "" {
		t.Skip("set E2E_FAILURE_INJECTION to run the tests that stop services")
	}
}

// takeDown stops a Compose service for the duration of the test and brings it
// back afterwards, waiting until it serves again so the tests that follow find
// the stack as they expect. readyURL is what proves it is serving.
func takeDown(t *testing.T, service, readyURL string) {
	t.Helper()
	failureInjectionEnabled(t)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 3*time.Minute)
	defer cancel()
	if err := StopService(ctx, service); err != nil {
		t.Skipf("cannot stop %s with docker compose: %v", service, err)
	}
	t.Logf("%s stopped", service)

	t.Cleanup(func() {
		// Runs whatever the test did, so a failure cannot leave the stack
		// with one service missing.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Minute)
		defer cancel()
		if err := StartService(ctx, service); err != nil {
			t.Errorf("could not start %s again: %v", service, err)
			return
		}
		if err := waitForHTTP(ctx, readyURL); err != nil {
			t.Errorf("%s did not serve again within %s: %v", service, env.Timeout, err)
		}
		t.Logf("%s is serving again", service)
	})
}

// bringBack starts the service again mid-test and waits until it serves, so
// the assertions after it are about a working stack. The cleanup registered by
// takeDown still runs and does nothing the second time.
func bringBack(t *testing.T, service, readyURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	if err := StartService(ctx, service); err != nil {
		t.Fatalf("start %s: %v", service, err)
	}
	if err := waitForHTTP(ctx, readyURL); err != nil {
		t.Fatalf("%s did not serve again: %v", service, err)
	}
	t.Logf("%s is serving again", service)
}

// recoverFromDeadLetter republishes every event for this document that the
// consumer gave up on while the store was down, oldest first, and reports how
// many there were.
//
// Whether there are any depends on timing: the retry window is a few seconds
// and starting a container takes longer, so the events usually reach the
// dead-letter topic first. Replaying them in order is what a reprocessor would
// do, and each payload is the original event, byte for byte. Replay order does
// not actually matter here -- an older event is refused once a newer one is
// indexed -- but it is the order a reprocessor would use.
func recoverFromDeadLetter(t *testing.T, ctx context.Context, id string) int {
	t.Helper()

	// Bounded: no dead letter for this document is the other valid outcome,
	// not a failure. The read ends when the window closes.
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	letters, err := ReadAllDeadLetters(readCtx, env, func(m dlq.Message) bool {
		// The key is Debezium's primary-key JSON; matching the payload too
		// keeps this from depending on how that JSON is spaced.
		return strings.Contains(m.EventKey, id) || bytes.Contains(m.Payload, []byte(id))
	})
	if err != nil {
		t.Fatalf("read the dead-letter topic: %v", err)
	}
	if len(letters) == 0 {
		t.Log("the events survived their retries; nothing to replay")
		return 0
	}

	for _, letter := range letters {
		t.Logf("replaying an event dead-lettered after %d attempts (%s)", letter.Attempts, letter.ErrorType)
		if err := Publish(ctx, env, []byte(letter.EventKey), letter.Payload); err != nil {
			t.Fatalf("replay the dead-lettered event: %v", err)
		}
	}
	return len(letters)
}

// TestOpenSearchFailureRecovery stops the keyword index while a document is
// being indexed. Nothing is written until it comes back, and then the document
// exists exactly once in both stores.
func TestOpenSearchFailureRecovery(t *testing.T) {
	ctx := t.Context()
	failureInjectionEnabled(t)
	doc := newTestDocument(t, "opensearch-failure")

	takeDown(t, "opensearch", env.OpenSearchURL+"/_cluster/health?wait_for_status=yellow&timeout=5s")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	bringBack(t, "opensearch", env.OpenSearchURL+"/_cluster/health?wait_for_status=yellow&timeout=5s")
	recoverFromDeadLetter(t, ctx, doc.ID)

	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)
	assertIndexedOnce(t, ctx, doc)
}

// TestQdrantFailureRecovery stops the vector index, which is the one partial
// state an upsert can reach: the keyword index takes the write and the vector
// index does not. The event is never reported as done, and once Qdrant is back
// the two converge.
func TestQdrantFailureRecovery(t *testing.T) {
	ctx := t.Context()
	failureInjectionEnabled(t)
	doc := newTestDocument(t, "qdrant-failure")

	takeDown(t, "qdrant", env.QdrantURL+"/readyz")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	// The documented partial state: searchable by keyword, with no vector.
	indexed := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	t.Logf("the keyword index holds version %d while the vector index is down", indexed.Version)

	bringBack(t, "qdrant", env.QdrantURL+"/readyz")
	recoverFromDeadLetter(t, ctx, doc.ID)

	point := waitForQdrantPoint(t, ctx, doc.ID)
	if len(point.Vector) != env.VectorSize {
		t.Errorf("vector has %d dimensions, want %d", len(point.Vector), env.VectorSize)
	}
	if title, _ := point.Payload["title"].(string); title != doc.Title {
		t.Errorf("point payload title = %q, want %q", title, doc.Title)
	}
	assertIndexedOnce(t, ctx, doc)
}

// TestRetryIdempotency is the reason idempotency is required: writes that
// already succeeded are made again when an event is retried or replayed. Two
// changes are made while the vector index is down, so the keyword index is
// several versions ahead of it, and both must end on the newest row without
// duplicating anything.
func TestRetryIdempotency(t *testing.T) {
	ctx := t.Context()
	failureInjectionEnabled(t)
	doc := newTestDocument(t, "retry-idempotency")

	if err := Insert(ctx, pool, doc); err != nil {
		t.Fatal(err)
	}
	waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error { return matchesDocument(d, doc) })
	waitForQdrantPoint(t, ctx, doc.ID)

	takeDown(t, "qdrant", env.QdrantURL+"/readyz")

	first := NewDocument("retry-idempotency-first")
	if err := Update(ctx, pool, doc.ID, first.Title, first.Body); err != nil {
		t.Fatal(err)
	}
	latest := NewDocument("retry-idempotency-latest")
	if err := Update(ctx, pool, doc.ID, latest.Title, latest.Body); err != nil {
		t.Fatal(err)
	}
	// The keyword index runs ahead while the vector index is unreachable.
	ahead := waitForOpenSearch(t, ctx, doc.ID, func(d OpenSearchDoc) error {
		if d.Source.Title != latest.Title {
			return fmt.Errorf("waiting for the latest update, have %q", d.Source.Title)
		}
		return nil
	})

	bringBack(t, "qdrant", env.QdrantURL+"/readyz")
	// Both changes may have been dead-lettered; replaying either is safe,
	// because an older one is refused and the newer one wins.
	recoverFromDeadLetter(t, ctx, doc.ID)

	point := waitForQdrantPoint(t, ctx, doc.ID)
	if title, _ := point.Payload["title"].(string); title != latest.Title {
		t.Errorf("point payload title = %q, want the latest %q", title, latest.Title)
	}
	if version := payloadVersion(point); version > ahead.Version {
		t.Errorf("point payload version = %d, which is past the row version %d", version, ahead.Version)
	}
	final, err := GetOpenSearchDoc(ctx, env, doc.ID)
	if err != nil || !final.Found {
		t.Fatalf("GetOpenSearchDoc: %+v, %v", final, err)
	}
	if final.Source.Title != latest.Title || final.Version != ahead.Version {
		t.Errorf("indexed document = %+v at version %d, want the latest row at %d",
			final.Source, final.Version, ahead.Version)
	}
	assertIndexedOnce(t, ctx, doc)
}
