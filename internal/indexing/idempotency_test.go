package indexing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/kafka"
)

// These tests are the idempotency matrix: every way the same change can reach
// the pipeline more than once, and every way one store can fail while the
// other succeeds. They drive the whole change-data-capture path -- parse,
// retry, dead-letter, write -- against the in-memory stores from
// cdc_pipeline_test.go, so nothing needs Kafka, OpenSearch or Qdrant running.
//
//	scenario              first attempt   retry   final state
//	--------------------------------------------------------------------
//	CREATE                success         no      1 document + 1 point
//	CREATE again          success         no      still 1 and 1
//	UPDATE                success         no      latest version
//	UPDATE again          success         no      latest version
//	DELETE                success         no      removed
//	DELETE again          success         no      still removed
//	OpenSearch down       failure         yes     1 and 1
//	Qdrant down           partial         yes     1 and 1
//	embedder down         partial         yes     1 and 1, or dead-lettered
//	consumer crash        partial         yes     1 and 1
//	stale replay          skipped         no      newer data kept
//
// "Consistent" here means both indexes end up holding the intended state. It
// does not mean the two writes are atomic: they are separate systems, and
// nothing in this package pretends otherwise.

// state is what both indexes hold for one document.
type state struct {
	documents, points int
	title             string
	version           int64
	pointVersion      int64
}

func stateOf(t *testing.T, st *stores, id string) state {
	t.Helper()
	documents, points := st.counts()
	s := state{documents: documents, points: points}

	doc := st.document(id)
	s.title, s.version = doc.Title, doc.Version
	if point, ok := st.point(id); ok {
		s.pointVersion, _ = point.Payload["version"].(int64)
	}
	return s
}

// deliver processes msg and fails the test if its offset could not be
// committed, which is what the worker pool requires before acknowledging it.
func deliver(t *testing.T, p *Pipeline[cdc.ChangeEvent], msg kafka.Message) {
	t.Helper()
	if err := p.Process(context.Background(), msg); err != nil {
		t.Fatalf("Process offset %d: %v", msg.Offset, err)
	}
}

// Kafka may deliver the same message again after a crash, a rebalance or a
// redelivery. Three identical creates must leave what one create left.
func TestDuplicateCreate(t *testing.T) {
	st := newStores()
	deadLetters := &fakeDeadLetters{}
	pipeline := newCDCTestPipeline(t, st, deadLetters, attemptsPolicy(3))

	// The same record three times: same key, same payload, same offset.
	msg := changeMessage("c", cdcTestID, 1, 0)
	for range 3 {
		deliver(t, pipeline, msg)
	}

	got := stateOf(t, st, cdcTestID)
	want := state{documents: 1, points: 1, title: "Title " + cdcTestID, version: 1, pointVersion: 1}
	if got != want {
		t.Errorf("after three identical creates: %+v, want %+v", got, want)
	}
	if deadLetters.count() != 0 {
		t.Errorf("a redelivery was dead-lettered: %v", deadLetters.all())
	}
}

// Replaying an update must leave the latest row indexed once, in both stores.
func TestDuplicateUpdate(t *testing.T) {
	st := newStores()
	pipeline := newCDCTestPipeline(t, st, &fakeDeadLetters{}, attemptsPolicy(3))

	deliver(t, pipeline, changeMessage("c", cdcTestID, 1, 0))
	update := changeMessage("u", cdcTestID, 2, 1)
	for range 3 {
		deliver(t, pipeline, update)
	}

	got := stateOf(t, st, cdcTestID)
	want := state{documents: 1, points: 1, title: "Title " + cdcTestID, version: 2, pointVersion: 2}
	if got != want {
		t.Errorf("after three identical updates: %+v, want %+v", got, want)
	}
}

// A delete that arrives again must not fail, or the message would be retried
// until it was dead-lettered, for a document that is already gone.
func TestDuplicateDelete(t *testing.T) {
	st := newStores()
	deadLetters := &fakeDeadLetters{}
	pipeline := newCDCTestPipeline(t, st, deadLetters, attemptsPolicy(3))

	deliver(t, pipeline, changeMessage("c", cdcTestID, 1, 0))
	del := changeMessage("d", cdcTestID, 1, 1)
	for range 3 {
		deliver(t, pipeline, del)
	}
	// The tombstone Debezium writes after a delete, also redelivered.
	for range 2 {
		deliver(t, pipeline, tombstoneMessage(cdcTestID, 2))
	}

	if got := stateOf(t, st, cdcTestID); got.documents != 0 || got.points != 0 {
		t.Errorf("after three deletes: %+v, want both indexes empty", got)
	}
	if deadLetters.count() != 0 {
		t.Errorf("a repeated delete was dead-lettered: %v", deadLetters.all())
	}
}

// OpenSearch is down when the event first arrives. Nothing is written until it
// comes back, and then exactly one document and one point exist.
func TestOpenSearchFailureRecovery(t *testing.T) {
	st := newStores()
	deadLetters := &fakeDeadLetters{}
	pipeline := newCDCTestPipeline(t, st, deadLetters, attemptsPolicy(5))

	// Down for two attempts, then back.
	st.failIndexes(2, errors.New("opensearch unavailable"))
	deliver(t, pipeline, changeMessage("c", cdcTestID, 1, 0))

	got := stateOf(t, st, cdcTestID)
	want := state{documents: 1, points: 1, title: "Title " + cdcTestID, version: 1, pointVersion: 1}
	if got != want {
		t.Errorf("after the keyword index recovered: %+v, want %+v", got, want)
	}
	if deadLetters.count() != 0 {
		t.Errorf("a recoverable failure was dead-lettered: %v", deadLetters.all())
	}
}

// Qdrant is down after OpenSearch has already taken the write. This is the
// partial state the pipeline is built to survive: the retry writes the
// keyword document again, which changes nothing, and finally stores the point.
func TestQdrantFailureRecovery(t *testing.T) {
	st := newStores()
	deadLetters := &fakeDeadLetters{}
	pipeline := newCDCTestPipeline(t, st, deadLetters, attemptsPolicy(5))

	st.failUpserts(2, errors.New("qdrant unavailable"))
	deliver(t, pipeline, changeMessage("c", cdcTestID, 1, 0))

	got := stateOf(t, st, cdcTestID)
	want := state{documents: 1, points: 1, title: "Title " + cdcTestID, version: 1, pointVersion: 1}
	if got != want {
		t.Errorf("after the vector index recovered: %+v, want %+v", got, want)
	}
	if deadLetters.count() != 0 {
		t.Errorf("a recoverable failure was dead-lettered: %v", deadLetters.all())
	}
}

// The whole reason idempotency is required: a retry repeats writes that
// already succeeded. Whichever store failed first, the indexes converge and
// the message may be committed.
func TestRetryIdempotency(t *testing.T) {
	unavailable := errors.New("store unavailable")

	tests := []struct {
		name string
		fail func(st *stores)
	}{
		{"the keyword write fails", func(st *stores) { st.failIndexes(1, unavailable) }},
		{"the embedding fails", func(st *stores) { st.failEmbeddings(1, unavailable) }},
		{"the vector write fails", func(st *stores) { st.failUpserts(1, unavailable) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newStores()
			deadLetters := &fakeDeadLetters{}
			pipeline := newCDCTestPipeline(t, st, deadLetters, attemptsPolicy(3))

			var reports []Report
			pipeline.Observe(func(r Report) { reports = append(reports, r) })

			tt.fail(st)
			deliver(t, pipeline, changeMessage("c", cdcTestID, 1, 0))

			got := stateOf(t, st, cdcTestID)
			want := state{documents: 1, points: 1, title: "Title " + cdcTestID, version: 1, pointVersion: 1}
			if got != want {
				t.Errorf("after the retry: %+v, want %+v", got, want)
			}
			if deadLetters.count() != 0 {
				t.Errorf("dead-lettered a recoverable failure: %v", deadLetters.all())
			}
			// One message, one outcome, and it took more than one attempt.
			if len(reports) != 1 {
				t.Fatalf("%d outcomes reported, want 1", len(reports))
			}
			if reports[0].Outcome != OutcomeIndexed || reports[0].Attempts != 2 {
				t.Errorf("report = %+v, want indexed after 2 attempts", reports[0])
			}
			// A later change still applies on top of the recovered state.
			deliver(t, pipeline, changeMessage("u", cdcTestID, 2, 1))
			if got := stateOf(t, st, cdcTestID); got.version != 2 || got.pointVersion != 2 || got.documents != 1 || got.points != 1 {
				t.Errorf("after the next change: %+v, want one document and one point at version 2", got)
			}
		})
	}
}

// An embedding failure must never be reported as success: the vector index
// would silently never hold the document. It is retried, and if the provider
// stays down the message is dead-lettered rather than quietly dropped.
func TestEmbeddingFailureIsNeverSilentlySuccessful(t *testing.T) {
	st := newStores()
	deadLetters := &fakeDeadLetters{}
	pipeline := newCDCTestPipeline(t, st, deadLetters, attemptsPolicy(3))

	st.failEmbeddings(-1, errors.New("embedding provider unavailable"))
	deliver(t, pipeline, changeMessage("c", cdcTestID, 1, 0))

	// The keyword index took the write; the vector index could not.
	got := stateOf(t, st, cdcTestID)
	if got.documents != 1 || got.points != 0 {
		t.Errorf("state = %+v, want the keyword write kept and no point", got)
	}
	letter := deadLetters.only(t)
	if letter.Attempts != 3 {
		t.Errorf("dead-lettered after %d attempts, want the configured 3", letter.Attempts)
	}

	// Once the provider recovers, replaying the message converges both stores
	// rather than adding a second document.
	st.failEmbeddings(0, nil)
	deliver(t, pipeline, changeMessage("c", cdcTestID, 1, 0))
	if got := stateOf(t, st, cdcTestID); got.documents != 1 || got.points != 1 || got.pointVersion != 1 {
		t.Errorf("after the replay: %+v, want one document and one point", got)
	}
}

// A consumer that crashes between writing and committing gets the message
// again after it restarts. The restart is modelled by building a second
// pipeline over the same stores, as a new process would.
func TestConsumerRestartRecovery(t *testing.T) {
	tests := []struct {
		name string
		op   string
		// version is the row version the event carries.
		version int
		// wantDocuments and wantPoints are the state after the restart.
		wantDocuments, wantPoints int
	}{
		{name: "create", op: "c", version: 1, wantDocuments: 1, wantPoints: 1},
		{name: "update", op: "u", version: 2, wantDocuments: 1, wantPoints: 1},
		{name: "delete", op: "d", version: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newStores()
			msg := changeMessage(tt.op, cdcTestID, tt.version, 7)

			// Before the crash: for an update or a delete the document is
			// already indexed, as it would be in a running system.
			if tt.op != "c" {
				deliver(t, newCDCTestPipeline(t, st, &fakeDeadLetters{}, attemptsPolicy(3)),
					changeMessage("c", cdcTestID, 1, 6))
			}

			// The crash: processing is cancelled part way through, so the
			// offset is not committed.
			crashing := newCDCTestPipeline(t, st, &fakeDeadLetters{}, attemptsPolicy(3))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := crashing.Process(ctx, msg); err == nil {
				t.Fatal("Process returned nil for a cancelled message, so its offset would be committed")
			}

			// The restart: a new pipeline over the same indexes is given the
			// same message again, because Kafka still has the old offset.
			restarted := newCDCTestPipeline(t, st, &fakeDeadLetters{}, attemptsPolicy(3))
			deliver(t, restarted, msg)

			got := stateOf(t, st, cdcTestID)
			if got.documents != tt.wantDocuments || got.points != tt.wantPoints {
				t.Errorf("after the restart: %d documents and %d points, want %d and %d",
					got.documents, got.points, tt.wantDocuments, tt.wantPoints)
			}
			if tt.wantDocuments > 0 && got.version != int64(tt.version) {
				t.Errorf("indexed version = %d, want %d", got.version, tt.version)
			}
		})
	}
}

// A replay of an old event must not put old data back. The keyword index
// refuses it, and the pipeline then leaves the vector index alone too, because
// nothing there would refuse it.
func TestStaleReplayDoesNotOverwriteNewerData(t *testing.T) {
	st := newStores()
	pipeline := newCDCTestPipeline(t, st, &fakeDeadLetters{}, attemptsPolicy(3))

	var reports []Report
	pipeline.Observe(func(r Report) { reports = append(reports, r) })

	deliver(t, pipeline, changeMessage("u", cdcTestID, 9, 0))
	embedsBefore := len(st.embedTexts())

	// The same document as it was five versions ago.
	deliver(t, pipeline, changeMessage("u", cdcTestID, 4, 1))

	got := stateOf(t, st, cdcTestID)
	if got.version != 9 || got.pointVersion != 9 {
		t.Errorf("state = %+v, want version 9 in both indexes", got)
	}
	if got.documents != 1 || got.points != 1 {
		t.Errorf("state = %+v, want one document and one point", got)
	}
	if embeds := len(st.embedTexts()); embeds != embedsBefore {
		t.Errorf("%d embed calls, want %d: a superseded row must not be embedded", embeds, embedsBefore)
	}
	if len(reports) != 2 || reports[1].Effect != cdc.EffectStale {
		t.Errorf("reports = %+v, want the second one to record a stale event", reports)
	}
}

// Every way one message can end has to be visible to a metrics exporter, and
// a replay has to be distinguishable from a first delivery.
func TestReportsDistinguishReplaysFromFirstDeliveries(t *testing.T) {
	st := newStores()
	pipeline := newCDCTestPipeline(t, st, &fakeDeadLetters{}, attemptsPolicy(3))

	var effects []cdc.Effect
	pipeline.Observe(func(r Report) { effects = append(effects, r.Effect) })

	create := changeMessage("c", cdcTestID, 1, 0)
	deliver(t, pipeline, create)
	deliver(t, pipeline, create)
	deliver(t, pipeline, changeMessage("u", cdcTestID, 2, 1))
	deliver(t, pipeline, changeMessage("u", cdcTestID, 1, 2))
	deliver(t, pipeline, changeMessage("d", cdcTestID, 2, 3))

	want := []cdc.Effect{cdc.EffectIndexed, cdc.EffectReapplied, cdc.EffectIndexed, cdc.EffectStale, cdc.EffectDeleted}
	if len(effects) != len(want) {
		t.Fatalf("%d outcomes reported, want %d", len(effects), len(want))
	}
	for i, effect := range effects {
		if effect != want[i] {
			t.Errorf("message %d reported %q, want %q", i, effect, want[i])
		}
	}
}

// Different documents are processed by different workers at the same time.
// Nothing about one document's writes may depend on, or disturb, another's.
func TestConcurrentDocumentsAreIndexedIndependently(t *testing.T) {
	const documents, deliveries = 8, 4
	st := newStores()
	pipeline := newCDCTestPipeline(t, st, &fakeDeadLetters{}, attemptsPolicy(3))

	var done sync.WaitGroup
	done.Add(documents * deliveries)
	handler := func(ctx context.Context, msg kafka.Message) error {
		defer done.Done()
		return pipeline.Process(ctx, msg)
	}

	pool, err := NewWorkerPool(documents/2, 16, handler,
		func(err error) { t.Errorf("unexpected fatal error: %v", err) }, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewWorkerPool: %v", err)
	}
	pool.Start(context.Background())

	offset := int64(0)
	submit := func(msg kafka.Message) {
		if err := pool.Submit(context.Background(), msg); err != nil {
			t.Errorf("Submit: %v", err)
		}
	}
	for d := range documents {
		id := fmt.Sprintf("doc-%d", d)
		// The same event delivered several times, as a redelivery would, plus
		// one later version. Events for one document share a key, so they all
		// go to one worker and stay in order.
		for range deliveries - 1 {
			submit(changeMessage("c", id, 1, offset))
			offset++
		}
		submit(changeMessage("u", id, 2, offset))
		offset++
	}
	done.Wait()
	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	gotDocuments, gotPoints := st.counts()
	if gotDocuments != documents || gotPoints != documents {
		t.Fatalf("%d documents and %d points, want %d of each: duplicates or lost writes",
			gotDocuments, gotPoints, documents)
	}
	for d := range documents {
		id := fmt.Sprintf("doc-%d", d)
		if got := st.document(id).Version; got != 2 {
			t.Errorf("document %s is at version %d, want the latest, 2", id, got)
		}
	}
}
