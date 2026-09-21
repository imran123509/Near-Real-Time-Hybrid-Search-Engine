package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
)

// The fakes below stand in for OpenSearch, Qdrant and the embedding provider,
// so the whole package can be tested with `go test` alone. They are small on
// purpose: each one records what it was asked to do and can be told to fail.
// All three are safe for concurrent use, because the service is.

type fakeKeyword struct {
	mu          sync.Mutex
	docs        map[string]opensearch.Document
	indexCalls  int
	deleteCalls int
	indexErr    error
	deleteErr   error
}

func newFakeKeyword() *fakeKeyword {
	return &fakeKeyword{docs: map[string]opensearch.Document{}}
}

func (f *fakeKeyword) IndexDocument(_ context.Context, doc opensearch.Document) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.indexCalls++
	if f.indexErr != nil {
		return f.indexErr
	}
	// Mirrors the real client, where OpenSearch enforces external versioning
	// server-side: a write with an older or equal version is ignored.
	if existing, ok := f.docs[doc.ID]; ok && existing.Version > 0 && doc.Version > 0 && doc.Version <= existing.Version {
		return nil
	}
	f.docs[doc.ID] = doc
	return nil
}

func (f *fakeKeyword) DeleteDocument(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.docs, id) // deleting what is not there succeeds, as in the real client
	return nil
}

func (f *fakeKeyword) document(id string) (opensearch.Document, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	doc, ok := f.docs[id]
	return doc, ok
}

func (f *fakeKeyword) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.docs)
}

type fakeVectors struct {
	mu          sync.Mutex
	points      map[string]qdrant.Point
	upsertCalls int
	deleteCalls int
	upsertErr   error
	deleteErr   error
}

func newFakeVectors() *fakeVectors {
	return &fakeVectors{points: map[string]qdrant.Point{}}
}

func (f *fakeVectors) Upsert(_ context.Context, p qdrant.Point) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upsertCalls++
	if f.upsertErr != nil {
		return f.upsertErr
	}
	// The real client derives the point ID from p.ID, so the same document
	// always replaces its own point rather than adding one.
	f.points[p.ID] = p
	return nil
}

func (f *fakeVectors) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.points, id)
	return nil
}

func (f *fakeVectors) point(id string) (qdrant.Point, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.points[id]
	return p, ok
}

func (f *fakeVectors) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.points)
}

// fakeEmbedder returns a fixed vector and records the text it was given, so a
// test can check what BuildEmbeddingText produced without calling a provider.
type fakeEmbedder struct {
	mu    sync.Mutex
	texts []string
	err   error
}

func (f *fakeEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.texts = append(f.texts, text)
	if f.err != nil {
		return nil, f.err
	}
	return []float32{0.1, 0.2, 0.3}, nil
}

func (f *fakeEmbedder) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.texts...)
}

type testStores struct {
	keyword  *fakeKeyword
	vectors  *fakeVectors
	embedder *fakeEmbedder
	service  *Service
}

func newTestService(t *testing.T) testStores {
	t.Helper()
	s := testStores{keyword: newFakeKeyword(), vectors: newFakeVectors(), embedder: &fakeEmbedder{}}

	service, err := NewService(s.keyword, s.vectors, s.embedder, DefaultMapping())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	s.service = service
	return s
}

func TestServiceProcessCreate(t *testing.T) {
	s := newTestService(t)
	ev := parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "Indexing", "Change events keep indexes current.", 1)))

	if err := s.service.Process(context.Background(), ev); err != nil {
		t.Fatalf("Process: %v", err)
	}

	doc, ok := s.keyword.document(testDocumentID)
	if !ok {
		t.Fatalf("no document was indexed; keyword index holds %d documents", s.keyword.count())
	}
	if doc.Title != "Indexing" || doc.Content != "Change events keep indexes current." || doc.Version != 1 {
		t.Errorf("indexed document = %+v, want the row values", doc)
	}

	point, ok := s.vectors.point(testDocumentID)
	if !ok {
		t.Fatal("no point was upserted")
	}
	// The same ID reaches both stores, which is what makes a result from one
	// joinable with a result from the other.
	if point.ID != doc.ID {
		t.Errorf("point ID = %q, keyword ID = %q; they must match", point.ID, doc.ID)
	}
	if len(point.Vector) == 0 {
		t.Error("point has no vector")
	}
	if got := point.Payload["title"]; got != "Indexing" {
		t.Errorf("point payload title = %v, want the document title", got)
	}

	// The embedder saw exactly what BuildEmbeddingText produced.
	want := []string{"title: Indexing | text: Change events keep indexes current."}
	if got := s.embedder.calls(); len(got) != 1 || got[0] != want[0] {
		t.Errorf("embedded texts = %q, want %q", got, want)
	}
}

func TestServiceProcessUpdateReusesTheDocumentID(t *testing.T) {
	s := newTestService(t)
	ctx := context.Background()

	create := parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "First", "First body.", 1)))
	if err := s.service.Process(ctx, create); err != nil {
		t.Fatalf("Process create: %v", err)
	}

	update := parse(t, debeziumEvent("u",
		documentRow(testDocumentID, "First", "First body.", 1),
		documentRow(testDocumentID, "Second", "Second body.", 2)))
	if err := s.service.Process(ctx, update); err != nil {
		t.Fatalf("Process update: %v", err)
	}

	// An update replaces the document rather than adding a second one.
	if got := s.keyword.count(); got != 1 {
		t.Errorf("keyword index holds %d documents, want 1", got)
	}
	if got := s.vectors.count(); got != 1 {
		t.Errorf("vector index holds %d points, want 1", got)
	}

	doc, _ := s.keyword.document(testDocumentID)
	if doc.Title != "Second" || doc.Version != 2 {
		t.Errorf("document = %+v, want the updated row", doc)
	}
	if _, ok := s.vectors.point(testDocumentID); !ok {
		t.Error("the point is no longer stored under the document ID")
	}

	// The new text was embedded again, because the content changed.
	if got := s.embedder.calls(); len(got) != 2 || got[1] != "title: Second | text: Second body." {
		t.Errorf("embedded texts = %q, want the updated text second", got)
	}
}

func TestServiceProcessDeleteRemovesFromBothStores(t *testing.T) {
	s := newTestService(t)
	ctx := context.Background()

	row := documentRow(testDocumentID, "Doomed", "Body.", 1)
	if err := s.service.Process(ctx, parse(t, debeziumEvent("c", "null", row))); err != nil {
		t.Fatalf("Process create: %v", err)
	}

	del := parse(t, debeziumEvent("d", row, "null"))
	if del.ID != testDocumentID {
		t.Fatalf("delete event ID = %q, want the id from the before row", del.ID)
	}
	if err := s.service.Process(ctx, del); err != nil {
		t.Fatalf("Process delete: %v", err)
	}

	if s.keyword.deleteCalls != 1 || s.vectors.deleteCalls != 1 {
		t.Errorf("delete calls: keyword = %d, vector = %d; both stores must be asked",
			s.keyword.deleteCalls, s.vectors.deleteCalls)
	}
	if s.keyword.count() != 0 || s.vectors.count() != 0 {
		t.Errorf("after delete: %d documents and %d points remain", s.keyword.count(), s.vectors.count())
	}
	// A delete needs no vector, so no embedding request is made for it.
	if got := len(s.embedder.calls()); got != 1 {
		t.Errorf("embed calls = %d, want 1 (the create only)", got)
	}
}

func TestServiceProcessSnapshotReadIndexesLikeACreate(t *testing.T) {
	row := documentRow(testDocumentID, "Backfilled", "From the initial snapshot.", 1)

	read := newTestService(t)
	if err := read.service.Process(context.Background(), parse(t, debeziumEvent("r", "null", row))); err != nil {
		t.Fatalf("Process snapshot read: %v", err)
	}
	create := newTestService(t)
	if err := create.service.Process(context.Background(), parse(t, debeziumEvent("c", "null", row))); err != nil {
		t.Fatalf("Process create: %v", err)
	}

	readDoc, ok := read.keyword.document(testDocumentID)
	if !ok {
		t.Fatal("a snapshot read indexed nothing")
	}
	createDoc, _ := create.keyword.document(testDocumentID)
	if readDoc != createDoc {
		t.Errorf("snapshot read produced %+v, create produced %+v; they must be identical", readDoc, createDoc)
	}
	if _, ok := read.vectors.point(testDocumentID); !ok {
		t.Error("a snapshot read upserted no point")
	}
}

// Re-processing an event, which Kafka can deliver more than once, must leave
// the same single document and point behind.
func TestServiceProcessIsIdempotent(t *testing.T) {
	s := newTestService(t)
	ctx := context.Background()
	ev := parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "Once", "Only once.", 1)))

	for i := range 5 {
		if err := s.service.Process(ctx, ev); err != nil {
			t.Fatalf("Process %d: %v", i+1, err)
		}
	}

	if got := s.keyword.count(); got != 1 {
		t.Errorf("keyword index holds %d documents after 5 identical events, want 1", got)
	}
	if got := s.vectors.count(); got != 1 {
		t.Errorf("vector index holds %d points after 5 identical events, want 1", got)
	}

	doc, _ := s.keyword.document(testDocumentID)
	if doc.Title != "Once" || doc.Version != 1 {
		t.Errorf("document = %+v, want the original row", doc)
	}

	// Deleting twice is also harmless.
	del := parse(t, debeziumEvent("d", documentRow(testDocumentID, "Once", "Only once.", 1), "null"))
	for i := range 2 {
		if err := s.service.Process(ctx, del); err != nil {
			t.Fatalf("Process delete %d: %v", i+1, err)
		}
	}
	if s.keyword.count() != 0 || s.vectors.count() != 0 {
		t.Error("a repeated delete did not leave both stores empty")
	}
}

// A stale event arriving after a newer one must not put old data back. The row
// version is carried to OpenSearch as an external version for exactly this.
func TestServiceProcessCarriesTheRowVersion(t *testing.T) {
	s := newTestService(t)
	ctx := context.Background()

	newer := parse(t, debeziumEvent("u", "null", documentRow(testDocumentID, "Newer", "Newer body.", 9)))
	if err := s.service.Process(ctx, newer); err != nil {
		t.Fatalf("Process newer: %v", err)
	}
	stale := parse(t, debeziumEvent("u", "null", documentRow(testDocumentID, "Older", "Older body.", 4)))
	if err := s.service.Process(ctx, stale); err != nil {
		t.Fatalf("Process stale: %v", err)
	}

	doc, _ := s.keyword.document(testDocumentID)
	if doc.Version != 9 || doc.Title != "Newer" {
		t.Fatalf("document = %+v, want the version 9 row to survive the stale event", doc)
	}
}

func TestServiceProcessRejectsInvalidEvents(t *testing.T) {
	tests := []struct {
		name    string
		event   ChangeEvent
		wantErr error
	}{
		{
			name:    "no document id",
			event:   ChangeEvent{Operation: OperationCreate, Row: map[string]any{"title": "t"}},
			wantErr: ErrMissingID,
		},
		{
			name:    "operation not mapped",
			event:   ChangeEvent{ID: testDocumentID, Operation: "PATCH", Row: map[string]any{"title": "t"}},
			wantErr: ErrUnknownOperation,
		},
		{
			name:    "nothing to embed",
			event:   ChangeEvent{ID: testDocumentID, Operation: OperationCreate, Row: map[string]any{"title": "  ", "body": ""}},
			wantErr: ErrNoEmbeddingText,
		},
		{
			name:    "row the mapping cannot read",
			event:   ChangeEvent{ID: testDocumentID, Operation: OperationCreate, Row: map[string]any{"title": []any{"t"}}},
			wantErr: ErrMalformedEvent,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestService(t)
			err := s.service.Process(context.Background(), tt.event)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			// Nothing may be written for an event that cannot be processed.
			if s.keyword.indexCalls != 0 || s.vectors.upsertCalls != 0 {
				t.Errorf("wrote to the stores anyway: %d index calls, %d upsert calls",
					s.keyword.indexCalls, s.vectors.upsertCalls)
			}
		})
	}
}

// OpenSearch fails, Qdrant is untouched: the caller must be told, or the
// document would silently never be searchable.
func TestServiceProcessReportsKeywordFailure(t *testing.T) {
	s := newTestService(t)
	want := errors.New("opensearch unavailable")
	s.keyword.indexErr = want

	err := s.service.Process(context.Background(), parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1))))
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want it to wrap %v", err, want)
	}

	var storeErr *StoreError
	if !errors.As(err, &storeErr) || storeErr.Store != StoreKeyword {
		t.Fatalf("err = %v, want a StoreError naming %q", err, StoreKeyword)
	}
	if storeErr.DocumentID != testDocumentID {
		t.Errorf("StoreError.DocumentID = %q, want %q", storeErr.DocumentID, testDocumentID)
	}
	// Writing the vector for a document the keyword index rejected would leave
	// a point behind that nothing ever removes.
	if s.vectors.upsertCalls != 0 {
		t.Errorf("vector upsert calls = %d, want 0 after the keyword write failed", s.vectors.upsertCalls)
	}
}

// OpenSearch succeeded and Qdrant failed. This is the one partial state an
// upsert can reach, and it must be reported rather than passed off as success.
func TestServiceProcessReportsVectorFailure(t *testing.T) {
	s := newTestService(t)
	want := errors.New("qdrant unavailable")
	s.vectors.upsertErr = want

	err := s.service.Process(context.Background(), parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1))))
	if err == nil {
		t.Fatal("Process returned nil although the vector index rejected the write")
	}
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want it to wrap %v", err, want)
	}

	var storeErr *StoreError
	if !errors.As(err, &storeErr) || storeErr.Store != StoreVector {
		t.Fatalf("err = %v, want a StoreError naming %q", err, StoreVector)
	}
	// The stores are now out of step: the keyword index holds the document and
	// the vector index does not. Re-processing the event converges them.
	if _, ok := s.keyword.document(testDocumentID); !ok {
		t.Error("expected the keyword write to have gone through before the failure")
	}
	if s.vectors.count() != 0 {
		t.Error("expected no point to be stored")
	}

	s.vectors.upsertErr = nil
	if err := s.service.Process(context.Background(), parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1)))); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if s.keyword.count() != 1 || s.vectors.count() != 1 {
		t.Fatalf("after the retry: %d documents, %d points; want 1 and 1",
			s.keyword.count(), s.vectors.count())
	}
}

// Keyword indexing runs before the embedder, so that keyword search stays
// current while the embedding provider is down. The failure must still be
// reported, because the vector index is behind until the event is retried.
func TestServiceProcessReportsEmbedderFailure(t *testing.T) {
	s := newTestService(t)
	want := errors.New("embedding api rate limited")
	s.embedder.err = want

	err := s.service.Process(context.Background(), parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1))))
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want it to wrap %v", err, want)
	}

	var storeErr *StoreError
	if !errors.As(err, &storeErr) || storeErr.Store != StoreEmbedder {
		t.Fatalf("err = %v, want a StoreError naming %q", err, StoreEmbedder)
	}
	if _, ok := s.keyword.document(testDocumentID); !ok {
		t.Error("the keyword index was not written before the embedder was called")
	}
	if s.vectors.upsertCalls != 0 {
		t.Errorf("vector upsert calls = %d, want 0: there is no vector to store", s.vectors.upsertCalls)
	}

	// Once the provider recovers, re-processing the same event converges both
	// stores on one document and one point.
	s.embedder.mu.Lock()
	s.embedder.err = nil
	s.embedder.mu.Unlock()
	if err := s.service.Process(context.Background(), parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1)))); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if s.keyword.count() != 1 || s.vectors.count() != 1 {
		t.Fatalf("after the retry: %d documents, %d points; want 1 and 1", s.keyword.count(), s.vectors.count())
	}
}

// A delete must reach both stores even when the first one fails, or a document
// would stay searchable in the index that was skipped.
func TestServiceProcessDeleteAsksBothStoresDespiteFailure(t *testing.T) {
	keywordErr := errors.New("opensearch unavailable")
	vectorErr := errors.New("qdrant unavailable")

	tests := []struct {
		name       string
		keywordErr error
		vectorErr  error
		wantErrs   []error
	}{
		{"keyword fails", keywordErr, nil, []error{keywordErr}},
		{"vector fails", nil, vectorErr, []error{vectorErr}},
		{"both fail", keywordErr, vectorErr, []error{keywordErr, vectorErr}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestService(t)
			s.keyword.deleteErr = tt.keywordErr
			s.vectors.deleteErr = tt.vectorErr

			del := parse(t, debeziumEvent("d", documentRow(testDocumentID, "T", "B", 1), "null"))
			err := s.service.Process(context.Background(), del)
			if err == nil {
				t.Fatal("Process returned nil although a delete failed")
			}
			for _, want := range tt.wantErrs {
				if !errors.Is(err, want) {
					t.Errorf("err = %v, want it to report %v", err, want)
				}
			}
			if s.keyword.deleteCalls != 1 || s.vectors.deleteCalls != 1 {
				t.Errorf("delete calls: keyword = %d, vector = %d; both must be asked even when one fails",
					s.keyword.deleteCalls, s.vectors.deleteCalls)
			}
		})
	}
}

func TestServiceProcessStopsOnCancelledContext(t *testing.T) {
	s := newTestService(t)
	s.embedder.err = context.Canceled

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.service.Process(ctx, parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1))))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if s.keyword.indexCalls != 0 || s.vectors.upsertCalls != 0 {
		t.Error("a cancelled event still wrote to the stores")
	}
}

// One service is shared by every worker, so concurrent use must be safe. Run
// with -race for this to mean anything.
func TestServiceProcessIsSafeForConcurrentUse(t *testing.T) {
	s := newTestService(t)
	const documents, repeats = 8, 25

	var wg sync.WaitGroup
	for d := range documents {
		id := fmt.Sprintf("doc-%d", d)
		for r := range repeats {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ev := ChangeEvent{
					ID:        id,
					Operation: OperationUpdate,
					Schema:    "public",
					Table:     "documents",
					Row:       map[string]any{"title": id, "body": fmt.Sprintf("body %d", r)},
				}
				if err := s.service.Process(context.Background(), ev); err != nil {
					t.Errorf("Process: %v", err)
				}
			}()
		}
	}
	wg.Wait()

	if got := s.keyword.count(); got != documents {
		t.Errorf("keyword index holds %d documents, want %d", got, documents)
	}
	if got := s.vectors.count(); got != documents {
		t.Errorf("vector index holds %d points, want %d", got, documents)
	}
}

func TestNewServiceRejectsIncompleteDependencies(t *testing.T) {
	keyword, vectors, embedder := newFakeKeyword(), newFakeVectors(), &fakeEmbedder{}

	tests := []struct {
		name     string
		keyword  KeywordIndex
		vectors  VectorIndex
		embedder Embedder
		mapping  FieldMapping
	}{
		{"no keyword index", nil, vectors, embedder, DefaultMapping()},
		{"no vector index", keyword, nil, embedder, DefaultMapping()},
		{"no embedder", keyword, vectors, nil, DefaultMapping()},
		{"mapping names no text column", keyword, vectors, embedder, FieldMapping{Version: "version"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewService(tt.keyword, tt.vectors, tt.embedder, tt.mapping); err == nil {
				t.Fatal("NewService returned no error")
			}
		})
	}

	if _, err := NewService(keyword, vectors, embedder, DefaultMapping()); err != nil {
		t.Fatalf("NewService with complete dependencies: %v", err)
	}
}

// EmbedderFunc is how a client whose method has a different name is plugged in
// without this package knowing about the provider.
func TestEmbedderFuncAdaptsAFunction(t *testing.T) {
	var got string
	embedder := EmbedderFunc(func(_ context.Context, text string) ([]float32, error) {
		got = text
		return []float32{1}, nil
	})

	service, err := NewService(newFakeKeyword(), newFakeVectors(), embedder, DefaultMapping())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	ev := parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "Title", "Body.", 1)))
	if err := service.Process(context.Background(), ev); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if want := "title: Title | text: Body."; got != want {
		t.Fatalf("embedded text = %q, want %q", got, want)
	}
}
