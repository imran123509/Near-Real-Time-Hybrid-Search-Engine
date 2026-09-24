package loadtest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeIndexes stands in for OpenSearch and Qdrant over HTTP, so the probing
// and waiting code can be tested without either running.
type fakeIndexes struct {
	mu sync.Mutex
	// present are the document IDs the keyword index holds.
	present map[string]bool
	// appearAfter makes an ID show up only once it has been asked for this
	// many times, which is how a test reproduces a document arriving late.
	appearAfter map[string]int
	asked       map[string]int
	documents   int
	points      int
	refreshes   int
	status      int // when non-zero, every document lookup answers with it
}

func newFakeIndexes() *fakeIndexes {
	return &fakeIndexes{present: map[string]bool{}, appearAfter: map[string]int{}, asked: map[string]int{}}
}

func (f *fakeIndexes) start(t *testing.T) Indexes {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(srv.Close)
	return Indexes{
		OpenSearchURL: srv.URL,
		Index:         "documents",
		QdrantURL:     srv.URL,
		Collection:    "documents",
		HTTP:          srv.Client(),
	}
}

func (f *fakeIndexes) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case strings.HasSuffix(r.URL.Path, "/_count"):
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"count": %d}`, f.documents)

	case strings.HasSuffix(r.URL.Path, "/points/count"):
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"result": {"count": %d}}`, f.points)

	case strings.HasSuffix(r.URL.Path, "/_refresh"):
		f.refreshes++
		w.WriteHeader(http.StatusOK)

	case strings.Contains(r.URL.Path, "/_doc/"):
		if f.status != 0 {
			w.WriteHeader(f.status)
			return
		}
		id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		f.asked[id]++
		if after, ok := f.appearAfter[id]; ok && f.asked[id] > after {
			f.present[id] = true
		}
		if f.present[id] {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeIndexes) add(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.present[id] = true
}

func (f *fakeIndexes) arriveAfter(id string, polls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appearAfter[id] = polls
}

func (f *fakeIndexes) setCounts(documents, points int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.documents, f.points = documents, points
}

func TestDocumentExists(t *testing.T) {
	fake := newFakeIndexes()
	indexes := fake.start(t)
	fake.add("here")

	ctx := context.Background()
	if exists, err := indexes.DocumentExists(ctx, "here"); err != nil || !exists {
		t.Errorf("DocumentExists(here) = %v, %v; want true, nil", exists, err)
	}
	if exists, err := indexes.DocumentExists(ctx, "absent"); err != nil || exists {
		t.Errorf("DocumentExists(absent) = %v, %v; want false, nil", exists, err)
	}

	// Anything other than found or not found is a broken measurement, not an
	// absent document: it has to be reported.
	fake.mu.Lock()
	fake.status = http.StatusServiceUnavailable
	fake.mu.Unlock()
	if _, err := indexes.DocumentExists(ctx, "here"); err == nil {
		t.Error("a 503 was reported as a missing document")
	}
}

func TestCounts(t *testing.T) {
	fake := newFakeIndexes()
	indexes := fake.start(t)
	fake.setCounts(1200, 1199)

	ctx := context.Background()
	documents, err := indexes.CountDocuments(ctx)
	if err != nil || documents != 1200 {
		t.Errorf("CountDocuments = %d, %v; want 1200", documents, err)
	}
	points, err := indexes.CountPoints(ctx)
	if err != nil || points != 1199 {
		t.Errorf("CountPoints = %d, %v; want 1199", points, err)
	}
	if err := indexes.Refresh(ctx); err != nil {
		t.Errorf("Refresh: %v", err)
	}
	fake.mu.Lock()
	refreshes := fake.refreshes
	fake.mu.Unlock()
	if refreshes != 1 {
		t.Errorf("%d refreshes, want 1", refreshes)
	}
}

// The watcher is what turns "the documents arrived" into numbers, so it has to
// wait for every one of them and time each from its own insert.
func TestWatcherWaitsForEveryDocument(t *testing.T) {
	fake := newFakeIndexes()
	indexes := fake.start(t)

	arrivals := []Arrival{
		{ID: "a", Inserted: time.Now()},
		{ID: "b", Inserted: time.Now()},
		{ID: "c", Inserted: time.Now()},
	}
	fake.add("a")            // already indexed
	fake.arriveAfter("b", 1) // arrives on the second poll
	fake.arriveAfter("c", 2) // and this one on the third

	watcher := Watcher{Indexes: indexes, Interval: time.Millisecond, Workers: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := watcher.Wait(ctx, arrivals)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(result.Pending) != 0 {
		t.Errorf("%d documents still pending: %v", len(result.Pending), result.Pending)
	}
	if result.Latency.Count != 3 {
		t.Errorf("recorded %d latencies, want 3", result.Latency.Count)
	}
	if result.Last.IsZero() {
		t.Error("the watcher did not record when the last document arrived")
	}
	if result.Latency.Min <= 0 {
		t.Errorf("latency min = %s, want a positive measurement", result.Latency.Min)
	}
}

// A run that does not finish is still a result: what was measured is
// returned, and what never arrived is named.
func TestWatcherReportsWhatNeverArrived(t *testing.T) {
	fake := newFakeIndexes()
	indexes := fake.start(t)
	fake.add("a")

	arrivals := []Arrival{{ID: "a", Inserted: time.Now()}, {ID: "never", Inserted: time.Now()}}
	watcher := Watcher{Indexes: indexes, Interval: time.Millisecond, Workers: 2}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	result, err := watcher.Wait(ctx, arrivals)
	if err == nil {
		t.Fatal("Wait returned no error although a document never arrived")
	}
	if len(result.Pending) != 1 || result.Pending[0] != "never" {
		t.Errorf("pending = %v, want [never]", result.Pending)
	}
	if result.Latency.Count != 1 {
		t.Errorf("recorded %d latencies, want the one document that did arrive", result.Latency.Count)
	}
}
