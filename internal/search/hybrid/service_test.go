package hybrid

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
	"near-real-time-hybrid-search-engine/internal/search/rrf"
)

// fakeKeyword stands in for OpenSearch.
type fakeKeyword struct {
	results []opensearch.SearchResult
	err     error
	// block, if set, runs before the fake answers; returning an error makes
	// the call fail with it.
	block func(ctx context.Context) error

	mu      sync.Mutex
	queries []string
	limits  []int
}

func (f *fakeKeyword) Search(ctx context.Context, query string, limit int) ([]opensearch.SearchResult, error) {
	f.mu.Lock()
	f.queries, f.limits = append(f.queries, query), append(f.limits, limit)
	f.mu.Unlock()
	if f.block != nil {
		if err := f.block(ctx); err != nil {
			return nil, err
		}
	}
	return f.results, f.err
}

func (f *fakeKeyword) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queries)
}

// fakeVectors stands in for Qdrant.
type fakeVectors struct {
	results []qdrant.SearchResult
	err     error

	mu      sync.Mutex
	vectors [][]float32
	limits  []int
}

func (f *fakeVectors) Search(_ context.Context, vector []float32, limit int) ([]qdrant.SearchResult, error) {
	f.mu.Lock()
	f.vectors, f.limits = append(f.vectors, vector), append(f.limits, limit)
	f.mu.Unlock()
	return f.results, f.err
}

func (f *fakeVectors) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.vectors)
}

// fakeEmbedder stands in for the embedding provider.
type fakeEmbedder struct {
	vector []float32
	err    error
	block  func(ctx context.Context) error

	mu    sync.Mutex
	texts []string
}

func (f *fakeEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	f.mu.Lock()
	f.texts = append(f.texts, text)
	f.mu.Unlock()
	if f.block != nil {
		if err := f.block(ctx); err != nil {
			return nil, err
		}
	}
	return f.vector, f.err
}

func (f *fakeEmbedder) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.texts)
}

var testConfig = config.SearchConfig{DefaultLimit: 10, MaxLimit: 50, CandidateLimit: 50, RRFK: rrf.DefaultK}

var queryVector = []float32{0.1, 0.2, 0.3, 0.4}

func newTestService(t *testing.T, kw *fakeKeyword, vec *fakeVectors, emb *fakeEmbedder, cfg config.SearchConfig) *Service {
	t.Helper()
	if emb.vector == nil && emb.err == nil {
		emb.vector = queryVector
	}
	s, err := New(kw, vec, emb, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// keywordHits builds OpenSearch results for ids, best first.
func keywordHits(ids ...string) []opensearch.SearchResult {
	out := make([]opensearch.SearchResult, len(ids))
	for i, id := range ids {
		out[i] = opensearch.SearchResult{ID: id, Score: float64(len(ids) - i), Document: opensearch.Document{
			ID: id, Title: "keyword title " + id, Content: "content " + id, URL: "https://example.com/" + id,
		}}
	}
	return out
}

// vectorHits builds Qdrant results for ids, best first, with the payload the
// indexing layer writes.
func vectorHits(ids ...string) []qdrant.SearchResult {
	out := make([]qdrant.SearchResult, len(ids))
	for i, id := range ids {
		out[i] = qdrant.SearchResult{ID: id, Score: 0.9 - float64(i)/100, Payload: map[string]any{
			"document_id": id, "title": "vector title " + id, "url": "https://example.com/" + id, "version": int64(1),
		}}
	}
	return out
}

func manyIDs(prefix string, n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("%s-%02d", prefix, i)
	}
	return out
}

func resultIDs(rs []Result) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

func rrfScore(k int, ranks ...int) float64 {
	var sum float64
	for _, r := range ranks {
		sum += 1 / float64(k+r)
	}
	return sum
}

func TestSearchFusesKeywordAndVectorResults(t *testing.T) {
	kw := &fakeKeyword{results: keywordHits("A", "B", "C")}
	vec := &fakeVectors{results: vectorHits("C", "A", "D")}
	emb := &fakeEmbedder{}
	s := newTestService(t, kw, vec, emb, testConfig)

	results, err := s.Search(context.Background(), SearchRequest{Query: "  distributed systems with kafka  ", Limit: 10})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	// Each dependency got what it should.
	if kw.queries[0] != "distributed systems with kafka" || kw.limits[0] != testConfig.CandidateLimit {
		t.Errorf("keyword search got (%q, %d), want the trimmed query and the candidate limit", kw.queries[0], kw.limits[0])
	}
	if want := QueryEmbeddingText("distributed systems with kafka"); emb.texts[0] != want {
		t.Errorf("embedder got %q, want %q", emb.texts[0], want)
	}
	if !slices.Equal(vec.vectors[0], queryVector) || vec.limits[0] != testConfig.CandidateLimit {
		t.Errorf("vector search got (%v, %d), want the query vector and the candidate limit", vec.vectors[0], vec.limits[0])
	}

	// A: keyword 1 + vector 2; C: keyword 3 + vector 1; B: keyword 2; D: vector 3.
	wantOrder := []string{"A", "C", "B", "D"}
	wantScores := []float64{rrfScore(60, 1, 2), rrfScore(60, 3, 1), rrfScore(60, 2), rrfScore(60, 3)}
	if got := resultIDs(results); !slices.Equal(got, wantOrder) {
		t.Fatalf("order = %v, want %v", got, wantOrder)
	}
	for i, r := range results {
		if math.Abs(r.Score-wantScores[i]) > 1e-12 {
			t.Errorf("%s: score %v, want %v", r.ID, r.Score, wantScores[i])
		}
	}

	// A was found by both: it carries the OpenSearch document, body included.
	if a := results[0]; a.Title != "keyword title A" || a.Content != "content A" || a.URL != "https://example.com/A" {
		t.Errorf("A = %+v, want the keyword document", a)
	}
	// D was found only by vector search: title and URL from the vector payload.
	if d := results[3]; d.Title != "vector title D" || d.URL != "https://example.com/D" || d.Content != "" {
		t.Errorf("D = %+v, want the vector payload's title and URL and no content", d)
	}
}

func TestSearchRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		req  SearchRequest
	}{
		{"empty query", SearchRequest{Query: ""}},
		{"whitespace query", SearchRequest{Query: " \t\n "}},
		{"query too long", SearchRequest{Query: strings.Repeat("a", MaxQueryLength+1)}},
		{"invalid utf-8", SearchRequest{Query: "kafka \xff"}},
		{"negative limit", SearchRequest{Query: "kafka", Limit: -1}},
		{"limit above maximum", SearchRequest{Query: "kafka", Limit: testConfig.MaxLimit + 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kw, vec, emb := &fakeKeyword{}, &fakeVectors{}, &fakeEmbedder{}
			s := newTestService(t, kw, vec, emb, testConfig)

			_, err := s.Search(context.Background(), tt.req)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("err = %v, want ErrInvalidRequest", err)
			}
			if kw.calls()+vec.calls()+emb.calls() != 0 {
				t.Error("a dependency was called for an invalid request")
			}
		})
	}

	// A query exactly at the limit is fine; length counts characters, not bytes.
	s := newTestService(t, &fakeKeyword{}, &fakeVectors{}, &fakeEmbedder{}, testConfig)
	if _, err := s.Search(context.Background(), SearchRequest{Query: strings.Repeat("é", MaxQueryLength)}); err != nil {
		t.Errorf("query of %d characters rejected: %v", MaxQueryLength, err)
	}
}

func TestSearchLimit(t *testing.T) {
	kw := &fakeKeyword{results: keywordHits(manyIDs("kw", 50)...)}
	vec := &fakeVectors{results: vectorHits(manyIDs("vec", 50)...)}
	s := newTestService(t, kw, vec, &fakeEmbedder{}, testConfig)

	for _, tt := range []struct{ requested, want int }{
		{0, testConfig.DefaultLimit}, // zero means the default
		{1, 1},
		{7, 7},
		{testConfig.MaxLimit, testConfig.MaxLimit},
	} {
		results, err := s.Search(context.Background(), SearchRequest{Query: "kafka", Limit: tt.requested})
		if err != nil {
			t.Fatalf("limit %d: %v", tt.requested, err)
		}
		if len(results) != tt.want {
			t.Errorf("limit %d: got %d results, want %d", tt.requested, len(results), tt.want)
		}
	}

	// However few results are wanted, each retriever supplies a full candidate
	// list, so fusion can surface documents that rank moderately in both.
	for i, l := range kw.limits {
		if l != testConfig.CandidateLimit || vec.limits[i] != testConfig.CandidateLimit {
			t.Fatalf("retrievers asked for %d and %d, want %d each", l, vec.limits[i], testConfig.CandidateLimit)
		}
	}
}

func TestSearchReturnsEachDocumentOnce(t *testing.T) {
	kw := &fakeKeyword{results: keywordHits("A", "B")}
	vec := &fakeVectors{results: vectorHits("B", "C")}
	s := newTestService(t, kw, vec, &fakeEmbedder{}, testConfig)

	results, err := s.Search(context.Background(), SearchRequest{Query: "kafka"})
	if err != nil {
		t.Fatal(err)
	}
	// B is in both lists (ranks 2 and 1) and comes first; A and C once each.
	if got := resultIDs(results); !slices.Equal(got, []string{"B", "A", "C"}) {
		t.Errorf("results = %v, want [B A C]", got)
	}
}

// TestSearchPassesRankedListsToRRF checks that the service hands RRF the keyword
// list and then the vector list, each in retriever order, with the configured
// k: its output must equal calling rrf.Fuse on exactly those lists.
func TestSearchPassesRankedListsToRRF(t *testing.T) {
	cfg := testConfig
	cfg.RRFK = 5 // not the default, so the test also proves config k is used

	kw := &fakeKeyword{results: keywordHits("D", "A", "E", "B")}
	vec := &fakeVectors{results: vectorHits("B", "F", "A")}
	s := newTestService(t, kw, vec, &fakeEmbedder{}, cfg)

	results, err := s.Search(context.Background(), SearchRequest{Query: "kafka", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}

	want, err := rrf.Fuse([][]rrf.Result{
		{{ID: "D"}, {ID: "A"}, {ID: "E"}, {ID: "B"}},
		{{ID: "B"}, {ID: "F"}, {ID: "A"}},
	}, 5, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(want) {
		t.Fatalf("got %d results, want %d", len(results), len(want))
	}
	for i := range want {
		if results[i].ID != want[i].ID || results[i].Score != want[i].Score {
			t.Errorf("position %d: got %s (%v), want %s (%v)", i+1, results[i].ID, results[i].Score, want[i].ID, want[i].Score)
		}
	}
}

func TestSearchEmptyResultSets(t *testing.T) {
	tests := []struct {
		name    string
		keyword []opensearch.SearchResult
		vector  []qdrant.SearchResult
		want    []string
	}{
		{"keyword empty", nil, vectorHits("A", "B"), []string{"A", "B"}},
		{"vector empty", keywordHits("A", "B"), nil, []string{"A", "B"}},
		{"both empty", nil, nil, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestService(t, &fakeKeyword{results: tt.keyword}, &fakeVectors{results: tt.vector}, &fakeEmbedder{}, testConfig)
			results, err := s.Search(context.Background(), SearchRequest{Query: "kafka"})
			if err != nil {
				t.Fatalf("Search: %v", err)
			}
			if results == nil || !slices.Equal(resultIDs(results), tt.want) {
				t.Errorf("results = %v, want %v (non-nil)", resultIDs(results), tt.want)
			}
		})
	}
}

// waitForCancel blocks until ctx is cancelled and records that it was.
func waitForCancel(cancelled *atomic.Bool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			cancelled.Store(true)
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("never cancelled")
		}
	}
}

func TestSearchFailures(t *testing.T) {
	down := errors.New("connection refused")

	t.Run("keyword search fails and the vector side is cancelled", func(t *testing.T) {
		var cancelled atomic.Bool
		kw := &fakeKeyword{err: down}
		emb := &fakeEmbedder{block: waitForCancel(&cancelled)}
		s := newTestService(t, kw, &fakeVectors{}, emb, testConfig)

		_, err := s.Search(context.Background(), SearchRequest{Query: "kafka"})
		if !errors.Is(err, down) || !strings.Contains(err.Error(), "keyword_search") {
			t.Fatalf("err = %v, want the keyword failure", err)
		}
		if !cancelled.Load() {
			t.Error("the embedding call was not cancelled")
		}
	})

	t.Run("embedding fails and the keyword side is cancelled", func(t *testing.T) {
		var cancelled atomic.Bool
		kw := &fakeKeyword{block: waitForCancel(&cancelled)}
		vec := &fakeVectors{}
		s := newTestService(t, kw, vec, &fakeEmbedder{err: down}, testConfig)

		_, err := s.Search(context.Background(), SearchRequest{Query: "kafka"})
		if !errors.Is(err, down) || !strings.Contains(err.Error(), "embedding") {
			t.Fatalf("err = %v, want the embedding failure", err)
		}
		if !cancelled.Load() {
			t.Error("the keyword call was not cancelled")
		}
		if vec.calls() != 0 {
			t.Error("vector search ran without a query vector")
		}
	})

	t.Run("vector search fails", func(t *testing.T) {
		s := newTestService(t, &fakeKeyword{results: keywordHits("A")}, &fakeVectors{err: down}, &fakeEmbedder{}, testConfig)
		_, err := s.Search(context.Background(), SearchRequest{Query: "kafka"})
		if !errors.Is(err, down) || !strings.Contains(err.Error(), "vector_search") {
			t.Fatalf("err = %v, want the vector failure", err)
		}
	})

	t.Run("embedder returns an empty vector", func(t *testing.T) {
		vec := &fakeVectors{}
		s := newTestService(t, &fakeKeyword{}, vec, &fakeEmbedder{vector: []float32{}}, testConfig)
		_, err := s.Search(context.Background(), SearchRequest{Query: "kafka"})
		if err == nil || !strings.Contains(err.Error(), "empty vector") {
			t.Fatalf("err = %v, want an empty-vector error", err)
		}
		if vec.calls() != 0 {
			t.Error("vector search ran with an empty vector")
		}
	})
}

func TestSearchContextCancellation(t *testing.T) {
	var keywordCancelled, embeddingCancelled atomic.Bool
	started := make(chan struct{}, 2)
	signalThen := func(next func(context.Context) error) func(context.Context) error {
		return func(ctx context.Context) error {
			started <- struct{}{}
			return next(ctx)
		}
	}
	kw := &fakeKeyword{block: signalThen(waitForCancel(&keywordCancelled))}
	emb := &fakeEmbedder{block: signalThen(waitForCancel(&embeddingCancelled))}
	s := newTestService(t, kw, &fakeVectors{}, emb, testConfig)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		<-started
		cancel() // the caller gives up while both calls are in flight
	}()

	_, err := s.Search(ctx, SearchRequest{Query: "kafka"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// Search returns only after both calls have returned, so no goroutine is
	// left running; both must have seen the cancellation.
	if !keywordCancelled.Load() || !embeddingCancelled.Load() {
		t.Errorf("keyword cancelled = %v, embedding cancelled = %v; want both", keywordCancelled.Load(), embeddingCancelled.Load())
	}

	t.Run("deadline", func(t *testing.T) {
		var cancelled atomic.Bool
		kw := &fakeKeyword{block: waitForCancel(&cancelled)}
		s := newTestService(t, kw, &fakeVectors{}, &fakeEmbedder{}, testConfig)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if _, err := s.Search(ctx, SearchRequest{Query: "kafka"}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
	})
}

// TestSearchRunsRetrieversConcurrently makes each side wait until the other
// has started. Run one after the other, the first would wait forever, so the
// test only passes if both are in flight at the same time.
func TestSearchRunsRetrieversConcurrently(t *testing.T) {
	keywordStarted := make(chan struct{})
	vectorStarted := make(chan struct{})
	waitFor := func(mine, other chan struct{}, name string) func(context.Context) error {
		return func(ctx context.Context) error {
			close(mine)
			select {
			case <-other:
				return nil
			case <-time.After(2 * time.Second):
				return fmt.Errorf("%s ran alone: searches are sequential", name)
			}
		}
	}

	kw := &fakeKeyword{results: keywordHits("A"), block: waitFor(keywordStarted, vectorStarted, "keyword search")}
	emb := &fakeEmbedder{block: waitFor(vectorStarted, keywordStarted, "embedding")}
	s := newTestService(t, kw, &fakeVectors{results: vectorHits("B")}, emb, testConfig)

	results, err := s.Search(context.Background(), SearchRequest{Query: "kafka"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}
}

func TestSearchLogsNoQueryOrVector(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	emb := &fakeEmbedder{vector: []float32{0.123456, 0.654321}}
	s, err := New(&fakeKeyword{results: keywordHits("A")}, &fakeVectors{results: vectorHits("B")}, emb, testConfig, logger)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Search(context.Background(), SearchRequest{Query: "patient record 4411"}); err != nil {
		t.Fatal(err)
	}
	logged := buf.String()
	for _, secret := range []string{"patient record 4411", "0.123456", "0.654321"} {
		if strings.Contains(logged, secret) {
			t.Errorf("log contains %q: %s", secret, logged)
		}
	}
	for _, field := range []string{"hybrid search completed", "keyword_duration", "embedding_duration",
		"vector_duration", "fusion_duration", "query_length=19", "results=2"} {
		if !strings.Contains(logged, field) {
			t.Errorf("log is missing %q: %s", field, logged)
		}
	}

	buf.Reset()
	emb.err = errors.New("quota exceeded")
	if _, err := s.Search(context.Background(), SearchRequest{Query: "patient record 4411"}); err == nil {
		t.Fatal("expected an error")
	}
	if logged := buf.String(); !strings.Contains(logged, "stage=embedding") || strings.Contains(logged, "patient record 4411") {
		t.Errorf("failure log = %s, want stage=embedding and no query text", logged)
	}
}

func TestNewValidatesDependenciesAndConfig(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	if _, err := New(nil, &fakeVectors{}, &fakeEmbedder{}, testConfig, logger); err == nil {
		t.Error("New accepted a nil keyword searcher")
	}
	if _, err := New(&fakeKeyword{}, &fakeVectors{}, &fakeEmbedder{}, testConfig, nil); err == nil {
		t.Error("New accepted a nil logger")
	}

	for name, change := range map[string]func(*config.SearchConfig){
		"zero default limit":          func(c *config.SearchConfig) { c.DefaultLimit = 0 },
		"max below default":           func(c *config.SearchConfig) { c.MaxLimit = 5 },
		"candidates below max":        func(c *config.SearchConfig) { c.CandidateLimit = 20 },
		"candidates above retrievers": func(c *config.SearchConfig) { c.CandidateLimit = opensearch.MaxSearchLimit + 1 },
		"zero rrf k":                  func(c *config.SearchConfig) { c.RRFK = 0 },
	} {
		cfg := testConfig
		change(&cfg)
		if _, err := New(&fakeKeyword{}, &fakeVectors{}, &fakeEmbedder{}, cfg, logger); err == nil {
			t.Errorf("%s: New accepted %+v", name, cfg)
		}
	}
}

func TestQueryEmbeddingText(t *testing.T) {
	if got, want := QueryEmbeddingText("kafka streams"), "task: search result | query: kafka streams"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
