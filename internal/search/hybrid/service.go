// Package hybrid answers a search query by running keyword and vector search
// side by side and fusing their rankings with Reciprocal Rank Fusion.
//
// The package only orchestrates. Building the keyword query, calling Qdrant,
// generating embeddings and scoring with RRF all stay in their own packages;
// this one decides what runs, in what order, and what the caller gets back:
//
//	query ─┬─ keyword search (OpenSearch, BM25) ───────────────┬─ RRF ─ results
//	       └─ embed query ─ vector search (Qdrant, cosine) ────┘
package hybrid

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"

	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/embedding"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
	"near-real-time-hybrid-search-engine/internal/search/rrf"
)

// KeywordSearcher runs a full-text search. *opensearch.Client implements it.
type KeywordSearcher interface {
	Search(ctx context.Context, query string, limit int) ([]opensearch.SearchResult, error)
}

// VectorSearcher finds the stored vectors closest to a query vector.
// *qdrant.Client implements it.
type VectorSearcher interface {
	Search(ctx context.Context, vector []float32, limit int) ([]qdrant.SearchResult, error)
}

// Embedder turns text into a vector. Every embedding.Provider implements it.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

var (
	_ KeywordSearcher = (*opensearch.Client)(nil)
	_ VectorSearcher  = (*qdrant.Client)(nil)
	_ Embedder        = embedding.Provider(nil)
)

// Service runs hybrid searches. It does not own its dependencies; the caller
// creates and closes them. It is safe for concurrent use.
type Service struct {
	keyword  KeywordSearcher
	vectors  VectorSearcher
	embedder Embedder
	cfg      config.SearchConfig
	logger   *slog.Logger
}

// New returns a Service that searches with the given dependencies.
func New(keyword KeywordSearcher, vectors VectorSearcher, embedder Embedder, cfg config.SearchConfig, logger *slog.Logger) (*Service, error) {
	if keyword == nil || vectors == nil || embedder == nil || logger == nil {
		return nil, errors.New("hybrid search: keyword searcher, vector searcher, embedder and logger are required")
	}
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("hybrid search: %w", err)
	}
	return &Service{keyword: keyword, vectors: vectors, embedder: embedder, cfg: cfg, logger: logger}, nil
}

// validateConfig checks the settings against each other and against what the
// retrievers accept, so a bad combination fails at startup, not per request.
func validateConfig(cfg config.SearchConfig) error {
	maxCandidates := min(opensearch.MaxSearchLimit, qdrant.MaxSearchLimit)
	switch {
	case cfg.DefaultLimit < 1:
		return fmt.Errorf("default limit must be at least 1, got %d", cfg.DefaultLimit)
	case cfg.MaxLimit < cfg.DefaultLimit:
		return fmt.Errorf("max limit (%d) must not be below the default limit (%d)", cfg.MaxLimit, cfg.DefaultLimit)
	case cfg.CandidateLimit < cfg.MaxLimit:
		return fmt.Errorf("candidate limit (%d) must not be below the max limit (%d)", cfg.CandidateLimit, cfg.MaxLimit)
	case cfg.CandidateLimit > maxCandidates:
		return fmt.Errorf("candidate limit (%d) exceeds what the retrievers return (%d)", cfg.CandidateLimit, maxCandidates)
	case cfg.RRFK < 1:
		return fmt.Errorf("rrf k must be at least 1, got %d", cfg.RRFK)
	}
	return nil
}

// QueryEmbeddingText returns the text embedded for a search query:
//
//	task: search result | query: {query}
//
// It is the query-side counterpart of cdc.BuildEmbeddingText, which embeds
// documents as "title: ... | text: ...". gemini-embedding-2 has no task type
// parameter and uses these prefixes to tell queries from documents; a query
// embedded without its prefix lands in the wrong place relative to the
// documents. Change the two rules together, or not at all.
func QueryEmbeddingText(query string) string {
	return "task: search result | query: " + query
}

// Search runs keyword and vector search for req concurrently, fuses the two
// rankings with RRF and returns at most the requested number of results, best
// first.
//
// Both retrievers must succeed. If either fails, the other is cancelled and
// the error is returned: a keyword-only or vector-only answer would look like
// a hybrid one while silently ranking worse. If ctx is cancelled or its
// deadline passes, every in-flight call is cancelled and the returned error
// matches ctx.Err() with errors.Is.
func (s *Service) Search(ctx context.Context, req SearchRequest) ([]Result, error) {
	query, limit, err := s.validate(req)
	if err != nil {
		return nil, err
	}

	st := stats{queryLength: utf8.RuneCountInString(query), limit: limit}
	start := time.Now()
	results, err := s.search(ctx, query, limit, &st)
	st.total = time.Since(start)
	st.results = len(results)
	s.record(ctx, st, err)
	return results, err
}

func (s *Service) validate(req SearchRequest) (query string, limit int, err error) {
	query = strings.TrimSpace(req.Query)
	invalid := func(format string, args ...any) error {
		return &ValidationError{Reason: fmt.Sprintf(format, args...)}
	}
	switch {
	case query == "":
		return "", 0, invalid("query is empty")
	case !utf8.ValidString(query):
		return "", 0, invalid("query is not valid UTF-8")
	case utf8.RuneCountInString(query) > MaxQueryLength:
		return "", 0, invalid("query is longer than %d characters", MaxQueryLength)
	case req.Limit < 0:
		return "", 0, invalid("limit must not be negative, got %d", req.Limit)
	case req.Limit > s.cfg.MaxLimit:
		return "", 0, invalid("limit %d exceeds the maximum of %d", req.Limit, s.cfg.MaxLimit)
	}
	limit = req.Limit
	if limit == 0 {
		limit = s.cfg.DefaultLimit
	}
	return query, limit, nil
}

func (s *Service) search(ctx context.Context, query string, limit int, st *stats) ([]Result, error) {
	var (
		keywordHits []opensearch.SearchResult
		vectorHits  []qdrant.SearchResult
	)

	// groupCtx is cancelled as soon as either side fails, which stops the
	// other; Wait returns only once both have returned, so nothing leaks.
	g, groupCtx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		keywordHits, err = s.searchKeyword(groupCtx, query, st)
		return err
	})
	g.Go(func() (err error) {
		vectorHits, err = s.searchVector(groupCtx, query, st)
		return err
	})
	err := g.Wait()

	// When the caller gave up, that is the reason, whichever call noticed first.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("hybrid search: %w", ctxErr)
	}
	if err != nil {
		return nil, err
	}
	st.keywordHits, st.vectorHits = len(keywordHits), len(vectorHits)

	return s.fuse(keywordHits, vectorHits, limit, st)
}

// searchKeyword fetches the BM25 candidates.
func (s *Service) searchKeyword(ctx context.Context, query string, st *stats) ([]opensearch.SearchResult, error) {
	start := time.Now()
	hits, err := s.keyword.Search(ctx, query, s.cfg.CandidateLimit)
	st.keywordTime = time.Since(start)
	if err != nil {
		return nil, &stageError{stage: stageKeyword, err: err}
	}
	return hits, nil
}

// searchVector embeds the query and fetches the nearest vectors.
func (s *Service) searchVector(ctx context.Context, query string, st *stats) ([]qdrant.SearchResult, error) {
	start := time.Now()
	vector, err := s.embedder.Embed(ctx, QueryEmbeddingText(query))
	st.embeddingTime = time.Since(start)
	if err != nil {
		return nil, &stageError{stage: stageEmbedding, err: err}
	}
	if len(vector) == 0 {
		return nil, &stageError{stage: stageEmbedding, err: errors.New("provider returned an empty vector")}
	}

	start = time.Now()
	hits, err := s.vectors.Search(ctx, vector, s.cfg.CandidateLimit)
	st.vectorTime = time.Since(start)
	if err != nil {
		return nil, &stageError{stage: stageVector, err: err}
	}
	return hits, nil
}

// fuse ranks the two candidate lists together with RRF and converts the top
// limit into Results.
//
// The keyword list goes first. RRF keeps the first payload it sees for a
// document, so a document both retrievers found carries the full OpenSearch
// document, body included, rather than the smaller vector payload.
func (s *Service) fuse(keywordHits []opensearch.SearchResult, vectorHits []qdrant.SearchResult, limit int, st *stats) ([]Result, error) {
	start := time.Now()
	defer func() { st.fusionTime = time.Since(start) }()

	keywordList := make([]rrf.Result, len(keywordHits))
	for i, h := range keywordHits {
		keywordList[i] = rrf.Result{ID: h.ID, Payload: h.Document}
	}
	vectorList := make([]rrf.Result, len(vectorHits))
	for i, h := range vectorHits {
		vectorList[i] = rrf.Result{ID: h.ID, Payload: h.Payload}
	}

	ranked, err := rrf.Fuse([][]rrf.Result{keywordList, vectorList}, s.cfg.RRFK, limit)
	if err != nil {
		return nil, &stageError{stage: stageFusion, err: err}
	}

	results := make([]Result, len(ranked))
	for i, r := range ranked {
		results[i] = toResult(r)
	}
	return results, nil
}

// toResult reads the display fields from whichever payload RRF kept.
func toResult(r rrf.RankedResult) Result {
	res := Result{ID: r.ID, Score: r.Score}
	switch p := r.Payload.(type) {
	case opensearch.Document:
		res.Title, res.Content, res.URL = p.Title, p.Content, p.URL
	case map[string]any:
		// Written by the indexing layer; see cdc.FieldMapping.VectorPayload.
		res.Title, _ = p["title"].(string)
		res.URL, _ = p["url"].(string)
	}
	return res
}

// Stage names say which part of a search failed.
const (
	stageKeyword   = "keyword_search"
	stageEmbedding = "embedding"
	stageVector    = "vector_search"
	stageFusion    = "fusion"
)

// stageError wraps a failure with the stage it happened in. The message reads
// "<stage>: <cause>" and the cause stays reachable with errors.Is and
// errors.As, so callers can still test for, say, embedding.ErrRateLimited.
type stageError struct {
	stage string
	err   error
}

func (e *stageError) Error() string { return e.stage + ": " + e.err.Error() }
func (e *stageError) Unwrap() error { return e.err }

// stats describes one search. It is the single record of what a search cost:
// logged today, and the one place metrics will read from later.
//
// The keyword and vector goroutines write different fields, so they never
// touch the same memory; everything is read after both have finished.
type stats struct {
	queryLength int
	limit       int

	keywordHits int
	vectorHits  int
	results     int

	keywordTime   time.Duration
	embeddingTime time.Duration
	vectorTime    time.Duration
	fusionTime    time.Duration
	total         time.Duration
}

// record logs one search. The query text and the query vector are never
// logged: queries can hold personal data, and a vector is large and useless in
// a log line. Only the query's length is recorded.
func (s *Service) record(ctx context.Context, st stats, err error) {
	attrs := []slog.Attr{
		slog.String("operation", "hybrid_search"),
		slog.Int("query_length", st.queryLength),
		slog.Int("limit", st.limit),
		slog.Int("keyword_hits", st.keywordHits),
		slog.Int("vector_hits", st.vectorHits),
		slog.Int("results", st.results),
		slog.Duration("keyword_duration", st.keywordTime),
		slog.Duration("embedding_duration", st.embeddingTime),
		slog.Duration("vector_duration", st.vectorTime),
		slog.Duration("fusion_duration", st.fusionTime),
		slog.Duration("duration", st.total),
	}

	switch {
	case err == nil:
		s.logger.LogAttrs(ctx, slog.LevelDebug, "hybrid search completed", attrs...)
	case ctx.Err() != nil:
		// The caller went away or ran out of time; nothing here failed.
		s.logger.LogAttrs(ctx, slog.LevelInfo, "hybrid search cancelled", append(attrs, slog.Any("error", err))...)
	default:
		stage := "unknown"
		var se *stageError
		if errors.As(err, &se) {
			stage = se.stage
		}
		s.logger.LogAttrs(ctx, slog.LevelWarn, "hybrid search failed",
			append(attrs, slog.String("stage", stage), slog.Any("error", err))...)
	}
}
