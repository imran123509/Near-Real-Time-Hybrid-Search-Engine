package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// MaxSearchLimit is the most results one Search call can return.
const MaxSearchLimit = 100

var (
	// ErrEmptyQuery means the query had no searchable text.
	ErrEmptyQuery = errors.New("search query is empty")
	// ErrInvalidLimit means the limit was outside 1..MaxSearchLimit.
	ErrInvalidLimit = errors.New("search limit out of range")
)

// titleBoost weights a match in the title above the same match in the body.
const titleBoost = "title^2"

// SearchResult is one keyword match.
//
// Search returns results best first, so a result's position in the slice is
// its rank. Score is BM25 and only comparable within one result list; RRF
// combines lists by rank for exactly that reason.
type SearchResult struct {
	ID       string
	Score    float64
	Document Document
}

// Search runs a BM25 full-text query over title and content and returns up to
// limit results, best first.
func (c *Client) Search(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, ErrEmptyQuery
	}
	if limit < 1 || limit > MaxSearchLimit {
		return nil, fmt.Errorf("%w: got %d, want 1 to %d", ErrInvalidLimit, limit, MaxSearchLimit)
	}

	body, err := json.Marshal(newSearchRequest(query, limit))
	if err != nil {
		return nil, fmt.Errorf("encode search request: %w", err)
	}

	resp, err := c.api.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{c.index},
		Body:    bytes.NewReader(body),
	})
	if err != nil {
		var code int
		if resp != nil {
			code = statusCode(resp.Inspect().Response)
		}
		return nil, fmt.Errorf("search opensearch: %w", &RequestError{StatusCode: code, Err: err})
	}
	return toResults(resp.Hits.Hits)
}

type searchRequest struct {
	Size  int         `json:"size"`
	Query searchQuery `json:"query"`
}

type searchQuery struct {
	MultiMatch multiMatch `json:"multi_match"`
}

// multiMatch scores each document by its best-matching field ("best_fields"),
// so a strong title match is not diluted by a long body.
type multiMatch struct {
	Query  string   `json:"query"`
	Fields []string `json:"fields"`
	Type   string   `json:"type"`
}

func newSearchRequest(query string, limit int) searchRequest {
	return searchRequest{
		Size: limit,
		Query: searchQuery{MultiMatch: multiMatch{
			Query:  query,
			Fields: []string{titleBoost, "content"},
			Type:   "best_fields",
		}},
	}
}

// toResults converts OpenSearch hits into SearchResults, keeping their order.
func toResults(hits []opensearchapi.SearchHit) ([]SearchResult, error) {
	results := make([]SearchResult, 0, len(hits))
	for _, hit := range hits {
		var doc Document
		if err := json.Unmarshal(hit.Source, &doc); err != nil {
			return nil, fmt.Errorf("decode search hit %s: %w", hit.ID, err)
		}
		if doc.ID == "" {
			doc.ID = hit.ID
		}
		results = append(results, SearchResult{ID: hit.ID, Score: float64(hit.Score), Document: doc})
	}
	return results, nil
}
