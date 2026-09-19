// Package rrf merges ranked result lists with Reciprocal Rank Fusion.
//
// Each list comes from a different retriever, such as BM25 keyword search and
// vector search. Their scores are on unrelated scales, so RRF ignores them and
// combines the lists by rank alone:
//
//	score(d) = Σ 1 / (k + rank(d))
//
// summed over every list that contains document d, where rank is d's 1-based
// position in that list. The package depends only on the standard library; the
// caller converts each retriever's results into Result values.
package rrf

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
)

// DefaultK is the smoothing constant from the original RRF paper (Cormack,
// Clarke and Büttcher, 2009), which found it to work well across collections.
// Larger values flatten the gap between neighbouring ranks; smaller values
// reward the top of each list more strongly.
const DefaultK = 60

var (
	// ErrInvalidK means k was less than 1.
	ErrInvalidK = errors.New("rrf: k must be at least 1")
	// ErrInvalidLimit means limit was less than 1.
	ErrInvalidLimit = errors.New("rrf: limit must be at least 1")
)

// Result is one entry of a ranked list. Its rank is its position in the list:
// the first element has rank 1.
//
// There is deliberately no score field. RRF never reads retriever scores, and
// carrying one here would suggest otherwise.
type Result struct {
	ID      string
	Payload any
}

// RankedResult is one document after fusion.
type RankedResult struct {
	ID string
	// Score is the fused RRF score. It is only meaningful for ordering within
	// one Fuse call; it is not a probability or a relevance percentage.
	Score float64
	// Payload is the first non-nil payload seen for ID, taking lists in the
	// order they were passed to Fuse. Pass the list with the richest payload
	// first to prefer it. Payloads are never merged or copied.
	Payload any
}

// candidate accumulates one document's appearances across lists.
type candidate struct {
	ranks   []int
	payload any
}

// Fuse combines resultSets with Reciprocal Rank Fusion and returns the top
// limit documents, best first.
//
// Any number of lists is accepted. Nil and empty lists contribute nothing.
// Within a list:
//   - rank is the 1-based position in the list as given;
//   - only the first occurrence of an ID counts, so a retriever cannot boost a
//     document by returning it twice;
//   - entries with an empty ID are skipped.
//
// Skipped entries keep their position, so the documents after them keep the
// rank their retriever gave them.
//
// Results are ordered by score, highest first, and ties by ID ascending, so
// the same input always produces the same order. Fuse does not modify its
// input. It runs in O(n + u log u) time for n input entries and u unique IDs.
func Fuse(resultSets [][]Result, k, limit int) ([]RankedResult, error) {
	if k < 1 {
		return nil, fmt.Errorf("%w, got %d", ErrInvalidK, k)
	}
	if limit < 1 {
		return nil, fmt.Errorf("%w, got %d", ErrInvalidLimit, limit)
	}

	total := 0
	for _, set := range resultSets {
		total += len(set)
	}
	candidates := make(map[string]*candidate, total)
	seen := make(map[string]struct{})

	for _, set := range resultSets {
		clear(seen)
		for i, r := range set {
			if r.ID == "" {
				continue
			}
			if _, dup := seen[r.ID]; dup {
				continue
			}
			seen[r.ID] = struct{}{}

			c, ok := candidates[r.ID]
			if !ok {
				c = &candidate{}
				candidates[r.ID] = c
			}
			c.ranks = append(c.ranks, i+1)
			if c.payload == nil {
				c.payload = r.Payload
			}
		}
	}

	results := make([]RankedResult, 0, len(candidates))
	for id, c := range candidates {
		results = append(results, RankedResult{ID: id, Score: score(c.ranks, k), Payload: c.payload})
	}

	slices.SortFunc(results, func(a, b RankedResult) int {
		if byScore := cmp.Compare(b.Score, a.Score); byScore != 0 {
			return byScore
		}
		return cmp.Compare(a.ID, b.ID)
	})

	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

// score sums 1/(k+rank) over ranks.
//
// Floating-point addition is not associative, so ranks are summed in ascending
// order: two documents with the same ranks, in whichever lists, then get
// exactly the same score and fall through to the ID tie-breaker.
func score(ranks []int, k int) float64 {
	slices.Sort(ranks)
	var sum float64
	for _, r := range ranks {
		sum += 1 / (float64(k) + float64(r)) // float addition cannot overflow for large k
	}
	return sum
}
