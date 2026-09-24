package rrf

import (
	"fmt"
	"testing"
)

// Fusion is the one piece of a search request that is pure computation: no
// network, no I/O, nothing to wait for. If a search is slow, this is where to
// prove it is not the cause.
//
// These are micro-benchmarks. They say what one call costs, not what the
// system does under traffic, which is what benchmarks/search measures.
//
//	go test -run '^$' -bench . -benchmem ./internal/search/rrf/

// benchResults builds one retriever's ranked list of size n, with overlap
// controlling how many of its IDs the other list also holds. Overlap is what
// fusion actually works on: with none, it is a merge; with full overlap, every
// document is scored twice.
func benchResults(prefix string, n, offset int) []Result {
	results := make([]Result, 0, n)
	for i := range n {
		results = append(results, Result{
			ID:      fmt.Sprintf("%s-%d", prefix, i+offset),
			Payload: struct{ Title string }{Title: "document"},
		})
	}
	return results
}

func BenchmarkFuse(b *testing.B) {
	sizes := []int{10, 50, 200, 1000}
	for _, size := range sizes {
		// Half of each list is in the other, which is roughly what keyword and
		// vector search return for the same query.
		keyword := benchResults("doc", size, 0)
		vector := benchResults("doc", size, size/2)

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			sets := [][]Result{keyword, vector}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Fuse(sets, DefaultK, 10); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Fusing more than two lists is what adding a third retriever would cost.
func BenchmarkFuseRetrieverCount(b *testing.B) {
	const size = 100
	for _, retrievers := range []int{2, 3, 5} {
		sets := make([][]Result, 0, retrievers)
		for r := range retrievers {
			sets = append(sets, benchResults("doc", size, r*size/2))
		}

		b.Run(fmt.Sprintf("retrievers=%d", retrievers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Fuse(sets, DefaultK, 10); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The limit decides how much of the fused list is returned, not how much is
// scored, so this shows whether asking for a bigger page costs anything here.
func BenchmarkFuseLimit(b *testing.B) {
	keyword := benchResults("doc", 200, 0)
	vector := benchResults("doc", 200, 100)
	sets := [][]Result{keyword, vector}

	for _, limit := range []int{5, 10, 20, 50} {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Fuse(sets, DefaultK, limit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
