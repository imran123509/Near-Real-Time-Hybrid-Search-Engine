package rrf

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
)

const tolerance = 1e-12

// ids builds a ranked list from IDs, in order.
func ids(list ...string) []Result {
	out := make([]Result, len(list))
	for i, id := range list {
		out[i] = Result{ID: id}
	}
	return out
}

// rrf returns the expected score for a document with the given ranks.
func rrf(k int, ranks ...int) float64 {
	var sum float64
	for _, r := range ranks {
		sum += 1 / float64(k+r)
	}
	return sum
}

// want is an expected fused result.
type want struct {
	id    string
	score float64
}

func mustFuse(t *testing.T, sets [][]Result, k, limit int) []RankedResult {
	t.Helper()
	got, err := Fuse(sets, k, limit)
	if err != nil {
		t.Fatalf("Fuse: %v", err)
	}
	return got
}

func check(t *testing.T, got []RankedResult, expected []want) {
	t.Helper()
	if len(got) != len(expected) {
		t.Fatalf("got %d results %v, want %d", len(got), resultIDs(got), len(expected))
	}
	for i, w := range expected {
		if got[i].ID != w.id {
			t.Errorf("position %d: got %s, want %s (full order %v)", i+1, got[i].ID, w.id, resultIDs(got))
		}
		if math.Abs(got[i].Score-w.score) > tolerance {
			t.Errorf("%s: score %.15f, want %.15f", w.id, got[i].Score, w.score)
		}
	}
}

func resultIDs(rs []RankedResult) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

// Test 1
func TestSingleResultSet(t *testing.T) {
	got := mustFuse(t, [][]Result{ids("A", "B", "C")}, DefaultK, 10)
	check(t, got, []want{
		{"A", rrf(60, 1)},
		{"B", rrf(60, 2)},
		{"C", rrf(60, 3)},
	})
}

// Test 2: the example from the feature description.
func TestTwoResultSets(t *testing.T) {
	openSearch := ids("A", "B", "C")
	qdrant := ids("C", "A", "D")

	got := mustFuse(t, [][]Result{openSearch, qdrant}, DefaultK, 10)
	check(t, got, []want{
		{"A", rrf(60, 1, 2)}, // 0.032522...
		{"C", rrf(60, 3, 1)}, // 0.032266...
		{"B", rrf(60, 2)},
		{"D", rrf(60, 3)},
	})
}

// Test 3
func TestDocumentInBothListsIsMergedAndBoosted(t *testing.T) {
	got := mustFuse(t, [][]Result{ids("A", "B"), ids("A", "C")}, DefaultK, 10)
	check(t, got, []want{
		{"A", rrf(60, 1, 1)},
		{"B", rrf(60, 2)}, // B and C tie; ID order decides
		{"C", rrf(60, 2)},
	})
}

// Test 4
func TestEmptyResultSets(t *testing.T) {
	got := mustFuse(t, [][]Result{{}, {}}, DefaultK, 10)
	if got == nil || len(got) != 0 {
		t.Fatalf("got %v, want an empty non-nil slice", got)
	}
}

// Test 5
func TestOneEmptyResultSet(t *testing.T) {
	got := mustFuse(t, [][]Result{ids("A", "B"), {}}, DefaultK, 10)
	check(t, got, []want{{"A", rrf(60, 1)}, {"B", rrf(60, 2)}})
}

// Test 6
func TestLimit(t *testing.T) {
	var ten []string
	for i := range 10 {
		ten = append(ten, fmt.Sprintf("doc-%02d", i))
	}

	got := mustFuse(t, [][]Result{ids(ten...)}, DefaultK, 3)
	check(t, got, []want{
		{"doc-00", rrf(60, 1)},
		{"doc-01", rrf(60, 2)},
		{"doc-02", rrf(60, 3)},
	})

	// A limit above the number of unique documents returns all of them.
	if got := mustFuse(t, [][]Result{ids(ten...), ids(ten...)}, DefaultK, 50); len(got) != 10 {
		t.Errorf("got %d results, want all 10 unique documents", len(got))
	}
}

// Test 7
func TestCustomKChangesScoresAndOrder(t *testing.T) {
	// X is first in one list only; Y is fourth in both.
	sets := [][]Result{ids("X", "a", "b", "Y"), ids("c", "d", "e", "Y")}

	// A small k rewards being at the very top of one list...
	small := mustFuse(t, sets, 1, 2)
	check(t, small, []want{{"X", rrf(1, 1)}, {"c", rrf(1, 1)}})

	// ...while k=60 rewards being found by both retrievers.
	large := mustFuse(t, sets, DefaultK, 1)
	check(t, large, []want{{"Y", rrf(60, 4, 4)}})

	if small[0].Score == large[0].Score {
		t.Error("changing k did not change the scores")
	}
}

// Test 8
func TestDuplicateInsideOneListCountsOnce(t *testing.T) {
	got := mustFuse(t, [][]Result{ids("A", "A", "B")}, DefaultK, 10)
	// A contributes once. B keeps rank 3, the position its retriever gave it.
	check(t, got, []want{{"A", rrf(60, 1)}, {"B", rrf(60, 3)}})
}

// Test 9
func TestTiesAreBrokenByID(t *testing.T) {
	// B and A have the same ranks, just in different lists.
	got := mustFuse(t, [][]Result{ids("B", "A"), ids("A", "B")}, DefaultK, 10)
	check(t, got, []want{{"A", rrf(60, 1, 2)}, {"B", rrf(60, 1, 2)}})

	// P has ranks 1, 2, 7 and Q has ranks 7, 1, 2 across three lists. Added in
	// list order, (1/61 + 1/62) + 1/67 and (1/67 + 1/61) + 1/62 differ in the
	// last bit, which would order P and Q by rounding error instead of by ID.
	sets := [][]Result{
		ids("P", "f1", "f2", "f3", "f4", "f5", "Q"),
		ids("Q", "P"),
		ids("g1", "Q", "g2", "g3", "g4", "g5", "P"),
	}
	first := mustFuse(t, sets, DefaultK, 2)
	check(t, first, []want{{"P", rrf(60, 1, 2, 7)}, {"Q", rrf(60, 1, 2, 7)}})
	if first[0].Score != first[1].Score {
		t.Fatalf("equal ranks gave unequal scores: %.20f vs %.20f", first[0].Score, first[1].Score)
	}

	// The order does not depend on the order the lists are passed in.
	slices.Reverse(sets)
	if again := resultIDs(mustFuse(t, sets, DefaultK, 2)); !slices.Equal(again, resultIDs(first)) {
		t.Errorf("reordering the lists changed the result: %v vs %v", again, resultIDs(first))
	}
}

// Test 10
func TestNilAndInvalidInput(t *testing.T) {
	for name, sets := range map[string][][]Result{
		"nil sets":            nil,
		"nil inner list":      {nil, ids("A")},
		"only nil lists":      {nil, nil},
		"empty id is skipped": {{{ID: ""}, {ID: "A"}}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := Fuse(sets, DefaultK, 10)
			if err != nil {
				t.Fatalf("Fuse: %v", err)
			}
			for _, r := range got {
				if r.ID == "" {
					t.Error("result with an empty ID")
				}
			}
		})
	}

	// A skipped entry keeps its position, so A keeps rank 2.
	got := mustFuse(t, [][]Result{{{ID: ""}, {ID: "A"}}}, DefaultK, 10)
	check(t, got, []want{{"A", rrf(60, 2)}})

	for _, k := range []int{0, -1, -60} {
		if _, err := Fuse([][]Result{ids("A")}, k, 10); !errors.Is(err, ErrInvalidK) {
			t.Errorf("k=%d: err = %v, want ErrInvalidK", k, err)
		}
	}
	for _, limit := range []int{0, -1} {
		if _, err := Fuse([][]Result{ids("A")}, DefaultK, limit); !errors.Is(err, ErrInvalidLimit) {
			t.Errorf("limit=%d: err = %v, want ErrInvalidLimit", limit, err)
		}
	}
}

func TestManyResultSets(t *testing.T) {
	sets := [][]Result{ids("A", "B"), ids("B", "C"), ids("C", "A"), ids("A", "D")}
	got := mustFuse(t, sets, DefaultK, 10)
	check(t, got, []want{
		{"A", rrf(60, 1, 2, 1)},
		{"B", rrf(60, 2, 1)},
		{"C", rrf(60, 2, 1)},
		{"D", rrf(60, 2)},
	})
}

func TestPayloadIsFirstNonNilInListOrder(t *testing.T) {
	keyword := []Result{{ID: "A", Payload: "from keyword"}, {ID: "B"}}
	vector := []Result{{ID: "B", Payload: "from vector"}, {ID: "A", Payload: "ignored"}}

	got := mustFuse(t, [][]Result{keyword, vector}, DefaultK, 10)
	payloads := map[string]any{}
	for _, r := range got {
		payloads[r.ID] = r.Payload
	}
	if payloads["A"] != "from keyword" {
		t.Errorf("A payload = %v, want the first list's payload", payloads["A"])
	}
	if payloads["B"] != "from vector" {
		t.Errorf("B payload = %v, want the first non-nil payload", payloads["B"])
	}
}

func TestFuseDoesNotModifyInput(t *testing.T) {
	sets := [][]Result{ids("C", "B", "A"), ids("A", "C")}
	before := [][]Result{slices.Clone(sets[0]), slices.Clone(sets[1])}

	mustFuse(t, sets, DefaultK, 1)
	for i := range sets {
		if !slices.Equal(sets[i], before[i]) {
			t.Errorf("list %d changed: %v, was %v", i, sets[i], before[i])
		}
	}
}

func TestLargeKDoesNotOverflow(t *testing.T) {
	got := mustFuse(t, [][]Result{ids("A", "B")}, math.MaxInt, 10)
	if got[0].Score <= 0 || got[0].Score < got[1].Score {
		t.Errorf("scores = %v, want small positive scores in rank order", got)
	}
}

func BenchmarkFuseTwoLists(b *testing.B) {
	keyword := make([]Result, 100)
	vector := make([]Result, 100)
	for i := range 100 {
		keyword[i] = Result{ID: fmt.Sprintf("doc-%d", i)}
		vector[i] = Result{ID: fmt.Sprintf("doc-%d", (i*7)%150)}
	}
	sets := [][]Result{keyword, vector}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := Fuse(sets, DefaultK, 10); err != nil {
			b.Fatal(err)
		}
	}
}
