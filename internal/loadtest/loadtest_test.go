package loadtest

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// A benchmark is only comparable with itself if it generates the same data
// every time, so the generator must be a pure function of its run ID.
func TestGeneratorIsDeterministic(t *testing.T) {
	first, err := NewGenerator("bench-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewGenerator("bench-1")
	if err != nil {
		t.Fatal(err)
	}

	for n := range 50 {
		a, b := first.Document(n), second.Document(n)
		if a != b {
			t.Fatalf("document %d differs between two generators with the same run id:\n%+v\n%+v", n, a, b)
		}
	}
}

func TestGeneratorProducesDistinctDocuments(t *testing.T) {
	g, err := NewGenerator("bench-2")
	if err != nil {
		t.Fatal(err)
	}

	ids := map[string]int{}
	for n := range 200 {
		doc := g.Document(n)
		if previous, seen := ids[doc.ID]; seen {
			t.Fatalf("documents %d and %d share the id %s", previous, n, doc.ID)
		}
		ids[doc.ID] = n

		if err := uuid.Validate(doc.ID); err != nil {
			t.Fatalf("document %d has an id that is not a UUID: %v", n, err)
		}
		if !strings.HasPrefix(doc.URL, URLPrefix) {
			t.Fatalf("document %d has url %q, which cleanup would not match", n, doc.URL)
		}
		if doc.Title == "" || doc.Body == "" {
			t.Fatalf("document %d has nothing to index: %+v", n, doc)
		}
	}
}

// Two runs must not share documents, or cleaning up one would remove the
// other's rows and a comparison between them would be meaningless.
func TestRunsDoNotShareDocuments(t *testing.T) {
	first, _ := NewGenerator("bench-a")
	second, _ := NewGenerator("bench-b")

	for n := range 20 {
		a, b := first.Document(n), second.Document(n)
		if a.ID == b.ID {
			t.Fatalf("document %d has the same id in two runs: %s", n, a.ID)
		}
		if a.URL == b.URL {
			t.Fatalf("document %d has the same url in two runs: %s", n, a.URL)
		}
	}
}

// Every document carries its run's marker, which is how a search benchmark
// finds this run's documents and only this run's.
func TestDocumentsCarryTheRunMarker(t *testing.T) {
	g, _ := NewGenerator("bench-3")
	marker := g.Marker()

	if strings.ContainsAny(marker, " -") {
		t.Fatalf("marker %q is not a single word, so it would not match as one term", marker)
	}
	for n := range 10 {
		doc := g.Document(n)
		if !strings.Contains(doc.Title, marker) || !strings.Contains(doc.Body, marker) {
			t.Fatalf("document %d does not carry the marker %q: %+v", n, marker, doc)
		}
	}
}

// The generated text has to be about the things the search benchmark asks
// about, or the benchmark measures how fast the system finds nothing.
func TestDocumentsCoverEveryQueryTopic(t *testing.T) {
	g, _ := NewGenerator("bench-4")
	docs := g.Documents(len(Topics()) * 2)

	for _, topic := range Topics() {
		found := false
		for _, doc := range docs {
			if strings.Contains(strings.ToLower(doc.Title+" "+doc.Body), topic) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no generated document mentions %q", topic)
		}
	}
}

func TestNewGeneratorRequiresARunID(t *testing.T) {
	for _, runID := range []string{"", "   "} {
		if _, err := NewGenerator(runID); err == nil {
			t.Errorf("NewGenerator(%q) returned no error", runID)
		}
	}
}

func TestDocumentsCount(t *testing.T) {
	g, _ := NewGenerator("bench-5")
	for _, count := range []int{0, 1, 7} {
		if got := len(g.Documents(count)); got != count {
			t.Errorf("Documents(%d) returned %d documents", count, got)
		}
	}
	if got := len(g.Documents(-1)); got != 0 {
		t.Errorf("Documents(-1) returned %d documents, want none", got)
	}
}
