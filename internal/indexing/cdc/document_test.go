package cdc

import (
	"errors"
	"testing"
	"time"

	"near-real-time-hybrid-search-engine/internal/search/opensearch"
)

// parse is a test helper that fails the test rather than returning an error.
func parse(t *testing.T, value string) ChangeEvent {
	t.Helper()
	ev, err := ParseChangeEvent([]byte(value))
	if err != nil {
		t.Fatalf("ParseChangeEvent: %v", err)
	}
	return ev
}

func TestFieldMappingDocument(t *testing.T) {
	ev := parse(t, debeziumEvent("c", "null", `{
	  "id": "`+testDocumentID+`",
	  "title": "Indexing with Debezium",
	  "body": "Change events keep the search indexes current.",
	  "version": 9,
	  "updated_at": "2024-05-01T10:00:00Z"
	}`))

	doc, err := DefaultMapping().Document(ev)
	if err != nil {
		t.Fatalf("Document: %v", err)
	}

	want := opensearch.Document{
		ID:        testDocumentID,
		Title:     "Indexing with Debezium",
		Content:   "Change events keep the search indexes current.",
		Version:   9,
		UpdatedAt: time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC),
	}
	if doc.ID != want.ID || doc.Title != want.Title || doc.Content != want.Content {
		t.Errorf("document = %+v, want %+v", doc, want)
	}
	if doc.Version != want.Version {
		t.Errorf("Version = %d, want %d; without it OpenSearch cannot reject stale writes", doc.Version, want.Version)
	}
	if !doc.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("UpdatedAt = %s, want %s", doc.UpdatedAt, want.UpdatedAt)
	}
}

// The document ID always comes from the event, never from the row, so that a
// row cannot rename its own document.
func TestFieldMappingDocumentTakesTheIDFromTheEvent(t *testing.T) {
	ev := parse(t, debeziumEvent("c", "null", documentRow(testDocumentID, "T", "B", 1)))
	ev.ID = "chosen-by-the-parser"

	doc, err := DefaultMapping().Document(ev)
	if err != nil {
		t.Fatalf("Document: %v", err)
	}
	if doc.ID != "chosen-by-the-parser" {
		t.Fatalf("ID = %q, want the event ID", doc.ID)
	}
}

// The mapping exists so that a second table with different column names does
// not need a second parser or a second document type.
func TestFieldMappingIsNotTiedToOneTable(t *testing.T) {
	ev := parse(t, debeziumEvent("c", "null", `{
	  "id": "a-1",
	  "headline": "Another table",
	  "article": "With entirely different column names.",
	  "permalink": "https://example.test/a-1",
	  "revision": 3
	}`))

	mapping := FieldMapping{Title: "headline", Content: "article", URL: "permalink", Version: "revision"}
	doc, err := mapping.Document(ev)
	if err != nil {
		t.Fatalf("Document: %v", err)
	}
	if doc.Title != "Another table" || doc.Content != "With entirely different column names." {
		t.Errorf("document = %+v, want the mapped columns", doc)
	}
	if doc.URL != "https://example.test/a-1" || doc.Version != 3 {
		t.Errorf("URL/Version = %q/%d, want the mapped values", doc.URL, doc.Version)
	}
}

func TestFieldMappingHandlesMissingAndNullColumns(t *testing.T) {
	ev := parse(t, debeziumEvent("c", "null", `{"id": "`+testDocumentID+`", "title": null, "body": "Only a body."}`))

	doc, err := DefaultMapping().Document(ev)
	if err != nil {
		t.Fatalf("Document: %v", err)
	}
	if doc.Title != "" {
		t.Errorf("Title = %q, want empty for a NULL column", doc.Title)
	}
	if doc.Content != "Only a body." {
		t.Errorf("Content = %q, want the body", doc.Content)
	}
	// No url or version column at all: absent is not an error.
	if doc.URL != "" || doc.Version != 0 {
		t.Errorf("URL/Version = %q/%d, want empty and zero", doc.URL, doc.Version)
	}
	if !doc.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt = %s, want the zero time", doc.UpdatedAt)
	}
}

func TestFieldMappingRejectsUnreadableColumns(t *testing.T) {
	tests := []struct {
		name string
		row  string
	}{
		{"title is an object", `{"id": "x", "title": {"en": "t"}, "body": "b"}`},
		{"body is an array", `{"id": "x", "title": "t", "body": ["a"]}`},
		{"version is text", `{"id": "x", "title": "t", "body": "b", "version": "nine"}`},
		{"version is fractional", `{"id": "x", "title": "t", "body": "b", "version": 1.5}`},
		{"updated_at is unparseable", `{"id": "x", "title": "t", "body": "b", "updated_at": "last tuesday"}`},
		{"updated_at is a boolean", `{"id": "x", "title": "t", "body": "b", "updated_at": true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := parse(t, debeziumEvent("c", "null", tt.row))
			// A column the mapping cannot read will not read any better on a
			// retry, so it must be reported as malformed and dead-lettered.
			if _, err := DefaultMapping().Document(ev); !errors.Is(err, ErrMalformedEvent) {
				t.Fatalf("err = %v, want ErrMalformedEvent", err)
			}
		})
	}
}

// Debezium renders a TIMESTAMPTZ column as an ISO-8601 string and a TIMESTAMP
// column as microseconds since the epoch. Both have to work.
func TestFieldMappingUpdatedAtFormats(t *testing.T) {
	want := time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		row  string
		want time.Time
	}{
		{"iso-8601 utc", `{"id": "x", "body": "b", "updated_at": "2024-05-01T10:00:00Z"}`, want},
		{"iso-8601 with offset", `{"id": "x", "body": "b", "updated_at": "2024-05-01T12:00:00+02:00"}`, want},
		{"iso-8601 with nanoseconds", `{"id": "x", "body": "b", "updated_at": "2024-05-01T10:00:00.000000Z"}`, want},
		{"microseconds since epoch", `{"id": "x", "body": "b", "updated_at": 1714557600000000}`, want},
		{"empty string", `{"id": "x", "body": "b", "updated_at": ""}`, time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := parse(t, debeziumEvent("c", "null", tt.row))
			doc, err := DefaultMapping().Document(ev)
			if err != nil {
				t.Fatalf("Document: %v", err)
			}
			if !doc.UpdatedAt.Equal(tt.want) {
				t.Fatalf("UpdatedAt = %s, want %s", doc.UpdatedAt, tt.want)
			}
		})
	}
}

func TestBuildEmbeddingText(t *testing.T) {
	tests := []struct {
		name string
		doc  opensearch.Document
		want string
	}{
		{"title and content", opensearch.Document{Title: "Go", Content: "A language."}, "Go\nA language."},
		{"content only", opensearch.Document{Content: "A language."}, "A language."},
		{"title only", opensearch.Document{Title: "Go"}, "Go"},
		{"surrounding whitespace is dropped", opensearch.Document{Title: "  Go \n", Content: "\tA language.  "}, "Go\nA language."},
		{"whitespace only", opensearch.Document{Title: "   ", Content: "\n\t"}, ""},
		{"nothing to embed", opensearch.Document{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BuildEmbeddingText(tt.doc); got != tt.want {
				t.Fatalf("BuildEmbeddingText = %q, want %q", got, tt.want)
			}
		})
	}
}

// The same document must always produce the same text, or a replayed event
// would produce a different vector for a document that did not change.
func TestBuildEmbeddingTextIsDeterministic(t *testing.T) {
	doc := opensearch.Document{
		ID:      testDocumentID,
		Title:   "Indexing with Debezium",
		Content: "Change events keep the search indexes current.",
		URL:     "https://example.test/doc",
		Version: 4,
	}

	first := BuildEmbeddingText(doc)
	for range 100 {
		if got := BuildEmbeddingText(doc); got != first {
			t.Fatalf("BuildEmbeddingText returned %q then %q", first, got)
		}
	}

	// Only the embedded fields may change the text. A new version or URL for
	// the same words must not produce a different vector.
	unrelated := doc
	unrelated.Version = 99
	unrelated.URL = "https://example.test/moved"
	unrelated.ID = "another-id"
	if got := BuildEmbeddingText(unrelated); got != first {
		t.Fatalf("BuildEmbeddingText = %q, want %q; only title and content may affect it", got, first)
	}
}

// The vector payload carries what a result needs to render and nothing more:
// the body would make every search response larger for no gain.
func TestVectorPayloadLeavesOutTheContent(t *testing.T) {
	doc := opensearch.Document{
		ID:      testDocumentID,
		Title:   "Indexing with Debezium",
		Content: "A long body that has no business being in a search response.",
		URL:     "https://example.test/doc",
		Version: 4,
	}

	payload := DefaultMapping().VectorPayload(doc)
	if payload["title"] != doc.Title || payload["version"] != doc.Version || payload["url"] != doc.URL {
		t.Fatalf("payload = %v, want title, version and url", payload)
	}
	for key, value := range payload {
		if s, ok := value.(string); ok && s == doc.Content {
			t.Fatalf("payload key %q holds the document content", key)
		}
	}

	// A document with no URL does not get an empty one.
	if _, ok := DefaultMapping().VectorPayload(opensearch.Document{Title: "T"})["url"]; ok {
		t.Error("payload has a url key for a document without a URL")
	}
}
