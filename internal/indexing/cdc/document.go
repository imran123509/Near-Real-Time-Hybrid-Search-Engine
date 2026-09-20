package cdc

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"near-real-time-hybrid-search-engine/internal/search/opensearch"
)

// The search document is opensearch.Document, the model the keyword index
// already defines. Nothing here declares a second document type: a raw CDC row
// is converted once, at the edge of the service, and only the converted value
// travels on to OpenSearch and Qdrant.

// FieldMapping names the row columns that make up a search document, so that
// the conversion is not tied to one table's column names. A column left empty
// is simply not read.
type FieldMapping struct {
	Title   string // column holding the document title
	Content string // column holding the body text that is searched and embedded
	URL     string // column holding a link to the document, if any

	// Version is a column that increases on every change to the row. It is
	// passed to OpenSearch as an external version, which is what stops a
	// replayed or out-of-order event from overwriting newer indexed data.
	// Leave it empty for last-write-wins.
	Version string

	// UpdatedAt is a column holding the time of the last change. Debezium
	// renders a TIMESTAMPTZ column as an ISO-8601 string and a TIMESTAMP
	// column as microseconds since the epoch; both are accepted.
	UpdatedAt string
}

// DefaultMapping matches the documents table created by the migrations.
func DefaultMapping() FieldMapping {
	return FieldMapping{
		Title:     "title",
		Content:   "body",
		URL:       "url",
		Version:   "version",
		UpdatedAt: "updated_at",
	}
}

// Document converts a change event into the document stored in the search
// indexes. Failures wrap ErrMalformedEvent: a row whose columns hold types the
// mapping cannot read will not read any better on a retry.
//
// The ID comes from the event, never from the row body, so a document keeps
// the same identity in both stores across its whole life.
func (m FieldMapping) Document(ev ChangeEvent) (opensearch.Document, error) {
	doc := opensearch.Document{ID: ev.ID}

	var err error
	if doc.Title, err = m.text(ev.Row, m.Title); err != nil {
		return opensearch.Document{}, err
	}
	if doc.Content, err = m.text(ev.Row, m.Content); err != nil {
		return opensearch.Document{}, err
	}
	if doc.URL, err = m.text(ev.Row, m.URL); err != nil {
		return opensearch.Document{}, err
	}
	if doc.Version, err = m.version(ev.Row); err != nil {
		return opensearch.Document{}, err
	}
	if doc.UpdatedAt, err = m.updatedAt(ev.Row); err != nil {
		return opensearch.Document{}, err
	}
	return doc, nil
}

// VectorPayload is the metadata stored beside a document's vector in Qdrant.
// It holds only what a search result needs to render, never the document body:
// retrieval does not need it and it would make every response larger.
func (m FieldMapping) VectorPayload(doc opensearch.Document) map[string]any {
	payload := map[string]any{"title": doc.Title, "version": doc.Version}
	if doc.URL != "" {
		payload["url"] = doc.URL
	}
	return payload
}

// BuildEmbeddingText returns the text that represents doc to an embedding
// model: the title and the content, separated by a newline, with either part
// dropped when it is empty.
//
// It is deliberately a plain function of the document and nothing else. The
// same document always produces the same text, so re-processing an event
// produces the same vector, and the rule can be changed and reasoned about
// without touching any embedding provider. It is the only place that decides
// what gets embedded:
//
//	opensearch.Document -> BuildEmbeddingText -> Embedder -> []float32 -> Qdrant
//
// Changing this rule changes every future vector, so existing documents have
// to be reindexed for old and new vectors to stay comparable.
func BuildEmbeddingText(doc opensearch.Document) string {
	title := strings.TrimSpace(doc.Title)
	content := strings.TrimSpace(doc.Content)
	switch {
	case title == "":
		return content
	case content == "":
		return title
	default:
		return title + "\n" + content
	}
}

// text reads a string column. A missing column or a SQL NULL is an empty
// string, because not every table fills every mapped field.
func (m FieldMapping) text(row map[string]any, column string) (string, error) {
	if column == "" {
		return "", nil
	}
	switch v := row[column].(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	default:
		return "", fmt.Errorf("%w: column %q holds %T, want text", ErrMalformedEvent, column, v)
	}
}

// version reads the row version. Zero means the column is absent or NULL, in
// which case OpenSearch falls back to last-write-wins.
func (m FieldMapping) version(row map[string]any) (int64, error) {
	if m.Version == "" {
		return 0, nil
	}
	switch v := row[m.Version].(type) {
	case nil:
		return 0, nil
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("%w: column %q: %w", ErrMalformedEvent, m.Version, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("%w: column %q holds %T, want a number", ErrMalformedEvent, m.Version, v)
	}
}

// updatedAt reads the last-changed time in either of the two shapes Debezium
// produces for a timestamp column.
func (m FieldMapping) updatedAt(row map[string]any) (time.Time, error) {
	if m.UpdatedAt == "" {
		return time.Time{}, nil
	}
	switch v := row[m.UpdatedAt].(type) {
	case nil:
		return time.Time{}, nil
	case string:
		if strings.TrimSpace(v) == "" {
			return time.Time{}, nil
		}
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: column %q: %w", ErrMalformedEvent, m.UpdatedAt, err)
		}
		return t.UTC(), nil
	case json.Number:
		micros, err := v.Int64()
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: column %q: %w", ErrMalformedEvent, m.UpdatedAt, err)
		}
		return time.UnixMicro(micros).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("%w: column %q holds %T, want a timestamp", ErrMalformedEvent, m.UpdatedAt, v)
	}
}
