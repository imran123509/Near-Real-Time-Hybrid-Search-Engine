package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// maxIDBytes is OpenSearch's limit on the length of a document _id.
const maxIDBytes = 512

// ErrInvalidDocument means a document or ID was rejected before any request
// was sent. Retrying will not help.
var ErrInvalidDocument = errors.New("invalid document")

// Document is a searchable document as stored in the keyword index.
//
// Add a field here and to indexMapping together: the index uses a strict
// mapping, so OpenSearch rejects fields it does not know.
type Document struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Content   string    `json:"content"`
	URL       string    `json:"url,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitzero"`
	// Version makes writes conditional when it is positive: OpenSearch keeps
	// the highest version it has seen and ignores writes with an older or
	// equal one, so a replayed or out-of-order event cannot overwrite newer
	// data. Leave it zero for last-write-wins.
	Version int64 `json:"version,omitzero"`
}

// indexMapping defines how each Document field is stored.
//
// title and content are "text": analyzed into terms by the standard analyzer
// and scored with BM25, the OpenSearch default. id and url are "keyword":
// stored as one exact value, for lookups and filters rather than relevance.
// "strict" rejects unknown fields instead of guessing a type for them.
const indexMapping = `{
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id":         {"type": "keyword"},
      "title":      {"type": "text"},
      "content":    {"type": "text"},
      "url":        {"type": "keyword"},
      "updated_at": {"type": "date"},
      "version":    {"type": "long"}
    }
  }
}`

// EnsureIndex creates the index with indexMapping if it does not exist. An
// existing index is left untouched, even if its mapping differs.
func (c *Client) EnsureIndex(ctx context.Context) error {
	exists, err := c.indexExists(ctx)
	if err != nil {
		return fmt.Errorf("check index %s: %w", c.index, err)
	}
	if exists {
		return nil
	}

	resp, err := c.api.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: c.index,
		Body:  strings.NewReader(indexMapping),
	})
	if err == nil {
		return nil
	}
	// Another instance may have created the index between the two calls.
	if exists, existsErr := c.indexExists(ctx); existsErr == nil && exists {
		return nil
	}
	return fmt.Errorf("create index %s: %w", c.index, newRequestError(resp.Inspect().Response, err))
}

func (c *Client) indexExists(ctx context.Context) (bool, error) {
	resp, err := c.api.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{c.index}})
	switch {
	case err == nil:
		return true, nil
	case statusCode(resp) == http.StatusNotFound:
		return false, nil
	default:
		return false, newRequestError(resp, err)
	}
}

// WriteResult says what an index write did, so a caller can tell a first
// write from a replay, and both from an event the index has already moved
// past. It is what makes a duplicate delivery distinguishable from stale data
// without keeping a record of processed events anywhere.
type WriteResult int

const (
	// WriteUnknown means nothing is known about the write, which is what is
	// returned with every error.
	WriteUnknown WriteResult = iota
	// WriteApplied means the index now holds this document.
	WriteApplied
	// WriteDuplicate means the index already held exactly this version, so
	// the write changed nothing. The event is not out of date: an earlier
	// attempt may have stopped right after this step, so the caller should
	// carry on with whatever else the event requires.
	WriteDuplicate
	// WriteStale means the index holds a newer version of the document. The
	// event has been overtaken, and its data must not be written to any
	// other store either.
	WriteStale
)

func (r WriteResult) String() string {
	switch r {
	case WriteApplied:
		return "applied"
	case WriteDuplicate:
		return "duplicate"
	case WriteStale:
		return "stale"
	default:
		return "unknown"
	}
}

// IndexDocument creates or replaces the document stored under doc.ID.
//
// The OpenSearch _id is doc.ID itself, so indexing the same document again
// replaces it rather than adding a copy. When doc.Version is positive the
// write is conditional: OpenSearch refuses one whose version is not higher
// than the stored version, and IndexDocument reports that as WriteDuplicate or
// WriteStale instead of an error.
func (c *Client) IndexDocument(ctx context.Context, doc Document) (WriteResult, error) {
	if err := validateID(doc.ID); err != nil {
		return WriteUnknown, err
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return WriteUnknown, fmt.Errorf("%w: encode %s: %w", ErrInvalidDocument, doc.ID, err)
	}

	req := opensearchapi.IndexReq{
		Index:      c.index,
		DocumentID: doc.ID,
		Body:       bytes.NewReader(body),
	}
	if doc.Version > 0 {
		version := int(doc.Version)
		req.Params = opensearchapi.IndexParams{Version: &version, VersionType: "external"}
	}

	resp, err := c.api.Index(ctx, req)
	if err == nil {
		return WriteApplied, nil
	}
	code := statusCode(resp.Inspect().Response)
	if doc.Version > 0 && code == http.StatusConflict {
		return c.classifyConflict(ctx, doc), nil
	}
	return WriteUnknown, fmt.Errorf("index document %s: %w", doc.ID, &RequestError{StatusCode: code, Err: err})
}

// classifyConflict works out what a refused versioned write means.
//
// OpenSearch answers 409 both when the stored version equals the one offered
// and when it is higher, and its message is the only thing that tells them
// apart. Rather than reading error text, the stored version is read back and
// compared; that costs one request, and only on the replays and late events
// that produce a conflict in the first place.
//
// When the version cannot be read, the conflict counts as a duplicate. That
// is the assumption that cannot lose data: repeating the caller's remaining
// writes is harmless because they are idempotent, while wrongly calling an
// event stale would leave the document out of the other index.
func (c *Client) classifyConflict(ctx context.Context, doc Document) WriteResult {
	version, found, err := c.DocumentVersion(ctx, doc.ID)
	switch {
	case err != nil:
		return WriteDuplicate
	case !found:
		// The document was deleted between the refused write and this read,
		// so something newer than this event has already been applied.
		return WriteStale
	case version > doc.Version:
		return WriteStale
	default:
		return WriteDuplicate
	}
}

// DocumentVersion returns the version the index holds for id, and whether the
// document exists at all. The document body is not fetched.
//
// For documents written with a positive Document.Version this is that same
// external version, so it can be compared with an incoming event's version
// directly.
func (c *Client) DocumentVersion(ctx context.Context, id string) (version int64, found bool, err error) {
	if err := validateID(id); err != nil {
		return 0, false, err
	}
	resp, err := c.api.Document.Get(ctx, opensearchapi.DocumentGetReq{
		Index:      c.index,
		DocumentID: id,
		Params:     opensearchapi.DocumentGetParams{Source: false},
	})
	if err != nil {
		code := statusCode(resp.Inspect().Response)
		if code == http.StatusNotFound {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read version of document %s: %w", id, &RequestError{StatusCode: code, Err: err})
	}
	if !resp.Found {
		return 0, false, nil
	}
	return int64(resp.Version), true, nil
}

// DeleteDocument removes the document stored under id. Deleting a document
// that does not exist succeeds, so a repeated delete is harmless.
func (c *Client) DeleteDocument(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}

	resp, err := c.api.Document.Delete(ctx, opensearchapi.DocumentDeleteReq{Index: c.index, DocumentID: id})
	if err == nil {
		return nil
	}
	code := statusCode(resp.Inspect().Response)
	if code == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("delete document %s: %w", id, &RequestError{StatusCode: code, Err: err})
}

func validateID(id string) error {
	switch {
	case strings.TrimSpace(id) == "":
		return fmt.Errorf("%w: id is required", ErrInvalidDocument)
	case len(id) > maxIDBytes:
		return fmt.Errorf("%w: id is %d bytes, maximum is %d", ErrInvalidDocument, len(id), maxIDBytes)
	}
	return nil
}
