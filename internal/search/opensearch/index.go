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

// IndexDocument creates or replaces the document stored under doc.ID.
//
// The OpenSearch _id is doc.ID itself, so indexing the same document again
// replaces it rather than adding a copy. When doc.Version is positive, a write
// with an older or equal version is ignored and IndexDocument returns nil.
func (c *Client) IndexDocument(ctx context.Context, doc Document) error {
	if err := validateID(doc.ID); err != nil {
		return err
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("%w: encode %s: %w", ErrInvalidDocument, doc.ID, err)
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
		return nil
	}
	code := statusCode(resp.Inspect().Response)
	if doc.Version > 0 && code == http.StatusConflict {
		return nil // the index already holds this version or a newer one
	}
	return fmt.Errorf("index document %s: %w", doc.ID, &RequestError{StatusCode: code, Err: err})
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
