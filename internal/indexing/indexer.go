package indexing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"near-real-time-hybrid-search-engine/internal/embedding"
	"near-real-time-hybrid-search-engine/internal/postgres"
)

// Indexer writes the current state of a document to OpenSearch and Qdrant.
// It does not own its clients. It is safe for concurrent use.
//
// Idempotency:
//   - The document is always read from PostgreSQL, so replayed or out-of-order
//     events index the latest data rather than the data at event time.
//   - OpenSearch writes use the row version as an external version. A write
//     with an older or equal version is rejected with 409 and treated as done.
//   - Qdrant points use the document UUID as their ID, so an upsert replaces
//     the previous point instead of adding a duplicate.
//   - A document missing from PostgreSQL is deleted from both stores, and
//     deleting something already gone succeeds.
type Indexer struct {
	documents  *postgres.Repository
	embedder   *embedding.Client
	opensearch *opensearchapi.Client
	qdrant     *qdrant.Client
	index      string
	collection string
}

// NewIndexer returns an Indexer that writes to the given index and collection.
func NewIndexer(
	documents *postgres.Repository,
	embedder *embedding.Client,
	osClient *opensearchapi.Client,
	qdClient *qdrant.Client,
	index, collection string,
) *Indexer {
	return &Indexer{
		documents:  documents,
		embedder:   embedder,
		opensearch: osClient,
		qdrant:     qdClient,
		index:      index,
		collection: collection,
	}
}

// Index brings both stores in line with the document named by ev.
// Errors that retrying cannot fix are marked permanent.
func (ix *Indexer) Index(ctx context.Context, ev Event) error {
	doc, err := ix.documents.GetDocument(ctx, ev.DocumentID)
	if errors.Is(err, postgres.ErrNotFound) {
		return ix.delete(ctx, ev.DocumentID)
	}
	if err != nil {
		return fmt.Errorf("load document: %w", err)
	}

	// Embed first so a Gemini failure leaves both stores untouched.
	vector, err := ix.embedder.EmbedDocument(ctx, doc.Title, doc.Body)
	if err != nil {
		var apiErr *embedding.APIError
		if errors.As(err, &apiErr) && !apiErr.Temporary() {
			err = permanent(err)
		}
		return fmt.Errorf("embed document: %w", err)
	}

	if err := ix.upsertOpenSearch(ctx, doc); err != nil {
		return fmt.Errorf("index in opensearch: %w", err)
	}
	if err := ix.upsertQdrant(ctx, doc, vector); err != nil {
		return fmt.Errorf("index in qdrant: %w", err)
	}
	return nil
}

func (ix *Indexer) upsertOpenSearch(ctx context.Context, doc postgres.Document) error {
	body, err := json.Marshal(map[string]any{
		"document_id": doc.ID,
		"title":       doc.Title,
		"body":        doc.Body,
		"version":     doc.Version,
		"updated_at":  doc.UpdatedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return permanent(fmt.Errorf("encode document: %w", err))
	}

	version := int(doc.Version)
	resp, err := ix.opensearch.Index(ctx, opensearchapi.IndexReq{
		Index:      ix.index,
		DocumentID: doc.ID,
		Body:       bytes.NewReader(body),
		Params:     opensearchapi.IndexParams{Version: &version, VersionType: "external"},
	})
	if err == nil {
		return nil
	}
	code := statusCode(resp.Inspect().Response)
	if code == http.StatusConflict {
		return nil // the index already holds this version or a newer one
	}
	return classifyStatus(code, err)
}

func (ix *Indexer) upsertQdrant(ctx context.Context, doc postgres.Document, vector []float32) error {
	payload, err := qdrant.TryValueMap(map[string]any{
		"document_id": doc.ID,
		"title":       doc.Title,
		"version":     doc.Version,
	})
	if err != nil {
		return permanent(fmt.Errorf("build payload: %w", err))
	}

	_, err = ix.qdrant.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: ix.collection,
		Wait:           qdrant.PtrOf(true),
		Points: []*qdrant.PointStruct{{
			Id:      qdrant.NewID(doc.ID),
			Vectors: qdrant.NewVectorsDense(vector),
			Payload: payload,
		}},
	})
	return classifyQdrant(err)
}

func (ix *Indexer) delete(ctx context.Context, id string) error {
	resp, err := ix.opensearch.Document.Delete(ctx, opensearchapi.DocumentDeleteReq{Index: ix.index, DocumentID: id})
	if err != nil {
		// Not found means it is already gone.
		if code := statusCode(resp.Inspect().Response); code != http.StatusNotFound {
			return fmt.Errorf("delete from opensearch: %w", classifyStatus(code, err))
		}
	}

	_, err = ix.qdrant.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: ix.collection,
		Wait:           qdrant.PtrOf(true),
		Points:         qdrant.NewPointsSelector(qdrant.NewID(id)),
	})
	if err != nil {
		return fmt.Errorf("delete from qdrant: %w", classifyQdrant(err))
	}
	return nil
}

// EnsureStores creates the OpenSearch index and Qdrant collection if they do
// not exist. Call it once at startup.
func (ix *Indexer) EnsureStores(ctx context.Context, dimensions int) error {
	if err := ix.ensureIndex(ctx); err != nil {
		return fmt.Errorf("ensure opensearch index %q: %w", ix.index, err)
	}
	if err := ix.ensureCollection(ctx, dimensions); err != nil {
		return fmt.Errorf("ensure qdrant collection %q: %w", ix.collection, err)
	}
	return nil
}

const indexMapping = `{
  "mappings": {
    "properties": {
      "document_id": {"type": "keyword"},
      "title":       {"type": "text"},
      "body":        {"type": "text"},
      "version":     {"type": "long"},
      "updated_at":  {"type": "date"}
    }
  }
}`

func (ix *Indexer) ensureIndex(ctx context.Context) error {
	exists := opensearchapi.IndicesExistsReq{Indices: []string{ix.index}}
	resp, err := ix.opensearch.Indices.Exists(ctx, exists)
	if err == nil {
		return nil
	}
	if statusCode(resp) != http.StatusNotFound {
		return err
	}

	_, err = ix.opensearch.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: ix.index,
		Body:  strings.NewReader(indexMapping),
	})
	if err != nil {
		// Another consumer instance may have created it first.
		if _, existsErr := ix.opensearch.Indices.Exists(ctx, exists); existsErr == nil {
			return nil
		}
		return err
	}
	return nil
}

func (ix *Indexer) ensureCollection(ctx context.Context, dimensions int) error {
	exists, err := ix.qdrant.CollectionExists(ctx, ix.collection)
	if err != nil || exists {
		return err
	}
	err = ix.qdrant.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: ix.collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(dimensions),
			Distance: qdrant.Distance_Cosine,
		}),
	})
	if err != nil {
		// Another consumer instance may have created it first.
		if exists, existsErr := ix.qdrant.CollectionExists(ctx, ix.collection); existsErr == nil && exists {
			return nil
		}
		return err
	}
	return nil
}

// statusCode returns the HTTP status of an OpenSearch response, or 0 when no
// response was received (for example, a network error).
func statusCode(resp *opensearch.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

// classifyStatus marks OpenSearch client errors as permanent. Timeouts, rate
// limits, server errors and network errors stay retryable.
func classifyStatus(code int, err error) error {
	if code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests {
		return permanent(err)
	}
	return err
}

func classifyQdrant(err error) error {
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition, codes.OutOfRange,
		codes.Unimplemented, codes.PermissionDenied, codes.Unauthenticated:
		return permanent(err)
	default:
		return err
	}
}
