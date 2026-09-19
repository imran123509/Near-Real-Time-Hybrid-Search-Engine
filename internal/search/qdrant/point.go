package qdrant

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
	qd "github.com/qdrant/go-client/qdrant"
)

var (
	// ErrInvalidPoint means a point or ID was rejected before any request was
	// sent. Retrying will not help.
	ErrInvalidPoint = errors.New("invalid point")
	// ErrInvalidVector means a vector has the wrong dimension or unusable
	// values. Retrying will not help.
	ErrInvalidVector = errors.New("invalid vector")
)

// documentIDKey is the payload field holding the caller's document ID. Upsert
// sets it from Point.ID, and Search reads it back as SearchResult.ID.
const documentIDKey = "document_id"

// idNamespace roots the name-based UUIDs derived from IDs that are not UUIDs.
// Never change it: every derived point ID would change with it.
var idNamespace = uuid.MustParse("3f0f4a57-6d2e-4a8c-9c52-7a1e0b8d2c41")

// Point is one document's vector and the metadata returned with it.
type Point struct {
	// ID is the document ID. The same ID always maps to the same Qdrant point,
	// so writing a document again replaces its point.
	ID     string
	Vector []float32
	// Payload holds small metadata to return with search results, such as a
	// title or URL. Keep document text out of it: retrieval does not need it
	// and it would make every search response larger. The "document_id" key is
	// reserved; Upsert sets it from ID.
	Payload map[string]any
}

// Upsert creates the point for p.ID or replaces the existing one. It returns
// after Qdrant has applied the write.
func (c *Client) Upsert(ctx context.Context, p Point) error {
	if err := validateID(p.ID); err != nil {
		return err
	}
	if err := c.validateVector(p.Vector); err != nil {
		return fmt.Errorf("upsert qdrant point %s: %w", p.ID, err)
	}
	point, err := toPointStruct(p)
	if err != nil {
		return fmt.Errorf("upsert qdrant point %s: %w", p.ID, err)
	}

	_, err = c.api.Upsert(ctx, &qd.UpsertPoints{
		CollectionName: c.collection,
		Wait:           qd.PtrOf(true),
		Points:         []*qd.PointStruct{point},
	})
	if err != nil {
		return fmt.Errorf("upsert qdrant point %s: %w", p.ID, newRequestError(err))
	}
	return nil
}

// Delete removes the point for id. Deleting a point that does not exist
// succeeds, so a repeated delete is harmless.
func (c *Client) Delete(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	_, err := c.api.Delete(ctx, &qd.DeletePoints{
		CollectionName: c.collection,
		Wait:           qd.PtrOf(true),
		Points:         qd.NewPointsSelector(pointID(id)),
	})
	if err != nil {
		return fmt.Errorf("delete qdrant point %s: %w", id, newRequestError(err))
	}
	return nil
}

// pointID maps a document ID to its Qdrant point ID. Qdrant accepts only UUIDs
// and unsigned integers as IDs, so a canonical UUID is used unchanged and any
// other string becomes a name-based (version 5) UUID, which is always the same
// for the same input.
func pointID(id string) *qd.PointId {
	if u, err := uuid.Parse(id); err == nil && u.String() == id {
		return qd.NewIDUUID(id)
	}
	return qd.NewIDUUID(uuid.NewSHA1(idNamespace, []byte(id)).String())
}

// toPointStruct converts p to the SDK type without modifying p.Payload.
func toPointStruct(p Point) (*qd.PointStruct, error) {
	payload := make(map[string]any, len(p.Payload)+1)
	for k, v := range p.Payload {
		payload[k] = v
	}
	payload[documentIDKey] = p.ID

	values, err := qd.TryValueMap(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: payload: %w", ErrInvalidPoint, err)
	}
	return &qd.PointStruct{
		Id:      pointID(p.ID),
		Vectors: qd.NewVectorsDense(p.Vector),
		Payload: values,
	}, nil
}

func validateID(id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("%w: id is required", ErrInvalidPoint)
	}
	return nil
}

// validateVector rejects vectors Qdrant would refuse or could not compare.
// Vectors are never resized or truncated to fit.
func (c *Client) validateVector(v []float32) error {
	if len(v) != c.vectorSize {
		return fmt.Errorf("%w: expected vector dimension %d, got %d", ErrInvalidVector, c.vectorSize, len(v))
	}
	allZero := true
	for i, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return fmt.Errorf("%w: value at index %d is %v", ErrInvalidVector, i, x)
		}
		if x != 0 {
			allZero = false
		}
	}
	if allZero {
		return fmt.Errorf("%w: all values are zero, so it has no direction to compare", ErrInvalidVector)
	}
	return nil
}
