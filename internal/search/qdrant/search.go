package qdrant

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	qd "github.com/qdrant/go-client/qdrant"
)

// MaxSearchLimit is the most results one Search call can return.
const MaxSearchLimit = 100

// ErrInvalidLimit means the limit was outside 1..MaxSearchLimit.
var ErrInvalidLimit = errors.New("search limit out of range")

// SearchResult is one vector match.
//
// Search returns results best first, so a result's position in the slice is
// its rank. ID is the document ID given to Upsert, the same ID OpenSearch
// uses, so results from both can be matched up.
type SearchResult struct {
	ID string
	// Score is the cosine similarity between the query and the stored vector:
	// 1 means the same direction, 0 unrelated, -1 opposite.
	Score   float64
	Payload map[string]any
}

// Search returns up to limit points whose vectors are most similar to vector,
// best first.
func (c *Client) Search(ctx context.Context, vector []float32, limit int) ([]SearchResult, error) {
	if err := c.validateVector(vector); err != nil {
		return nil, err
	}
	if limit < 1 || limit > MaxSearchLimit {
		return nil, fmt.Errorf("%w: got %d, want 1 to %d", ErrInvalidLimit, limit, MaxSearchLimit)
	}

	points, err := c.api.Query(ctx, &qd.QueryPoints{
		CollectionName: c.collection,
		Query:          qd.NewQueryDense(vector),
		Limit:          qd.PtrOf(uint64(limit)),
		WithPayload:    qd.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("search qdrant: %w", newRequestError(err))
	}
	return toResults(points)
}

// toResults converts scored points into SearchResults, keeping their order.
func toResults(points []*qd.ScoredPoint) ([]SearchResult, error) {
	results := make([]SearchResult, 0, len(points))
	for _, p := range points {
		payload, err := fromValueMap(p.GetPayload())
		if err != nil {
			return nil, fmt.Errorf("decode payload of point %s: %w", formatPointID(p.GetId()), err)
		}
		id, _ := payload[documentIDKey].(string)
		if id == "" {
			id = formatPointID(p.GetId())
		}
		results = append(results, SearchResult{ID: id, Score: float64(p.GetScore()), Payload: payload})
	}
	return results, nil
}

func formatPointID(id *qd.PointId) string {
	if u := id.GetUuid(); u != "" {
		return u
	}
	return strconv.FormatUint(id.GetNum(), 10)
}

// fromValueMap converts a Qdrant payload into plain Go values: strings,
// float64, int64, bool, nil, []any and map[string]any.
func fromValueMap(m map[string]*qd.Value) (map[string]any, error) {
	out := make(map[string]any, len(m))
	for k, v := range m {
		converted, err := fromValue(v)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", k, err)
		}
		out[k] = converted
	}
	return out, nil
}

func fromValue(v *qd.Value) (any, error) {
	switch kind := v.GetKind().(type) {
	case *qd.Value_NullValue:
		return nil, nil
	case *qd.Value_BoolValue:
		return kind.BoolValue, nil
	case *qd.Value_IntegerValue:
		return kind.IntegerValue, nil
	case *qd.Value_DoubleValue:
		return kind.DoubleValue, nil
	case *qd.Value_StringValue:
		return kind.StringValue, nil
	case *qd.Value_StructValue:
		return fromValueMap(kind.StructValue.GetFields())
	case *qd.Value_ListValue:
		items := kind.ListValue.GetValues()
		list := make([]any, len(items))
		for i, item := range items {
			converted, err := fromValue(item)
			if err != nil {
				return nil, fmt.Errorf("item %d: %w", i, err)
			}
			list[i] = converted
		}
		return list, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", kind)
	}
}
