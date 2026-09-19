package qdrant

import (
	"context"
	"errors"
	"fmt"

	qd "github.com/qdrant/go-client/qdrant"
)

// distance is the similarity metric for the collection. Cosine compares only
// the direction of two vectors, which is what carries meaning in text
// embeddings. To change it, change this value and recreate the collection.
const distance = qd.Distance_Cosine

// EnsureCollection creates the collection if it does not exist, sized for the
// configured vector dimension.
//
// An existing collection is never changed or recreated. It is only checked:
// if its vector size or distance differs from the configuration, every write
// would fail, so EnsureCollection returns an error at startup instead.
func (c *Client) EnsureCollection(ctx context.Context) error {
	exists, err := c.api.CollectionExists(ctx, c.collection)
	if err != nil {
		return fmt.Errorf("check qdrant collection %s: %w", c.collection, newRequestError(err))
	}

	if !exists {
		createErr := c.api.CreateCollection(ctx, &qd.CreateCollection{
			CollectionName: c.collection,
			VectorsConfig: qd.NewVectorsConfig(&qd.VectorParams{
				Size:     uint64(c.vectorSize),
				Distance: distance,
			}),
		})
		if createErr == nil {
			return nil
		}
		// Another instance may have created it between the two calls; if so,
		// check that one like any other existing collection.
		if exists, err := c.api.CollectionExists(ctx, c.collection); err != nil || !exists {
			return fmt.Errorf("create qdrant collection %s: %w", c.collection, newRequestError(createErr))
		}
	}

	info, err := c.api.GetCollectionInfo(ctx, c.collection)
	if err != nil {
		return fmt.Errorf("read qdrant collection %s: %w", c.collection, newRequestError(err))
	}
	if err := checkVectorParams(info, c.vectorSize); err != nil {
		return fmt.Errorf("qdrant collection %s: %w", c.collection, err)
	}
	return nil
}

// checkVectorParams confirms that a collection stores a single unnamed vector
// of the given size, compared with the configured distance.
func checkVectorParams(info *qd.CollectionInfo, size int) error {
	params := info.GetConfig().GetParams().GetVectorsConfig().GetParams()
	switch {
	case params == nil:
		return errors.New("collection uses named vectors; this client needs one unnamed vector per point")
	case params.GetSize() != uint64(size):
		return fmt.Errorf("stores %d-dimension vectors but the configured vector size is %d; "+
			"use a new collection name or re-create it after changing embedding models", params.GetSize(), size)
	case params.GetDistance() != distance:
		return fmt.Errorf("uses %s distance, want %s", params.GetDistance(), distance)
	}
	return nil
}
