// Package postgres provides PostgreSQL access for documents.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a document does not exist.
var ErrNotFound = errors.New("document not found")

// NewPool creates a connection pool and verifies the database is reachable.
// The caller owns the pool and must call Close.
func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// Document is the source-of-truth copy of a searchable document.
type Document struct {
	ID        string
	Title     string
	Body      string
	Version   int64
	UpdatedAt time.Time
}

// Repository reads and writes documents in PostgreSQL.
// It does not own the pool.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository returns a Repository backed by pool.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// GetDocument returns the current version of a document, or ErrNotFound.
func (r *Repository) GetDocument(ctx context.Context, id string) (Document, error) {
	const query = `SELECT id::text, title, body, version, updated_at FROM documents WHERE id = $1`

	var d Document
	err := r.pool.QueryRow(ctx, query, id).Scan(&d.ID, &d.Title, &d.Body, &d.Version, &d.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Document{}, ErrNotFound
	}
	if err != nil {
		return Document{}, fmt.Errorf("get document %s: %w", id, err)
	}
	return d, nil
}
