package loadtest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Writer inserts generated documents into the documents table, which is where
// the pipeline being measured starts.
//
// Rows are written in batches because one statement per row would measure the
// benchmark's own round trips rather than the pipeline. The batch size is
// configurable for the same reason it matters in production: it trades write
// throughput against how bursty the change stream becomes.
type Writer struct {
	Pool      *pgxpool.Pool
	BatchSize int
}

// upsert keeps a repeated run with the same run ID an update instead of a
// failure, so a benchmark can be run again without cleaning up first. The
// trigger from migrations/0002 assigns a new version either way, so each
// repeat produces real change events.
const upsertSuffix = ` ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, body = EXCLUDED.body, url = EXCLUDED.url`

// Insert writes every document and returns when the last batch is committed.
//
// Each returned Arrival carries the moment its batch was committed, which is
// the earliest point at which Debezium could have seen the change. Latency
// measured from it therefore includes the whole pipeline and nothing before
// it.
func (w Writer) Insert(ctx context.Context, docs []Document) ([]Arrival, time.Duration, error) {
	batchSize := w.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	arrivals := make([]Arrival, 0, len(docs))
	started := time.Now()
	for start := 0; start < len(docs); start += batchSize {
		end := min(start+batchSize, len(docs))
		batch := docs[start:end]

		if err := w.insertBatch(ctx, batch); err != nil {
			return arrivals, time.Since(started), err
		}
		committed := time.Now()
		for _, doc := range batch {
			arrivals = append(arrivals, Arrival{ID: doc.ID, Inserted: committed})
		}
	}
	return arrivals, time.Since(started), nil
}

func (w Writer) insertBatch(ctx context.Context, docs []Document) error {
	var (
		sql  strings.Builder
		args = make([]any, 0, len(docs)*4)
	)
	sql.WriteString("INSERT INTO documents (id, title, body, url) VALUES ")
	for i, doc := range docs {
		if i > 0 {
			sql.WriteString(", ")
		}
		fmt.Fprintf(&sql, "($%d, $%d, $%d, $%d)", i*4+1, i*4+2, i*4+3, i*4+4)
		args = append(args, doc.ID, doc.Title, doc.Body, doc.URL)
	}
	sql.WriteString(upsertSuffix)

	if _, err := w.Pool.Exec(ctx, sql.String(), args...); err != nil {
		return fmt.Errorf("insert %d documents: %w", len(docs), err)
	}
	return nil
}

// DeleteRun removes the rows of one benchmark run and returns how many it
// removed. Deleting them also removes them from both search indexes, through
// the same change events any other delete produces.
//
// It matches on the generated URL prefix, so it can only ever delete rows a
// benchmark created: a hand-written or seeded document has a different URL.
func DeleteRun(ctx context.Context, pool *pgxpool.Pool, runID string) (int64, error) {
	prefix := URLPrefix
	if strings.TrimSpace(runID) != "" {
		prefix += strings.TrimSpace(runID) + "/"
	}
	tag, err := pool.Exec(ctx, `DELETE FROM documents WHERE url LIKE $1`, prefix+"%")
	if err != nil {
		return 0, fmt.Errorf("delete benchmark rows: %w", err)
	}
	return tag.RowsAffected(), nil
}

// CountRun returns how many rows of a run are still in the table. An empty
// run ID counts the rows of every run.
func CountRun(ctx context.Context, pool *pgxpool.Pool, runID string) (int64, error) {
	prefix := URLPrefix
	if strings.TrimSpace(runID) != "" {
		prefix += strings.TrimSpace(runID) + "/"
	}
	var count int64
	err := pool.QueryRow(ctx, `SELECT count(*) FROM documents WHERE url LIKE $1`, prefix+"%").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count benchmark rows: %w", err)
	}
	return count, nil
}

// LatestVersion returns the highest row version in the table, which is the
// sequence the trigger assigns. Compared before and after a run it says how
// many changes the database actually produced.
func LatestVersion(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var version *int64
	if err := pool.QueryRow(ctx, `SELECT max(version) FROM documents`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read the latest row version: %w", err)
	}
	if version == nil {
		return 0, nil
	}
	return *version, nil
}
