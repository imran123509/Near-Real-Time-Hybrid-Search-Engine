package loadtest

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Options configures one benchmark run. Every value has a default, and none
// of them is large: a benchmark that fills the disk on its first run is not
// one anybody repeats.
type Options struct {
	// RunID separates this run's documents from every other run's and makes
	// it reproducible.
	RunID string
	// Count is how many documents the run produces.
	Count int
	// BatchSize is how many rows go into PostgreSQL per statement.
	BatchSize int
	// WarmupDocs are indexed and waited for before the measurement starts, so
	// that connection pools, OpenSearch caches and the JVM are warm. They are
	// extra documents, not part of Count, and their timings are discarded.
	WarmupDocs int
	// PollInterval is how often the watcher asks whether a document has
	// arrived. It bounds how precisely one document's latency can be
	// measured, so it is reported with the results.
	PollInterval time.Duration
	// PollWorkers is how many documents are checked at once.
	PollWorkers int
	// TargetRate caps events per second in publish mode. Zero is as fast as
	// the broker accepts.
	TargetRate float64
	// Repeats is how many times each event is published in publish mode. More
	// than one makes every event a duplicate of the one before it.
	Repeats int
	// Operation is the Debezium operation published in publish mode.
	Operation string
}

func (o Options) withDefaults() Options {
	if o.Count <= 0 {
		o.Count = 1000
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 100
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 250 * time.Millisecond
	}
	if o.PollWorkers <= 0 {
		o.PollWorkers = 8
	}
	if o.Repeats <= 0 {
		o.Repeats = 1
	}
	if o.Operation == "" {
		o.Operation = "c"
	}
	if o.WarmupDocs < 0 {
		o.WarmupDocs = 0
	}
	return o
}

// Runner drives one benchmark against a running stack.
//
// It writes to PostgreSQL or Kafka and watches the search indexes over HTTP.
// It never talks to the API or the consumer directly: what it measures is what
// an operator could measure from outside.
type Runner struct {
	Pool      *pgxpool.Pool
	Indexes   Indexes
	Publisher *Publisher
	Brokers   []string
	Topic     string
	DLQTopic  string
	Group     string
	Log       *slog.Logger
}

func (r Runner) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.New(slog.DiscardHandler)
}

// Index runs the indexing benchmark: generate documents, write them to
// PostgreSQL, and wait for them to come out of the consumer into the search
// indexes.
//
// The measurement starts at the first insert and ends when the last document
// is visible in the keyword index, so it covers the whole path — the WAL,
// Debezium, Kafka, the worker pool, the embedding provider and both stores —
// rather than any one part of it.
func (r Runner) Index(ctx context.Context, opts Options) (IndexingResult, error) {
	opts = opts.withDefaults()
	generator, err := NewGenerator(opts.RunID)
	if err != nil {
		return IndexingResult{}, err
	}

	if opts.WarmupDocs > 0 {
		if err := r.warmup(ctx, generator, opts); err != nil {
			return IndexingResult{}, fmt.Errorf("warm-up: %w", err)
		}
	}

	// Counts before and after say how much each index grew, which is what
	// proves nothing was duplicated or lost.
	documentsBefore, pointsBefore, err := r.counts(ctx)
	if err != nil {
		return IndexingResult{}, err
	}
	dlqBefore, err := r.dlqSize(ctx)
	if err != nil {
		return IndexingResult{}, err
	}

	// The warm-up documents come first and are not part of the measurement,
	// so a run of -count 1000 still measures a thousand documents.
	docs := generator.Documents(opts.WarmupDocs + opts.Count)[opts.WarmupDocs:]

	writer := Writer{Pool: r.Pool, BatchSize: opts.BatchSize}
	r.log().Info("writing rows", "count", len(docs), "batch_size", opts.BatchSize, "run", opts.RunID)

	started := time.Now()
	arrivals, insertDuration, err := writer.Insert(ctx, docs)
	if err != nil {
		return IndexingResult{}, fmt.Errorf("insert documents: %w", err)
	}
	r.log().Info("rows written, waiting for the indexes to catch up",
		"count", len(arrivals), "insert_duration", insertDuration.Round(time.Millisecond))

	watcher := Watcher{Indexes: r.Indexes, Interval: opts.PollInterval, Workers: opts.PollWorkers}
	watch, watchErr := watcher.Wait(ctx, arrivals)

	duration := time.Since(started)
	if !watch.Last.IsZero() {
		duration = watch.Last.Sub(started)
	}

	// Counting needs a refresh, or OpenSearch's refresh interval would show
	// up as missing documents.
	if err := r.Indexes.Refresh(ctx); err != nil {
		r.log().Warn("could not refresh the keyword index before counting", "error", err)
	}
	documentsAfter, pointsAfter, err := r.counts(ctx)
	if err != nil {
		return IndexingResult{}, err
	}
	dlqAfter, err := r.dlqSize(ctx)
	if err != nil {
		return IndexingResult{}, err
	}
	if dead := dlqAfter - dlqBefore; dead > 0 {
		r.log().Warn("events reached the dead-letter topic during the run", "count", dead, "topic", r.DLQTopic)
	}

	result := IndexingResult{
		RunID:          opts.RunID,
		Produced:       len(arrivals),
		Indexed:        len(arrivals) - len(watch.Pending),
		InsertDuration: insertDuration,
		Duration:       duration,
		Latency:        watch.Latency,
		Documents:      documentsAfter - documentsBefore,
		Points:         pointsAfter - pointsBefore,
	}
	return result, watchErr
}

// warmup indexes a few documents and waits for them, so the measured run does
// not pay for cold connection pools and empty caches.
func (r Runner) warmup(ctx context.Context, generator *Generator, opts Options) error {
	docs := generator.Documents(opts.WarmupDocs)
	r.log().Info("warming up", "documents", len(docs))

	writer := Writer{Pool: r.Pool, BatchSize: opts.BatchSize}
	arrivals, _, err := writer.Insert(ctx, docs)
	if err != nil {
		return err
	}
	watcher := Watcher{Indexes: r.Indexes, Interval: opts.PollInterval, Workers: opts.PollWorkers}
	_, err = watcher.Wait(ctx, arrivals)
	return err
}

// Publish writes change events straight to Kafka, which is how the
// backpressure and duplicate benchmarks reach conditions the database cannot
// produce on demand: a sustained rate above what the consumer can absorb, and
// the same event delivered many times.
func (r Runner) Publish(ctx context.Context, opts Options) (PublishResult, error) {
	opts = opts.withDefaults()
	if r.Publisher == nil {
		return PublishResult{}, fmt.Errorf("loadtest: publishing needs a Kafka publisher")
	}
	generator, err := NewGenerator(opts.RunID)
	if err != nil {
		return PublishResult{}, err
	}

	docs := generator.Documents(opts.Count)
	r.log().Info("publishing change events",
		"documents", len(docs), "repeats", opts.Repeats, "target_rate", opts.TargetRate, "topic", r.Topic)

	return r.Publisher.Publish(ctx, PublishOptions{
		Documents:  docs,
		Repeats:    opts.Repeats,
		Operation:  opts.Operation,
		TargetRate: opts.TargetRate,
		Version:    1,
	})
}

// Snapshot is the state of the pipeline at one moment.
type Snapshot struct {
	Rows      int64
	Documents int
	Points    int
	Lag       int64
	DLQ       int64
	At        time.Time
}

// String renders a snapshot as one line, so a script can print it repeatedly
// while a benchmark runs.
func (s Snapshot) String() string {
	return fmt.Sprintf("%s  rows=%d  documents=%d  points=%d  lag=%d  dead-lettered=%d",
		s.At.Format("15:04:05"), s.Rows, s.Documents, s.Points, s.Lag, s.DLQ)
}

// Snapshot reads the row count, both index sizes, the consumer group's lag and
// the size of the dead-letter topic.
//
// Lag is the number to watch while producing faster than the consumer can
// process: throughput alone looks healthy in that state, and only the growing
// lag says the work is arriving faster than it leaves.
func (r Runner) Snapshot(ctx context.Context, runID string) (Snapshot, error) {
	snapshot := Snapshot{At: time.Now()}

	if r.Pool != nil {
		rows, err := CountRun(ctx, r.Pool, runID)
		if err != nil {
			return snapshot, err
		}
		snapshot.Rows = rows
	}

	documents, points, err := r.counts(ctx)
	if err != nil {
		return snapshot, err
	}
	snapshot.Documents, snapshot.Points = documents, points

	if len(r.Brokers) > 0 && r.Topic != "" && r.Group != "" {
		lags, err := ConsumerLag(ctx, r.Brokers, r.Topic, r.Group)
		if err != nil {
			return snapshot, err
		}
		snapshot.Lag = TotalLag(lags)
	}
	if snapshot.DLQ, err = r.dlqSize(ctx); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

// Cleanup deletes the rows of a run, which removes their documents and points
// through the ordinary delete path.
func (r Runner) Cleanup(ctx context.Context, runID string) (int64, error) {
	if r.Pool == nil {
		return 0, fmt.Errorf("loadtest: cleanup needs a database connection")
	}
	return DeleteRun(ctx, r.Pool, runID)
}

func (r Runner) counts(ctx context.Context) (documents, points int, err error) {
	if documents, err = r.Indexes.CountDocuments(ctx); err != nil {
		return 0, 0, err
	}
	if points, err = r.Indexes.CountPoints(ctx); err != nil {
		return 0, 0, err
	}
	return documents, points, nil
}

// dlqSize returns how many messages the dead-letter topic has ever held, or 0
// when no dead-letter topic is configured or it does not exist yet.
func (r Runner) dlqSize(ctx context.Context) (int64, error) {
	if len(r.Brokers) == 0 || r.DLQTopic == "" {
		return 0, nil
	}
	size, err := TopicSize(ctx, r.Brokers, r.DLQTopic)
	if err != nil {
		// A stack that has never dead-lettered anything may not have the
		// topic; that is not a benchmark failure.
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "Unknown Topic") {
			return 0, nil
		}
		return 0, err
	}
	return size, nil
}
