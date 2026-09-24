// Command loadgen drives the indexing benchmarks: it generates deterministic
// documents, writes them to PostgreSQL or straight to Kafka, and measures what
// comes out of the consumer.
//
// It is a measuring tool, not part of the running system. The search
// benchmarks are k6 scripts under benchmarks/search; this program covers the
// half of the system that HTTP load cannot reach.
//
//	go run ./cmd/loadgen -mode index      # write rows and time the pipeline
//	go run ./cmd/loadgen -mode publish    # produce faster than the consumer
//	go run ./cmd/loadgen -mode duplicate  # deliver the same events repeatedly
//	go run ./cmd/loadgen -mode watch      # print rows, indexes, lag and DLQ
//	go run ./cmd/loadgen -mode cleanup    # delete the rows a run created
//
// Every setting has a LOADTEST_* environment variable and a flag; the flag
// wins. See benchmarks/README.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/loadtest"
	"near-real-time-hybrid-search-engine/internal/postgres"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("benchmark stopped", "error", err)
		os.Exit(1)
	}
}

// settings are everything the benchmark needs to reach the local stack. The
// defaults match the ports docker-compose.yml publishes, so a default run
// needs no configuration at all.
type settings struct {
	mode     string
	runID    string
	database string

	openSearchURL   string
	openSearchIndex string
	qdrantURL       string
	qdrantColl      string

	brokers  []string
	topic    string
	dlqTopic string
	group    string

	count      int
	batchSize  int
	warmupDocs int
	repeats    int
	targetRate float64
	operation  string

	pollInterval time.Duration
	pollWorkers  int
	timeout      time.Duration
	interval     time.Duration
	all          bool
}

func run(logger *slog.Logger) error {
	cfg := parseSettings()

	// A benchmark must never be able to run away with the machine, and it
	// must stop cleanly on Ctrl+C so its cleanup still happens.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}

	runner := loadtest.Runner{
		Indexes: loadtest.Indexes{
			OpenSearchURL: cfg.openSearchURL,
			Index:         cfg.openSearchIndex,
			QdrantURL:     cfg.qdrantURL,
			Collection:    cfg.qdrantColl,
		},
		Brokers:  cfg.brokers,
		Topic:    cfg.topic,
		DLQTopic: cfg.dlqTopic,
		Group:    cfg.group,
		Log:      logger,
	}

	// Only the modes that write rows need PostgreSQL, and only those that
	// publish need Kafka, so neither is opened unless it is used.
	if cfg.mode == "index" || cfg.mode == "watch" || cfg.mode == "cleanup" {
		// A benchmark's own pool is small and short-lived: it writes in
		// batches from one goroutine, and holding more connections open would
		// take them from the services being measured.
		pool, err := postgres.New(ctx, config.PostgreSQLConfig{
			URL:               cfg.database,
			MaxConns:          8,
			MinConns:          1,
			MaxConnLifetime:   30 * time.Minute,
			MaxConnIdleTime:   5 * time.Minute,
			HealthCheckPeriod: time.Minute,
			ConnectTimeout:    10 * time.Second,
		})
		if err != nil {
			return fmt.Errorf("connect to postgres: %w", err)
		}
		defer pool.Close()
		runner.Pool = pool
	}
	if cfg.mode == "publish" || cfg.mode == "duplicate" {
		publisher, err := loadtest.NewPublisher(cfg.brokers, cfg.topic)
		if err != nil {
			return err
		}
		defer func() {
			if err := publisher.Close(); err != nil {
				logger.Error("closing the publisher failed", "error", err)
			}
		}()
		runner.Publisher = publisher
	}

	opts := loadtest.Options{
		RunID:        cfg.runID,
		Count:        cfg.count,
		BatchSize:    cfg.batchSize,
		WarmupDocs:   cfg.warmupDocs,
		PollInterval: cfg.pollInterval,
		PollWorkers:  cfg.pollWorkers,
		TargetRate:   cfg.targetRate,
		Repeats:      cfg.repeats,
		Operation:    cfg.operation,
	}

	switch cfg.mode {
	case "index":
		return indexMode(ctx, runner, opts, cfg)
	case "publish", "duplicate":
		return publishMode(ctx, runner, opts, cfg)
	case "watch":
		return watchMode(ctx, runner, cfg)
	case "cleanup":
		return cleanupMode(ctx, runner, cfg)
	default:
		return fmt.Errorf("unknown mode %q: want index, publish, duplicate, watch or cleanup", cfg.mode)
	}
}

func indexMode(ctx context.Context, runner loadtest.Runner, opts loadtest.Options, cfg settings) error {
	result, err := runner.Index(ctx, opts)
	// A run that did not finish still measured something, so the report is
	// printed either way and the error is reported after it.
	fmt.Println()
	fmt.Print(result.String())
	fmt.Printf("  Poll interval:   %s (the resolution of a single document's latency)\n", cfg.pollInterval)
	fmt.Println()

	if err != nil {
		return fmt.Errorf("%d documents never reached the keyword index: %w", result.Missing(), err)
	}
	if result.Documents != result.Produced || result.Points != result.Produced {
		fmt.Printf("WARNING: the indexes grew by %d documents and %d points for %d rows.\n"+
			"         A smaller number means documents were replaced rather than added,\n"+
			"         which is expected when the same run id is used twice.\n\n",
			result.Documents, result.Points, result.Produced)
	}
	return nil
}

func publishMode(ctx context.Context, runner loadtest.Runner, opts loadtest.Options, cfg settings) error {
	if cfg.mode == "duplicate" && opts.Repeats < 2 {
		opts.Repeats = 3 // duplicates are the point of this mode
	}

	before, err := runner.Snapshot(ctx, cfg.runID)
	if err != nil {
		return err
	}
	fmt.Printf("before:   %s\n", before)

	result, err := runner.Publish(ctx, opts)
	fmt.Printf("\npublished %d events in %s (%.1f events/sec)\n",
		result.Published, result.Duration.Round(time.Millisecond), result.Rate())
	if err != nil {
		return err
	}

	after, err := runner.Snapshot(ctx, cfg.runID)
	if err != nil {
		return err
	}
	fmt.Printf("after:    %s\n\n", after)
	fmt.Printf("Watch the consumer catch up with:\n"+
		"  go run ./cmd/loadgen -mode watch -run %s\n\n", cfg.runID)
	return nil
}

// watchMode prints one line per interval: rows, index sizes, consumer lag and
// dead-letter count. It is what a backpressure or recovery run is read from.
func watchMode(ctx context.Context, runner loadtest.Runner, cfg settings) error {
	interval := cfg.interval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		snapshot, err := runner.Snapshot(ctx, cfg.runID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		fmt.Println(snapshot)

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func cleanupMode(ctx context.Context, runner loadtest.Runner, cfg settings) error {
	runID := cfg.runID
	if cfg.all {
		// An empty run id matches every benchmark run, and still only
		// benchmark rows: they are found by the generated URL prefix.
		runID = ""
	}

	deleted, err := runner.Cleanup(ctx, runID)
	if err != nil {
		return err
	}
	scope := "run " + cfg.runID
	if cfg.all {
		scope = "every benchmark run"
	}
	fmt.Printf("deleted %d rows from %s\n", deleted, scope)
	fmt.Println("their documents and points leave both indexes through the ordinary delete path")
	return nil
}

func parseSettings() settings {
	cfg := settings{
		mode:            getEnv("LOADTEST_MODE", "index"),
		runID:           getEnv("LOADTEST_RUN_ID", time.Now().UTC().Format("20060102-150405")),
		database:        getEnv("LOADTEST_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/searchdb?sslmode=disable"),
		openSearchURL:   getEnv("LOADTEST_OPENSEARCH_URL", "http://localhost:9200"),
		openSearchIndex: getEnv("LOADTEST_OPENSEARCH_INDEX", "documents"),
		qdrantURL:       getEnv("LOADTEST_QDRANT_URL", "http://localhost:6333"),
		qdrantColl:      getEnv("LOADTEST_QDRANT_COLLECTION", "documents"),
		topic:           getEnv("LOADTEST_TOPIC", "search.public.documents"),
		group:           getEnv("LOADTEST_CONSUMER_GROUP", "near-realtime-search"),
		count:           getEnvInt("LOADTEST_DOCUMENT_COUNT", 1000),
		batchSize:       getEnvInt("LOADTEST_BATCH_SIZE", 100),
		warmupDocs:      getEnvInt("LOADTEST_WARMUP_DOCS", 50),
		repeats:         getEnvInt("LOADTEST_REPEATS", 1),
		targetRate:      getEnvFloat("LOADTEST_TARGET_RPS", 0),
		operation:       getEnv("LOADTEST_OPERATION", "c"),
		pollInterval:    getEnvDuration("LOADTEST_POLL_INTERVAL", 250*time.Millisecond),
		pollWorkers:     getEnvInt("LOADTEST_POLL_WORKERS", 8),
		timeout:         getEnvDuration("LOADTEST_TIMEOUT", 10*time.Minute),
		interval:        getEnvDuration("LOADTEST_WATCH_INTERVAL", 2*time.Second),
	}
	brokers := getEnv("LOADTEST_KAFKA_BROKERS", "localhost:29092")
	dlq := getEnv("LOADTEST_DLQ_TOPIC", "")

	flag.StringVar(&cfg.mode, "mode", cfg.mode, "index, publish, duplicate, watch or cleanup")
	flag.StringVar(&cfg.runID, "run", cfg.runID, "run id: the same id always generates the same documents")
	flag.IntVar(&cfg.count, "count", cfg.count, "how many documents to generate")
	flag.IntVar(&cfg.batchSize, "batch", cfg.batchSize, "rows per insert statement")
	flag.IntVar(&cfg.warmupDocs, "warmup", cfg.warmupDocs, "documents indexed before the measurement starts")
	flag.IntVar(&cfg.repeats, "repeats", cfg.repeats, "how many times to publish each event (publish and duplicate modes)")
	flag.Float64Var(&cfg.targetRate, "rate", cfg.targetRate, "events per second to publish; 0 is as fast as possible")
	flag.StringVar(&cfg.operation, "op", cfg.operation, "Debezium operation to publish: c, u, d or r")
	flag.DurationVar(&cfg.pollInterval, "poll", cfg.pollInterval, "how often to check whether a document has been indexed")
	flag.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "give up after this long")
	flag.DurationVar(&cfg.interval, "interval", cfg.interval, "how often watch mode prints a line")
	flag.BoolVar(&cfg.all, "all", false, "cleanup mode: delete the rows of every benchmark run, not just this one")
	flag.StringVar(&brokers, "brokers", brokers, "comma-separated Kafka brokers")
	flag.StringVar(&cfg.topic, "topic", cfg.topic, "the topic the consumer reads")
	flag.StringVar(&dlq, "dlq-topic", dlq, "dead-letter topic; defaults to the topic plus .dlq")
	flag.StringVar(&cfg.group, "group", cfg.group, "the consumer group whose lag is reported")
	flag.Parse()

	for _, broker := range strings.Split(brokers, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.brokers = append(cfg.brokers, broker)
		}
	}
	cfg.dlqTopic = dlq
	if cfg.dlqTopic == "" {
		cfg.dlqTopic = cfg.topic + ".dlq"
	}
	return cfg
}

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, err := strconv.Atoi(getEnv(key, "")); err == nil {
		return v
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(getEnv(key, ""), 64); err == nil {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(getEnv(key, "")); err == nil {
		return v
	}
	return fallback
}
