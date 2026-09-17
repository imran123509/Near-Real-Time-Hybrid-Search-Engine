// Command consumer reads document events from Kafka and indexes the documents
// into OpenSearch and Qdrant.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/embedding"
	"near-real-time-hybrid-search-engine/internal/indexing"
	"near-real-time-hybrid-search-engine/internal/kafka"
	"near-real-time-hybrid-search-engine/internal/postgres"
	"near-real-time-hybrid-search-engine/internal/search"
)

const (
	startupTimeout  = 30 * time.Second
	shutdownTimeout = 25 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("consumer stopped")
}

func run(logger *slog.Logger) error {
	// rootCtx is the parent of all message processing. It is not cancelled by
	// a shutdown signal, so in-flight messages can finish.
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, stop := signal.NotifyContext(rootCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger.Info("starting consumer",
		"topic", cfg.Kafka.Topic, "group", cfg.Kafka.GroupID, "dlq_topic", cfg.Kafka.DLQTopic,
		"workers", cfg.Kafka.Workers, "queue_size", cfg.Kafka.QueueSize)

	initCtx, cancelInit := context.WithTimeout(ctx, startupTimeout)
	defer cancelInit()

	db, err := postgres.NewPool(initCtx, cfg.PostgresURL)
	if err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}
	defer closeWithLog(logger, "postgres", func() error { db.Close(); return nil })
	logger.Info("connected to postgres")

	osClient, err := search.NewOpenSearchClient(initCtx, cfg.OpenSearch)
	if err != nil {
		return fmt.Errorf("init opensearch: %w", err)
	}
	defer closeWithLog(logger, "opensearch", osClient.Close)
	logger.Info("connected to opensearch")

	qdClient, err := search.NewQdrantClient(initCtx, cfg.Qdrant)
	if err != nil {
		return fmt.Errorf("init qdrant: %w", err)
	}
	defer closeWithLog(logger, "qdrant", qdClient.Close)
	logger.Info("connected to qdrant")

	embedder, err := embedding.NewClient(cfg.Gemini)
	if err != nil {
		return fmt.Errorf("init embedding client: %w", err)
	}

	consumer, err := kafka.NewConsumer(initCtx, cfg.Kafka, logger)
	if err != nil {
		return fmt.Errorf("init kafka consumer: %w", err)
	}
	defer closeWithLog(logger, "kafka consumer", consumer.Close)
	logger.Info("connected to kafka")

	deadLetters, err := kafka.NewDeadLetterWriter(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("init dead-letter writer: %w", err)
	}
	defer closeWithLog(logger, "kafka dead-letter writer", deadLetters.Close)

	indexer := indexing.NewIndexer(postgres.NewRepository(db), embedder, osClient, qdClient,
		cfg.OpenSearch.Index, cfg.Qdrant.Collection)
	if err := indexer.EnsureStores(initCtx, cfg.Gemini.Dimensions); err != nil {
		return fmt.Errorf("init search stores: %w", err)
	}
	pipeline := indexing.NewPipeline(indexer, deadLetters, cfg.Kafka.MaxAttempts, logger)

	// consumeCtx stops fetching on a shutdown signal, or when a worker reports
	// a failure that makes it unsafe to keep committing offsets.
	consumeCtx, stopConsuming := context.WithCancelCause(ctx)
	defer stopConsuming(nil)

	workers, err := indexing.NewWorkerPool(cfg.Kafka.Workers, cfg.Kafka.QueueSize, pipeline.Process, stopConsuming, logger)
	if err != nil {
		return fmt.Errorf("init worker pool: %w", err)
	}
	workers.Start(rootCtx)

	logger.Info("consuming messages")
	runErr := consumer.Run(consumeCtx, workers.Submit)
	stop() // a second signal now terminates immediately

	switch {
	case runErr != nil:
		runErr = fmt.Errorf("kafka consumer: %w", runErr)
	case ctx.Err() != nil:
		logger.Info("shutdown signal received")
	default:
		runErr = fmt.Errorf("worker failure: %w", context.Cause(consumeCtx))
	}
	if runErr != nil {
		logger.Error("stopping consumer", "error", runErr)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if err := workers.Shutdown(shutdownCtx); err != nil {
		return errors.Join(runErr, fmt.Errorf("shut down workers: %w", err))
	}
	logger.Info("workers stopped")

	// Deferred closes now run in reverse order: dead-letter writer, Kafka
	// consumer (flushing the final offset commits), Qdrant, OpenSearch, and
	// PostgreSQL last.
	return runErr
}

func closeWithLog(logger *slog.Logger, name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		logger.Error("close failed", "resource", name, "error", err)
		return
	}
	logger.Info("closed", "resource", name)
}
