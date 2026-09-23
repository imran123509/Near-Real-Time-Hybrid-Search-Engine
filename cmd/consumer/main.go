// Command consumer reads Debezium change events for the documents table from
// Kafka and applies them to OpenSearch and Qdrant.
//
// Each event carries the changed row, so the consumer never reads PostgreSQL:
//
//	Kafka -> worker pool -> cdc parser -> cdc.Service -> OpenSearch, embedding -> Qdrant
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
	"near-real-time-hybrid-search-engine/internal/indexing/cdc"
	"near-real-time-hybrid-search-engine/internal/kafka"
	"near-real-time-hybrid-search-engine/internal/retry"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
	"near-real-time-hybrid-search-engine/internal/startup"
)

const shutdownTimeout = 25 * time.Second

func main() {
	level := new(slog.LevelVar) // info until the configuration names a level
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	if err := run(logger, level); err != nil {
		logger.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("consumer stopped")
}

func run(logger *slog.Logger, level *slog.LevelVar) error {
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
	level.Set(cfg.App.LogLevel)
	logger.Info("starting consumer",
		"app", cfg.App.Name, "env", cfg.App.Env,
		"topic", cfg.Kafka.Topic, "group", cfg.Kafka.ConsumerGroup, "dlq_topic", cfg.Kafka.DLQTopic,
		"workers", cfg.Indexing.Workers, "queue_size", cfg.Indexing.QueueSize,
		"retry_max_attempts", cfg.Kafka.Retry.MaxAttempts,
		"retry_initial_backoff", cfg.Kafka.Retry.InitialBackoff,
		"retry_max_backoff", cfg.Kafka.Retry.MaxBackoff,
		"retry_multiplier", cfg.Kafka.Retry.Multiplier,
		"startup_timeout", cfg.App.StartupTimeout)

	// Dependencies may still be starting; each is retried until this deadline.
	initCtx, cancelInit := context.WithTimeout(ctx, cfg.App.StartupTimeout)
	defer cancelInit()

	osClient, err := opensearch.New(cfg.OpenSearch)
	if err != nil {
		return fmt.Errorf("init opensearch: %w", err)
	}
	defer closeWithLog(logger, "opensearch", osClient.Close)
	if err := startup.Retry(initCtx, logger, "opensearch", osClient.EnsureIndex); err != nil {
		return fmt.Errorf("init opensearch: %w", err)
	}
	logger.Info("opensearch ready", "index", cfg.OpenSearch.Index)

	qdClient, err := qdrant.New(cfg.Qdrant)
	if err != nil {
		return fmt.Errorf("init qdrant: %w", err)
	}
	defer closeWithLog(logger, "qdrant", qdClient.Close)
	// EnsureCollection also rejects an existing collection whose vector size
	// differs from QDRANT_VECTOR_SIZE.
	if err := startup.Retry(initCtx, logger, "qdrant", qdClient.EnsureCollection); err != nil {
		return fmt.Errorf("init qdrant: %w", err)
	}
	logger.Info("qdrant ready", "collection", cfg.Qdrant.Collection, "vector_size", cfg.Qdrant.VectorSize)

	embedder, err := embedding.New(initCtx, cfg.Embedding)
	if err != nil {
		return fmt.Errorf("init embedding provider: %w", err)
	}
	// The fake provider is for tests and local runs; its vectors mean nothing.
	if cfg.App.Env == "production" && embedder.Name() == embedding.ProviderFake {
		return fmt.Errorf("init embedding provider: the %q provider must not run in production", embedding.ProviderFake)
	}
	// Qdrant rejects vectors of any other length, so a mismatch has to stop
	// startup here rather than fail every message once consumption begins.
	if embedder.Dimension() != cfg.Qdrant.VectorSize {
		return fmt.Errorf("init embedding provider: %s produces %d-dimensional vectors but QDRANT_VECTOR_SIZE is %d",
			embedder.Name(), embedder.Dimension(), cfg.Qdrant.VectorSize)
	}
	logger.Info("embedding provider ready",
		"provider", embedder.Name(), "model", cfg.Embedding.Model,
		"dimension", embedder.Dimension(), "timeout", cfg.Embedding.Timeout)

	// The topic is created by Debezium or, under Docker Compose, by the
	// kafka-init job; until it exists, NewConsumer fails and is retried.
	var consumer *kafka.Consumer
	err = startup.Retry(initCtx, logger, "kafka", func(ctx context.Context) error {
		var err error
		consumer, err = kafka.NewConsumer(ctx, cfg.Kafka, logger)
		return err
	})
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

	changes, err := cdc.NewService(osClient, qdClient, embedder, cdc.DefaultMapping())
	if err != nil {
		return fmt.Errorf("init cdc service: %w", err)
	}
	pipeline, err := indexing.NewCDCPipeline(changes, deadLetters, retryPolicy(cfg.Kafka.Retry), logger)
	if err != nil {
		return fmt.Errorf("init indexing pipeline: %w", err)
	}

	// consumeCtx stops fetching on a shutdown signal, or when a worker reports
	// a failure that makes it unsafe to keep committing offsets.
	consumeCtx, stopConsuming := context.WithCancelCause(ctx)
	defer stopConsuming(nil)

	workers, err := indexing.NewWorkerPool(cfg.Indexing.Workers, cfg.Indexing.QueueSize, pipeline.Process, stopConsuming, logger)
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
	// consumer (flushing the final offset commits), Qdrant, then OpenSearch.
	return runErr
}

// retryPolicy turns the configured retry settings into the policy the
// indexing pipeline applies. The two types are kept apart so that config
// stays free of package dependencies and the retry package stays free of
// environment variables.
func retryPolicy(cfg config.RetryConfig) retry.Policy {
	return retry.Policy{
		MaxAttempts:    cfg.MaxAttempts,
		InitialBackoff: cfg.InitialBackoff,
		MaxBackoff:     cfg.MaxBackoff,
		Multiplier:     cfg.Multiplier,
	}
}

func closeWithLog(logger *slog.Logger, name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		logger.Error("close failed", "resource", name, "error", err)
		return
	}
	logger.Info("closed", "resource", name)
}
