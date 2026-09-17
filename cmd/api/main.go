// Command api serves the hybrid search HTTP API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"near-real-time-hybrid-search-engine/internal/api"
	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/postgres"
	"near-real-time-hybrid-search-engine/internal/search"
)

const (
	startupTimeout    = 10 * time.Second
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 120 * time.Second
	shutdownTimeout   = 15 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("api stopped with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	// rootCtx is the parent of every request context. Cancelling it after the
	// shutdown grace period stops any requests that are still running.
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, stop := signal.NotifyContext(rootCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger.Info("starting api", "addr", cfg.HTTPAddr)

	initCtx, cancelInit := context.WithTimeout(ctx, startupTimeout)
	defer cancelInit()

	pool, err := postgres.NewPool(initCtx, cfg.PostgresURL)
	if err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}
	defer func() {
		pool.Close()
		logger.Info("postgres pool closed")
	}()
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

	svc := search.NewService(osClient, qdClient, postgres.NewRepository(pool))
	handler := api.NewHandler(svc, logger)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewRouter(handler),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		BaseContext:       func(net.Listener) context.Context { return rootCtx },
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		stop() // a second signal now terminates immediately
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()

	err = srv.Shutdown(shutdownCtx)
	// Stop any requests still running so they release their database
	// connections before the deferred pool.Close waits on them.
	cancel()
	if err != nil {
		return fmt.Errorf("shutdown http server: %w", err)
	}
	logger.Info("http server stopped")
	return nil
}

func closeWithLog(logger *slog.Logger, name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		logger.Error("close failed", "resource", name, "error", err)
		return
	}
	logger.Info("closed", "resource", name)
}
