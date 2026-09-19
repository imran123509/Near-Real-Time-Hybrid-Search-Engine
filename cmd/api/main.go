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
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
)

const (
	startupTimeout    = 10 * time.Second
	readHeaderTimeout = 5 * time.Second
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
	logger.Info("starting api", "app", cfg.App.Name, "env", cfg.App.Env, "addr", cfg.Server.Addr())

	initCtx, cancelInit := context.WithTimeout(ctx, startupTimeout)
	defer cancelInit()

	pool, err := postgres.New(initCtx, cfg.PostgreSQL)
	if err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}
	defer func() {
		pool.Close()
		logger.Info("postgres pool closed")
	}()
	logger.Info("connected to postgres",
		"max_conns", cfg.PostgreSQL.MaxConns, "min_conns", cfg.PostgreSQL.MinConns)

	osClient, err := opensearch.New(cfg.OpenSearch)
	if err != nil {
		return fmt.Errorf("init opensearch: %w", err)
	}
	defer closeWithLog(logger, "opensearch", osClient.Close)
	if err := osClient.Ping(initCtx); err != nil {
		return fmt.Errorf("init opensearch: %w", err)
	}
	logger.Info("connected to opensearch", "index", cfg.OpenSearch.Index)

	qdClient, err := qdrant.New(cfg.Qdrant)
	if err != nil {
		return fmt.Errorf("init qdrant: %w", err)
	}
	defer closeWithLog(logger, "qdrant", qdClient.Close)
	if err := qdClient.Ping(initCtx); err != nil {
		return fmt.Errorf("init qdrant: %w", err)
	}
	logger.Info("connected to qdrant", "collection", cfg.Qdrant.Collection, "vector_size", cfg.Qdrant.VectorSize)

	svc := search.NewService(osClient, qdClient, postgres.NewRepository(pool))
	handler := api.NewHandler(svc, logger)

	srv := &http.Server{
		Addr:              cfg.Server.Addr(),
		Handler:           api.NewRouter(handler),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
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

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
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
