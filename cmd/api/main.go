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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"near-real-time-hybrid-search-engine/internal/api"
	"near-real-time-hybrid-search-engine/internal/config"
	"near-real-time-hybrid-search-engine/internal/embedding"
	"near-real-time-hybrid-search-engine/internal/metrics"
	"near-real-time-hybrid-search-engine/internal/postgres"
	"near-real-time-hybrid-search-engine/internal/search/hybrid"
	"near-real-time-hybrid-search-engine/internal/search/opensearch"
	"near-real-time-hybrid-search-engine/internal/search/qdrant"
	"near-real-time-hybrid-search-engine/internal/startup"
)

const readHeaderTimeout = 5 * time.Second

func main() {
	// Info until the configuration names a level. Records logged with a
	// request's context carry its request ID.
	level := new(slog.LevelVar)
	logger := slog.New(api.NewLogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	if err := run(logger, level); err != nil {
		logger.Error("api stopped with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, level *slog.LevelVar) error {
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
	level.Set(cfg.App.LogLevel)
	logger.Info("starting api", "app", cfg.App.Name, "env", cfg.App.Env, "addr", cfg.Server.Addr(),
		"metrics_enabled", cfg.Metrics.Enabled, "metrics_path", cfg.Metrics.Path)

	// Built once, before anything that reports into it. The default
	// registerer already carries the Go runtime and process collectors, so
	// scraping this endpoint also gives goroutines, memory and GC for free.
	var m *metrics.Metrics
	if cfg.Metrics.Enabled {
		m = metrics.New(prometheus.DefaultRegisterer)
		m.SetInfo(cfg.App.Name, cfg.App.Env, "api")
	}

	// Dependencies may still be starting; each is retried until this deadline.
	initCtx, cancelInit := context.WithTimeout(ctx, cfg.App.StartupTimeout)
	defer cancelInit()

	var pool *pgxpool.Pool
	err = startup.Retry(initCtx, logger, "postgres", func(ctx context.Context) error {
		var err error
		pool, err = postgres.New(ctx, cfg.PostgreSQL)
		return err
	})
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
	if err := startup.Retry(initCtx, logger, "opensearch", osClient.Ping); err != nil {
		return fmt.Errorf("init opensearch: %w", err)
	}
	logger.Info("connected to opensearch", "index", cfg.OpenSearch.Index)

	qdClient, err := qdrant.New(cfg.Qdrant)
	if err != nil {
		return fmt.Errorf("init qdrant: %w", err)
	}
	defer closeWithLog(logger, "qdrant", qdClient.Close)
	if err := startup.Retry(initCtx, logger, "qdrant", qdClient.Ping); err != nil {
		return fmt.Errorf("init qdrant: %w", err)
	}
	logger.Info("connected to qdrant", "collection", cfg.Qdrant.Collection, "vector_size", cfg.Qdrant.VectorSize)

	embedder, err := embedding.New(initCtx, cfg.Embedding)
	if err != nil {
		return fmt.Errorf("init embedding provider: %w", err)
	}
	// The fake provider is for tests and local runs; its vectors mean nothing.
	if cfg.App.Env == "production" && embedder.Name() == embedding.ProviderFake {
		return fmt.Errorf("init embedding provider: the %q provider must not run in production", embedding.ProviderFake)
	}
	// Query vectors are compared with the stored document vectors, so they
	// must have the collection's dimension.
	if embedder.Dimension() != cfg.Qdrant.VectorSize {
		return fmt.Errorf("init embedding provider: %s produces %d-dimensional vectors but QDRANT_VECTOR_SIZE is %d",
			embedder.Name(), embedder.Dimension(), cfg.Qdrant.VectorSize)
	}
	logger.Info("embedding provider ready",
		"provider", embedder.Name(), "model", cfg.Embedding.Model, "dimension", embedder.Dimension())

	svc, err := hybrid.New(osClient, qdClient, embedder, cfg.Search, logger)
	if err != nil {
		return fmt.Errorf("init hybrid search: %w", err)
	}
	// Observation only: the service answers searches exactly as it did
	// before, and reports afterwards what it did.
	svc.Observe(m.SearchObserver())
	logger.Info("hybrid search ready",
		"default_limit", cfg.Search.DefaultLimit, "max_limit", cfg.Search.MaxLimit,
		"candidate_limit", cfg.Search.CandidateLimit, "rrf_k", cfg.Search.RRFK, "timeout", cfg.Search.Timeout)

	// The embedding provider has no free health check; a failing provider
	// shows up as failed searches rather than as not ready.
	readiness := api.NewReadinessHandler(logger,
		api.ReadinessCheck{Name: "opensearch", Check: osClient.Ping},
		api.ReadinessCheck{Name: "qdrant", Check: qdClient.Ping},
	)
	readiness.Observe(m.SetReady)

	routerOpts := api.Options{MetricsPath: cfg.Metrics.Path}
	if cfg.Metrics.Enabled {
		routerOpts.Metrics = metrics.Handler(prometheus.DefaultGatherer, logger)
		routerOpts.Middleware = m.Middleware
		// Nothing else probes readiness on a schedule, and the API's image
		// has no shell for a container health check, so the gauge would
		// otherwise only change when somebody asked for /ready. This samples
		// the same checks the endpoint runs, and stops with the server.
		go sampleReadiness(ctx, readiness, logger)
	}
	router := api.NewRouter(api.NewSearchHandler(svc, cfg.Search.Timeout), readiness, logger, routerOpts)

	srv := &http.Server{
		Addr:              cfg.Server.Addr(),
		Handler:           router,
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

// readinessSampleInterval is how often readiness is checked in the
// background. It is shorter than a typical scrape interval, so the gauge a
// scrape reads is never stale by more than one sample.
const readinessSampleInterval = 10 * time.Second

// sampleReadiness keeps application_ready current by running the same checks
// /ready runs. It returns when ctx is cancelled, which happens on shutdown,
// so it leaves no goroutine behind.
func sampleReadiness(ctx context.Context, readiness *api.ReadinessHandler, logger *slog.Logger) {
	ticker := time.NewTicker(readinessSampleInterval)
	defer ticker.Stop()

	for {
		checkCtx, cancel := context.WithTimeout(ctx, readinessSampleInterval)
		readiness.Check(checkCtx)
		cancel()

		select {
		case <-ctx.Done():
			logger.Debug("readiness sampling stopped")
			return
		case <-ticker.C:
		}
	}
}

func closeWithLog(logger *slog.Logger, name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		logger.Error("close failed", "resource", name, "error", err)
		return
	}
	logger.Info("closed", "resource", name)
}
