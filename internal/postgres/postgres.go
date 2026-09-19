// Package postgres provides the PostgreSQL connection pool and document access.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"near-real-time-hybrid-search-engine/internal/config"
)

// maxPoolConns is an upper bound that catches a misconfigured pool before it
// can exhaust the server's connection slots.
const maxPoolConns = 10_000

// New creates a connection pool from cfg and verifies the database is
// reachable, so startup fails fast when PostgreSQL is unavailable.
//
// ctx controls how long the connection attempt and ping may take; it is not
// retained by the pool. The caller owns the returned pool and must call Close.
// The pool is safe for concurrent use and is meant to live for the whole
// process, shared by every request or worker.
func New(ctx context.Context, cfg config.PostgreSQLConfig) (*pgxpool.Pool, error) {
	poolCfg, err := poolConfig(cfg)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// poolConfig turns application settings into a pgxpool config. It rejects
// values that would leave the pool unusable rather than silently falling back
// to pgx defaults, which allow only four connections.
func poolConfig(cfg config.PostgreSQLConfig) (*pgxpool.Config, error) {
	switch {
	case cfg.URL == "":
		return nil, errors.New("postgres url is required")
	case cfg.MaxConns < 1 || cfg.MaxConns > maxPoolConns:
		return nil, fmt.Errorf("postgres max conns must be between 1 and %d, got %d", maxPoolConns, cfg.MaxConns)
	case cfg.MinConns < 0:
		return nil, fmt.Errorf("postgres min conns must not be negative, got %d", cfg.MinConns)
	case cfg.MinConns > cfg.MaxConns:
		return nil, fmt.Errorf("postgres min conns (%d) must not exceed max conns (%d)", cfg.MinConns, cfg.MaxConns)
	}
	for _, d := range []struct {
		name  string
		value time.Duration
	}{
		{"conn lifetime", cfg.MaxConnLifetime},
		{"conn idle time", cfg.MaxConnIdleTime},
		{"health check period", cfg.HealthCheckPeriod},
		{"connect timeout", cfg.ConnectTimeout},
	} {
		if d.value <= 0 {
			return nil, fmt.Errorf("postgres %s must be positive, got %s", d.name, d.value)
		}
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %s", parseFailureReason(err))
	}

	poolCfg.MaxConns = int32(cfg.MaxConns)
	poolCfg.MinConns = int32(cfg.MinConns)
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	// Spread reconnects out so connections opened together do not all expire at
	// the same moment and empty the pool.
	poolCfg.MaxConnLifetimeJitter = cfg.MaxConnLifetime / 10
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	return poolCfg, nil
}

// parseFailureReason explains why pgx rejected a connection string without
// repeating the string itself.
//
// pgx formats these errors as "cannot parse `<connection string>`: <reason>"
// and masks passwords only on a best-effort basis, so everything up to the
// reason is dropped. The pgx error is deliberately not wrapped: reaching it
// again would expose ParseConfigError.ConnString, credentials included.
func parseFailureReason(err error) string {
	var parseErr *pgconn.ParseConfigError
	if !errors.As(err, &parseErr) {
		return err.Error()
	}
	// Cut at the last separator: a connection string could contain one, and
	// keeping too little of the reason is better than leaking any of the URL.
	msg := err.Error()
	if i := strings.LastIndex(msg, "`: "); i >= 0 {
		return msg[i+len("`: "):]
	}
	return "invalid connection string"
}
