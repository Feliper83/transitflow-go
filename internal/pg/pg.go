// Package pg provides a small helper to open a pgx connection pool, retrying
// while Postgres finishes starting up (services and their databases come up
// together under docker compose).
package pg

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a pool for dsn, pinging until it succeeds or the deadline
// (~30s) passes.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var lastErr error
	for i := 0; i < 30; i++ {
		pool, err := pgxpool.New(connectCtx, dsn)
		if err == nil {
			if err = pool.Ping(connectCtx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		select {
		case <-connectCtx.Done():
			return nil, lastErr
		case <-time.After(time.Second):
		}
	}
	return nil, lastErr
}
