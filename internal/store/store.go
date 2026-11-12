// Package store runs SQL against an embedded PostgreSQL when it starts.
// A remote database is not contacted. If Start fails, callers record not-started
// and do not treat an in-memory map as PostgreSQL.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  id text PRIMARY KEY,
  tenant_id text NOT NULL,
  kind text NOT NULL,
  payload bytea,
  token text,
  state text NOT NULL,
  result text,
  applies integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobs_tenant_kind_idx ON jobs (tenant_id, kind);
`

// Store is one embedded PostgreSQL and a pgx pool.
// The token column is a synthetic lookup key for the index experiment.
// It does not store bearer tokens.
type Store struct {
	pool       *pgxpool.Pool
	emb        *embeddedpostgres.EmbeddedPostgres
	runtimeDir string
	dataDir    string
}

// Start downloads binaries into the shared runtime directory on first use
// and starts a database on 127.0.0.1. The DSN is not returned.
func Start(ctx context.Context) (*Store, error) {
	// The zip cache is shared. Each start gets its own extracted runtime so
	// parallel tests do not unpack into one directory.
	cache := filepath.Join(os.TempDir(), "syn-perf-lab-pg-cache")
	if err := os.MkdirAll(cache, 0o700); err != nil {
		return nil, err
	}
	unlock, err := lockCache(cache)
	if err != nil {
		return nil, err
	}
	defer unlock()
	runtime, err := os.MkdirTemp("", "syn-perf-lab-pg-runtime-")
	if err != nil {
		return nil, err
	}
	data, err := os.MkdirTemp("", "syn-perf-lab-pg-data-")
	if err != nil {
		os.RemoveAll(runtime)
		return nil, err
	}
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		port, err := freePort()
		if err != nil {
			os.RemoveAll(runtime)
			os.RemoveAll(data)
			return nil, err
		}
		cfg := embeddedpostgres.DefaultConfig().
			Username("lab").
			Password("lab").
			Database("labdb").
			Port(uint32(port)).
			CachePath(cache).
			RuntimePath(runtime).
			DataPath(data).
			Logger(io.Discard).
			StartTimeout(3 * time.Minute).
			Locale("C")
		emb := embeddedpostgres.NewDatabase(cfg)
		if err := emb.Start(); err != nil {
			last = err
			_ = emb.Stop()
			continue
		}
		pool, err := connect(ctx, port)
		if err != nil {
			_ = emb.Stop()
			last = err
			continue
		}
		if _, err := pool.Exec(ctx, schema); err != nil {
			pool.Close()
			_ = emb.Stop()
			os.RemoveAll(runtime)
			os.RemoveAll(data)
			return nil, err
		}
		return &Store{pool: pool, emb: emb, runtimeDir: runtime, dataDir: data}, nil
	}
	os.RemoveAll(runtime)
	os.RemoveAll(data)
	if last == nil {
		last = errors.New("embedded postgres did not start")
	}
	return nil, last
}

func lockCache(dir string) (func(), error) {
	path := filepath.Join(dir, "start.lock")
	deadline := time.Now().Add(4 * time.Minute)
	for {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = file.WriteString(strconv.Itoa(os.Getpid()))
			return func() {
				file.Close()
				os.Remove(path)
			}, nil
		}
		info, statErr := os.Stat(path)
		if statErr == nil && time.Since(info.ModTime()) > 5*time.Minute {
			os.Remove(path)
			continue
		}
		if time.Now().After(deadline) {
			return nil, errors.New("timed out waiting for the postgres cache lock")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func connect(ctx context.Context, port int) (*pgxpool.Pool, error) {
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("lab", "lab"),
		Host:     net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
		Path:     "labdb",
		RawQuery: "sslmode=disable",
	}
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, errors.New("postgres config was rejected")
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("postgres connect failed")
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, errors.New("postgres ping failed")
	}
	return pool, nil
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	return port, ln.Close()
}

// Started reports whether SQL can be executed.
func (s *Store) Started() bool { return s != nil && s.pool != nil }

// Complete inserts the job if needed and lets one transaction move it to done.
// The winner's applies value is 1. A concurrent or later call does not increment it.
func (s *Store) Complete(ctx context.Context, id, tenant, kind string, payload []byte, lookup, result string) (int, bool, error) {
	if !s.Started() {
		return 0, false, errors.New("postgres is not started")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
INSERT INTO jobs (id, tenant_id, kind, payload, token, state, result, applies)
VALUES ($1, $2, $3, $4, $5, 'open', '', 0)
ON CONFLICT (id) DO NOTHING`, id, tenant, kind, payload, lookup); err != nil {
		return 0, false, err
	}
	var applies int
	err = tx.QueryRow(ctx, `
UPDATE jobs
SET state = 'done', result = $2, applies = applies + 1, updated_at = now()
WHERE id = $1 AND state <> 'done'
RETURNING applies`, id, result).Scan(&applies)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.QueryRow(ctx, `SELECT applies FROM jobs WHERE id = $1`, id).Scan(&applies); err != nil {
			return 0, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, false, err
		}
		return applies, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, err
	}
	return applies, true, nil
}

// Plan is one EXPLAIN ANALYZE result and a timed query.
type Plan struct {
	Name       string
	Plan       string
	QueryMean  time.Duration
	Iterations int
	SeqScanOff bool
}

// SeedLookup inserts rows for the index comparison.
// token values are synthetic lookup keys, not credentials.
func (s *Store) SeedLookup(ctx context.Context, rows int) error {
	if !s.Started() {
		return errors.New("postgres is not started")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("seed-%d", i)
		tenant := fmt.Sprintf("SYN-TENANT-%d", i%50)
		kind := fmt.Sprintf("kind-%d", i%10)
		token := fmt.Sprintf("tok-%d", i)
		if _, err := tx.Exec(ctx, `
INSERT INTO jobs (id, tenant_id, kind, payload, token, state, result, applies)
VALUES ($1, $2, $3, $4, $5, 'ready', '', 0)
ON CONFLICT (id) DO NOTHING`, id, tenant, kind, []byte("seed"), token); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ExplainTenantKind runs the indexed predicate.
func (s *Store) ExplainTenantKind(ctx context.Context, tenant, kind string, iterations int, seqScanOff bool) (Plan, error) {
	query := `SELECT count(*) FROM jobs WHERE tenant_id = $1 AND kind = $2`
	name := "tenant-kind"
	if seqScanOff {
		name = "tenant-kind-seqscan-off"
	}
	return s.explain(ctx, name, query, []any{tenant, kind}, iterations, seqScanOff)
}

// ExplainToken runs the unindexed lookup-key predicate.
func (s *Store) ExplainToken(ctx context.Context, token string, iterations int) (Plan, error) {
	query := `SELECT count(*) FROM jobs WHERE token = $1`
	return s.explain(ctx, "token", query, []any{token}, iterations, false)
}

func (s *Store) explain(ctx context.Context, name, query string, args []any, iterations int, seqScanOff bool) (Plan, error) {
	if iterations < 1 {
		iterations = 1
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Plan{}, err
	}
	defer tx.Rollback(ctx)
	if seqScanOff {
		if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			return Plan{}, err
		}
	}
	explainSQL := `EXPLAIN (ANALYZE, FORMAT TEXT) ` + query
	rows, err := tx.Query(ctx, explainSQL, args...)
	if err != nil {
		return Plan{}, err
	}
	var text string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			return Plan{}, err
		}
		if text != "" {
			text += "\n"
		}
		text += line
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Plan{}, err
	}
	var total time.Duration
	for i := 0; i < iterations; i++ {
		start := time.Now()
		var n int
		if err := tx.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			return Plan{}, err
		}
		total += time.Since(start)
	}
	if err := tx.Commit(ctx); err != nil {
		return Plan{}, err
	}
	return Plan{
		Name: name, Plan: text, QueryMean: total / time.Duration(iterations),
		Iterations: iterations, SeqScanOff: seqScanOff,
	}, nil
}

// Stop closes the pool and the embedded server, then removes the data directory.
func (s *Store) Stop() error {
	if s == nil {
		return nil
	}
	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
	var err error
	if s.emb != nil {
		err = s.emb.Stop()
		s.emb = nil
	}
	if s.runtimeDir != "" {
		if rm := os.RemoveAll(s.runtimeDir); err == nil {
			err = rm
		}
		s.runtimeDir = ""
	}
	if s.dataDir != "" {
		if rm := os.RemoveAll(s.dataDir); err == nil {
			err = rm
		}
		s.dataDir = ""
	}
	return err
}
