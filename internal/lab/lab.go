// Package lab wires the pool, identity, cache, database, and JetStream server.
package lab

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/apperr"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/cache"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/clock"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/gates"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/identity"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ids"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ledger"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/pool"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/store"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/stream"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/work"
)

// Lab is the in-process performance lab.
type Lab struct {
	Clock       *clock.Clock
	Issuer      *identity.Issuer
	Pool        *pool.Pool
	Ledger      *ledger.Ledger
	Cache       *cache.Cache
	Store       *store.Store
	StoreErr    string
	Bus         *stream.Bus
	BusErr      string
	BusDir      string
	Gates       []gates.Gate
	HeavyRounds int
}

// Wire builds the identity, ledger, and worker pool. It does not start cache,
// PostgreSQL, or NATS.
func Wire(clk *clock.Clock, workers, queue int) (*Lab, error) {
	if clk == nil {
		clk = clock.New(time.Now().UTC())
	}
	issuer, err := identity.NewIssuer(clk)
	if err != nil {
		return nil, err
	}
	l := &Lab{
		Clock:       clk,
		Issuer:      issuer,
		Ledger:      ledger.New(),
		Gates:       gates.Evaluate(Environ()),
		HeavyRounds: 4096,
	}
	l.Pool = pool.New(workers, queue, l.execute)
	return l, nil
}

// Open starts the in-process cache and attempts embedded NATS and PostgreSQL.
// A failed embed is stored on the lab and is not replaced with a map.
func Open(ctx context.Context, clk *clock.Clock, workers, queue int) (*Lab, error) {
	l, err := Wire(clk, workers, queue)
	if err != nil {
		return nil, err
	}
	c, err := cache.Start()
	if err != nil {
		l.Close()
		return nil, err
	}
	l.Cache = c
	dir, err := os.MkdirTemp("", "syn-perf-lab-nats-")
	if err != nil {
		l.Close()
		return nil, err
	}
	l.BusDir = dir
	bus, err := stream.Start(ctx, dir)
	if err != nil {
		l.BusErr = err.Error()
	} else {
		l.Bus = bus
	}
	st, err := store.Start(ctx)
	if err != nil {
		l.StoreErr = err.Error()
	} else {
		l.Store = st
	}
	return l, nil
}

// Environ reads the process environment. Values are not logged by this package.
func Environ() map[string]string {
	out := make(map[string]string)
	for _, pair := range os.Environ() {
		key, value, ok := strings.Cut(pair, "=")
		if ok {
			out[key] = value
		}
	}
	return out
}

// Token mints a lab token for the synthetic worker. The key stays in memory.
func (l *Lab) Token(ttl time.Duration) (string, error) {
	return l.Issuer.Issue(ids.Worker, ids.Audience, ids.Tenant, ttl)
}

// Handle authenticates the job and applies it once.
func (l *Lab) Handle(ctx context.Context, job model.Job) model.Outcome {
	if strings.TrimSpace(job.ID) == "" {
		return model.Outcome{State: reason.Rejected, Reason: reason.Rejected}
	}
	if job.Token == "" {
		return model.Outcome{ID: job.ID, State: reason.Unauthenticated, Reason: reason.Unauthenticated}
	}
	claims, err := l.Issuer.ParseFor(job.Token, ids.Audience, "")
	if err != nil {
		code := reason.TokenInvalid
		if ae, ok := apperr.As(err); ok {
			code = ae.Code
		}
		return model.Outcome{ID: job.ID, State: code, Reason: code}
	}
	if claims.TenantID != ids.Tenant {
		return model.Outcome{ID: job.ID, State: reason.TokenInvalid, Reason: reason.TokenInvalid}
	}
	if job.TenantID == "" {
		job.TenantID = claims.TenantID
	}
	if job.TenantID != claims.TenantID {
		return model.Outcome{ID: job.ID, State: reason.TokenInvalid, Reason: reason.TokenInvalid}
	}
	if job.Kind == "" {
		job.Kind = "light"
	}
	return l.Ledger.Do(job.ID, func() model.Outcome {
		return l.Pool.Submit(ctx, job)
	})
}

func (l *Lab) execute(ctx context.Context, job model.Job) model.Outcome {
	if err := ctx.Err(); err != nil {
		code := reason.Cancelled
		if err == context.DeadlineExceeded {
			code = reason.DeadlineExceeded
		}
		return model.Outcome{ID: job.ID, State: code, Reason: code}
	}
	rounds := roundsFor(job.Kind, l.HeavyRounds)
	sum := work.Digest(job.Payload, rounds)
	if l.Store != nil && l.Store.Started() {
		_, won, err := l.Store.Complete(ctx, job.ID, job.TenantID, job.Kind, job.Payload, "", sum)
		if err != nil {
			return model.Outcome{ID: job.ID, State: reason.StoreUnavailable, Reason: reason.StoreUnavailable}
		}
		if !won {
			return model.Outcome{ID: job.ID, Accepted: true, State: reason.Duplicate, Reason: reason.Duplicate, Result: sum}
		}
	}
	if l.Cache != nil {
		_ = l.Cache.Set(ctx, "result:"+job.ID, sum)
	}
	return model.Outcome{ID: job.ID, Accepted: true, State: reason.Done, Reason: reason.Done, Result: sum}
}

func roundsFor(kind string, heavy int) int {
	if strings.HasPrefix(kind, "rounds:") {
		n, err := strconv.Atoi(strings.TrimPrefix(kind, "rounds:"))
		if err == nil && n > 0 {
			return n
		}
	}
	if kind == "heavy" {
		if heavy < 1 {
			return 4096
		}
		return heavy
	}
	return 1
}

// Close stops workers and in-process servers.
func (l *Lab) Close() {
	if l == nil {
		return
	}
	if l.Pool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = l.Pool.Shutdown(ctx)
		cancel()
	}
	if l.Cache != nil {
		l.Cache.Close()
	}
	if l.Bus != nil {
		l.Bus.Close()
	}
	if l.Store != nil {
		_ = l.Store.Stop()
	}
	if l.BusDir != "" {
		_ = os.RemoveAll(l.BusDir)
	}
}
