// Package work is the CPU-bound job body used by the pool and the benchmarks.
package work

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/pool"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

// Digest hashes payload rounds times. One round is a single SHA-256.
func Digest(payload []byte, rounds int) string {
	if rounds < 1 {
		rounds = 1
	}
	sum := sha256.Sum256(payload)
	for i := 1; i < rounds; i++ {
		sum = sha256.Sum256(sum[:])
	}
	return hex.EncodeToString(sum[:])
}

// Handler returns a pool handler that does rounds of SHA-256.
func Handler(rounds int) pool.Handler {
	return func(ctx context.Context, job model.Job) model.Outcome {
		if err := ctx.Err(); err != nil {
			return cancelled(job.ID, err)
		}
		return model.Outcome{
			ID: job.ID, Accepted: true, State: reason.Done, Reason: reason.Done,
			Result: Digest(job.Payload, rounds),
		}
	}
}

func cancelled(id string, err error) model.Outcome {
	code := reason.Cancelled
	if errors.Is(err, context.DeadlineExceeded) {
		code = reason.DeadlineExceeded
	}
	return model.Outcome{ID: id, State: code, Reason: code}
}
