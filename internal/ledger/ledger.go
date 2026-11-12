// Package ledger applies a job id once. A second caller receives the first outcome.
package ledger

import (
	"sync"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

type flight struct{ done chan struct{} }

type Ledger struct {
	mu       sync.Mutex
	done     map[string]model.Outcome
	inflight map[string]*flight
}

func New() *Ledger {
	return &Ledger{done: map[string]model.Outcome{}, inflight: map[string]*flight{}}
}

// Do runs fn once for id. A non-success result is not remembered, so a later
// attempt can retry. Concurrent callers wait. If the leader commits, they
// receive a duplicate outcome.
func (l *Ledger) Do(id string, fn func() model.Outcome) model.Outcome {
	l.mu.Lock()
	if out, ok := l.done[id]; ok {
		l.mu.Unlock()
		return duplicate(out)
	}
	if f, ok := l.inflight[id]; ok {
		l.mu.Unlock()
		<-f.done
		l.mu.Lock()
		out, ok := l.done[id]
		l.mu.Unlock()
		if !ok {
			return l.Do(id, fn)
		}
		return duplicate(out)
	}
	f := &flight{done: make(chan struct{})}
	l.inflight[id] = f
	l.mu.Unlock()

	out := fn()
	commit := out.Reason == reason.Done

	l.mu.Lock()
	if commit {
		l.done[id] = out
	}
	delete(l.inflight, id)
	close(f.done)
	l.mu.Unlock()
	return out
}

func (l *Ledger) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.done)
}

func duplicate(out model.Outcome) model.Outcome {
	out.Accepted = true
	out.State = reason.Duplicate
	out.Reason = reason.Duplicate
	return out
}
