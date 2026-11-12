// Package pool is a bounded worker pool.
// Submit waits on a buffered channel. A full queue plus a deadline returns backpressure.
package pool

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
)

type Handler func(context.Context, model.Job) model.Outcome

type ticket struct {
	ctx  context.Context
	job  model.Job
	done chan model.Outcome
}

type Pool struct {
	queue     chan *ticket
	handler   Handler
	accepting atomic.Bool
	mu        sync.Mutex
	idle      sync.Cond
	inflight  int
	accepted  uint64
	rejected  uint64
	closed    chan struct{}
	once      sync.Once
	wg        sync.WaitGroup
}

func New(workers, queue int, handler Handler) *Pool {
	if workers < 1 {
		workers = 1
	}
	if queue < 1 {
		queue = 1
	}
	p := &Pool{
		queue:   make(chan *ticket, queue),
		handler: handler,
		closed:  make(chan struct{}),
	}
	p.idle.L = &p.mu
	p.accepting.Store(true)
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *Pool) Submit(ctx context.Context, job model.Job) model.Outcome {
	t := &ticket{ctx: ctx, job: job, done: make(chan model.Outcome, 1)}
	p.mu.Lock()
	if !p.accepting.Load() {
		p.rejected++
		p.mu.Unlock()
		return model.Outcome{ID: job.ID, State: "shutting_down", Reason: "shutting_down"}
	}
	p.inflight++
	p.mu.Unlock()

	select {
	case p.queue <- t:
	case <-ctx.Done():
		p.finish()
		p.reject()
		return model.Outcome{ID: job.ID, State: waitReason(ctx), Reason: waitReason(ctx)}
	case <-p.closed:
		p.finish()
		return model.Outcome{ID: job.ID, State: "shutting_down", Reason: "shutting_down"}
	}

	select {
	case out := <-t.done:
		return out
	case <-ctx.Done():
		reason := "cancelled"
		if ctx.Err() == context.DeadlineExceeded {
			reason = "deadline_exceeded"
		}
		return model.Outcome{ID: job.ID, State: reason, Reason: reason}
	}
}

func waitReason(ctx context.Context) string {
	if ctx.Err() == context.DeadlineExceeded {
		return "backpressure"
	}
	return "cancelled"
}

func (p *Pool) reject() {
	p.mu.Lock()
	p.rejected++
	p.mu.Unlock()
}

func (p *Pool) finish() {
	p.mu.Lock()
	p.inflight--
	if p.inflight == 0 {
		p.idle.Broadcast()
	}
	p.mu.Unlock()
}

func (p *Pool) Inflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inflight
}

func (p *Pool) QueueLen() int { return len(p.queue) }

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.closed:
			for {
				select {
				case t := <-p.queue:
					p.run(t)
				default:
					return
				}
			}
		case t := <-p.queue:
			p.run(t)
		}
	}
}

func (p *Pool) run(t *ticket) {
	out := model.Outcome{ID: t.job.ID, State: "cancelled", Reason: "cancelled"}
	if t.ctx.Err() == nil {
		out = p.handler(t.ctx, t.job)
		if out.ID == "" {
			out.ID = t.job.ID
		}
	}
	t.done <- out
	p.mu.Lock()
	if out.Accepted {
		p.accepted++
	}
	p.mu.Unlock()
	p.finish()
}

// Shutdown stops admission and waits until accepted work has finished.
// The job queue is left open. Workers exit when closed is closed.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.accepting.Store(false)
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.idle.Broadcast()
			p.mu.Unlock()
		case <-stop:
		}
	}()
	for p.inflight > 0 {
		if err := ctx.Err(); err != nil {
			close(stop)
			p.mu.Unlock()
			return err
		}
		p.idle.Wait()
	}
	close(stop)
	p.once.Do(func() { close(p.closed) })
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

func (p *Pool) Accepted() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

func (p *Pool) Rejected() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rejected
}
