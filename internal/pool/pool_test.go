package pool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

func TestCompletesWork(t *testing.T) {
	p := New(4, 4, func(ctx context.Context, job model.Job) model.Outcome {
		return model.Outcome{ID: job.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := p.Submit(context.Background(), model.Job{ID: "j"})
			if out.Reason != reason.Done {
				t.Errorf("reason %s", out.Reason)
			}
		}()
	}
	wg.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := p.Submit(context.Background(), model.Job{ID: "late"})
	if out.Reason != reason.ShuttingDown {
		t.Fatalf("reason %s", out.Reason)
	}
	if p.Accepted() != 40 {
		t.Fatalf("accepted %d", p.Accepted())
	}
}

func TestBackpressureCancelAndDeadline(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	p := New(1, 1, func(ctx context.Context, job model.Job) model.Outcome {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return model.Outcome{ID: job.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	go p.Submit(context.Background(), model.Job{ID: "block"})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	go p.Submit(context.Background(), model.Job{ID: "queued"})
	deadline := time.Now().Add(time.Second)
	for p.QueueLen() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.QueueLen() != 1 {
		t.Fatal("queue did not fill")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	blocked := p.Submit(ctx, model.Job{ID: "third"})
	cancel()
	if blocked.Reason != reason.Backpressure {
		t.Fatalf("backpressure got %s", blocked.Reason)
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	out := p.Submit(cancelled, model.Job{ID: "cancel"})
	if out.Reason != reason.Cancelled {
		t.Fatalf("cancel got %s", out.Reason)
	}
	close(release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	hold := make(chan struct{})
	slow := New(1, 1, func(ctx context.Context, job model.Job) model.Outcome {
		<-hold
		return model.Outcome{ID: job.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
	late := slow.Submit(short, model.Job{ID: "late"})
	cancelShort()
	close(hold)
	if late.Reason != reason.DeadlineExceeded {
		t.Fatalf("deadline got %s", late.Reason)
	}
	_ = slow.Shutdown(context.Background())
}
