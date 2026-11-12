package ledger

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

func TestDoOnce(t *testing.T) {
	led := New()
	var calls atomic.Int32
	var wg sync.WaitGroup
	results := make([]model.Outcome, 32)
	for i := 0; i < len(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = led.Do("job", func() model.Outcome {
				calls.Add(1)
				return model.Outcome{ID: "job", Accepted: true, State: reason.Done, Reason: reason.Done}
			})
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("calls %d", calls.Load())
	}
	if led.Count() != 1 {
		t.Fatalf("count %d", led.Count())
	}
	dups := 0
	done := 0
	for _, out := range results {
		if out.Reason == reason.Duplicate {
			dups++
		}
		if out.Reason == reason.Done {
			done++
		}
	}
	if done != 1 || dups != len(results)-1 {
		t.Fatalf("done %d duplicate %d", done, dups)
	}
}

func TestRetryAfterBackpressure(t *testing.T) {
	led := New()
	first := led.Do("job", func() model.Outcome {
		return model.Outcome{ID: "job", Reason: reason.Backpressure, State: reason.Backpressure}
	})
	if first.Reason != reason.Backpressure || led.Count() != 0 {
		t.Fatalf("first %+v count %d", first, led.Count())
	}
	second := led.Do("job", func() model.Outcome {
		return model.Outcome{ID: "job", Accepted: true, Reason: reason.Done, State: reason.Done}
	})
	if second.Reason != reason.Done || led.Count() != 1 {
		t.Fatalf("second %+v", second)
	}
}
