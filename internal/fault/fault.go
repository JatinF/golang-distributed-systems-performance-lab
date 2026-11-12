// Package fault injects loss, delay, and duplicate delivery at a publish boundary.
package fault

import "sync"
import "time"

type Injector struct {
	mu        sync.Mutex
	lossEvery int
	seen      int
	delay     time.Duration
	duplicate bool
	dropped   int
}

func (i *Injector) SetLossEvery(n int) {
	i.mu.Lock()
	i.lossEvery = n
	i.mu.Unlock()
}

func (i *Injector) SetDelay(d time.Duration) {
	i.mu.Lock()
	i.delay = d
	i.mu.Unlock()
}

func (i *Injector) SetDuplicate(on bool) {
	i.mu.Lock()
	i.duplicate = on
	i.mu.Unlock()
}

func (i *Injector) Clear() {
	i.mu.Lock()
	i.lossEvery = 0
	i.delay = 0
	i.duplicate = false
	i.mu.Unlock()
}

// Observe returns the delay to apply, whether this attempt is dropped, and whether a second copy should be sent.
func (i *Injector) Observe() (time.Duration, bool, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.seen++
	drop := i.lossEvery > 0 && i.seen%i.lossEvery == 0
	if drop {
		i.dropped++
	}
	return i.delay, drop, i.duplicate
}

func (i *Injector) Dropped() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.dropped
}
