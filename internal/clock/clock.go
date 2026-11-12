// Package clock is an injectable UTC clock for token expiry.
package clock

import (
	"sync"
	"time"
)

type Clock struct {
	mu  sync.Mutex
	now time.Time
}

func New(now time.Time) *Clock { return &Clock{now: now.UTC()} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}
