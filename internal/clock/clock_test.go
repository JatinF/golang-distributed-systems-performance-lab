package clock

import (
	"testing"
	"time"
)

func TestAdvance(t *testing.T) {
	start := time.Date(2026, 11, 12, 16, 0, 0, 0, time.UTC)
	clk := New(start)
	clk.Advance(time.Hour)
	if !clk.Now().Equal(start.Add(time.Hour)) {
		t.Fatal(clk.Now())
	}
}
