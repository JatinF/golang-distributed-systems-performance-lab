package fault

import (
	"testing"
	"time"
)

func TestLossDelayAndDuplicate(t *testing.T) {
	var inj Injector
	inj.SetLossEvery(1)
	delay, drop, dup := inj.Observe()
	if delay != 0 || !drop || dup {
		t.Fatalf("loss %+v", []any{delay, drop, dup})
	}
	if inj.Dropped() != 1 {
		t.Fatal(inj.Dropped())
	}
	inj.Clear()
	inj.SetDelay(40 * time.Millisecond)
	inj.SetDuplicate(true)
	delay, drop, dup = inj.Observe()
	if delay != 40*time.Millisecond || drop || !dup {
		t.Fatalf("delay %+v", []any{delay, drop, dup})
	}
}
