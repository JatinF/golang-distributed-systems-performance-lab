package bench

import (
	"testing"
	"time"
)

func TestWorkerSaturationRule(t *testing.T) {
	rows := []Experiment{
		{Name: "worker-heavy", Completed: 10, Offered: 10, AchievedPerSec: 100},
		{Name: "worker-heavy", Completed: 10, Offered: 10, AchievedPerSec: 700},
		{Name: "worker-heavy", Completed: 10, Offered: 10, AchievedPerSec: 720},
		{Name: "worker-heavy", Completed: 10, Offered: 10, AchievedPerSec: 400},
	}
	MarkWorkerSaturation(rows, "worker-heavy")
	if rows[0].Saturated || rows[0].Reason != "baseline" {
		t.Fatalf("baseline %+v", rows[0])
	}
	if rows[1].Saturated || rows[1].Reason != "throughput increased" {
		t.Fatalf("increased %+v", rows[1])
	}
	if !rows[2].Saturated || rows[2].Reason != "adding workers stopped helping" {
		t.Fatalf("plateau %+v", rows[2])
	}
	if !rows[3].Saturated {
		t.Fatalf("drop %+v", rows[3])
	}
}

func TestLoadSaturationIgnoresZeroWall(t *testing.T) {
	if LoadSaturated(1000, 1000, 0, false, 0, 0, 1000) {
		t.Fatal("a zero wall clock with every request completed is quantization")
	}
	if !LoadSaturated(100, 1000, 0, false, time.Second, 100, 1000) {
		t.Fatal("short completion should saturate")
	}
	if LoadSaturated(10000, 10000, 0, false, time.Second, 20000, 10000) {
		t.Fatal("a burst above the target rate should not saturate")
	}
}
