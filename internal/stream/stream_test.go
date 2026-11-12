package stream

import (
	"context"
	"testing"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/fault"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ledger"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

func TestPublishFetchAckAndDuplicate(t *testing.T) {
	ctx := context.Background()
	bus, err := Start(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	body, err := Encode("SYN-JOB-1", "light", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	code, err := bus.Publish(ctx, "SYN-JOB-1", body, nil)
	if err != nil || code != reason.Done {
		t.Fatalf("publish %s %v", code, err)
	}
	dup, err := bus.Publish(ctx, "SYN-JOB-1", body, nil)
	if err != nil || dup != reason.Duplicate {
		t.Fatalf("duplicate %s %v", dup, err)
	}
	msgs, err := bus.Fetch(ctx, 5, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("fetched %d", len(msgs))
	}
	led := ledger.New()
	out, err := Apply(ctx, msgs[0], led, nil)
	if err != nil || out.Reason != reason.Done {
		t.Fatalf("apply %+v %v", out, err)
	}
	if led.Count() != 1 {
		t.Fatal(led.Count())
	}
}

func TestLossDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	bus, err := Start(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	var inj fault.Injector
	inj.SetLossEvery(1)
	body, _ := Encode("SYN-JOB-LOSS", "light", []byte("x"))
	code, err := bus.Publish(ctx, "SYN-JOB-LOSS", body, &inj)
	if err != nil || code != reason.Lost {
		t.Fatalf("%s %v", code, err)
	}
	msgs, err := bus.Fetch(ctx, 1, 300*time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("fetched %d", len(msgs))
	}
}

func TestReplayAfterNak(t *testing.T) {
	ctx := context.Background()
	bus, err := Start(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	body, _ := Encode("SYN-JOB-REPLAY", "light", []byte("x"))
	if _, err := bus.Publish(ctx, "SYN-JOB-REPLAY", body, nil); err != nil {
		t.Fatal(err)
	}
	msgs, err := bus.Fetch(ctx, 1, time.Second, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch %d %v", len(msgs), err)
	}
	led := ledger.New()
	env, err := msgs[0].Envelope()
	if err != nil {
		t.Fatal(err)
	}
	led.Do(env.ID, func() model.Outcome {
		return model.Outcome{ID: env.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	if err := msgs[0].Nak(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var again []Msg
	for time.Now().Before(deadline) && len(again) == 0 {
		again, err = bus.Fetch(ctx, 1, 400*time.Millisecond, time.Second)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(again) != 1 {
		t.Fatal("redelivery missing")
	}
	out, err := Apply(ctx, again[0], led, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reason != reason.Replayed {
		t.Fatalf("reason %s deliveries %d", out.Reason, again[0].Deliveries())
	}
}
