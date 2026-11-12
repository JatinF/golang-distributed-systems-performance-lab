package cache

import (
	"context"
	"testing"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

func TestHitMissInvalidateAndRecover(t *testing.T) {
	c, err := Start()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if _, code := c.Get(ctx, "job"); code != reason.CacheMiss {
		t.Fatal(code)
	}
	if err := c.Set(ctx, "job", "value"); err != nil {
		t.Fatal(err)
	}
	got, code := c.Get(ctx, "job")
	if code != reason.CacheHit || got != "value" {
		t.Fatalf("%s %s", got, code)
	}
	if code := c.Invalidate(ctx, "job"); code != reason.CacheInvalidated {
		t.Fatal(code)
	}
	if _, code := c.Get(ctx, "job"); code != reason.CacheMiss {
		t.Fatal(code)
	}
	c.Stop()
	if _, code := c.Get(ctx, "job"); code != reason.CacheUnavailable {
		t.Fatal(code)
	}
	recovered, err := c.Recover()
	if err != nil || recovered != reason.CacheRecovered {
		t.Fatal(recovered, err)
	}
	if _, code := c.Get(ctx, "job"); code != reason.CacheMiss {
		t.Fatal(code)
	}
}
