package store

import (
	"context"
	"testing"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ids"
)

func TestConcurrentCompleteAndPlans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	st, err := Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Stop() })
	type result struct {
		n   int
		won bool
		err error
	}
	ch := make(chan result, 8)
	for i := 0; i < 8; i++ {
		go func() {
			n, won, err := st.Complete(ctx, "SYN-JOB-PG", ids.Tenant, "light", []byte("x"), "", "x")
			ch <- result{n: n, won: won, err: err}
		}()
	}
	winners := 0
	applies := 0
	for i := 0; i < 8; i++ {
		item := <-ch
		if item.err != nil {
			t.Fatal(item.err)
		}
		if item.won {
			winners++
		}
		if item.n > applies {
			applies = item.n
		}
	}
	if winners != 1 || applies != 1 {
		t.Fatalf("winners %d applies %d", winners, applies)
	}
	if err := st.SeedLookup(ctx, 200); err != nil {
		t.Fatal(err)
	}
	indexed, err := st.ExplainTenantKind(ctx, "SYN-TENANT-1", "kind-1", 5, false)
	if err != nil {
		t.Fatal(err)
	}
	unindexed, err := st.ExplainToken(ctx, "tok-1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if indexed.Plan == "" || unindexed.Plan == "" {
		t.Fatal("missing plan")
	}
}
