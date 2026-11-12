package lab

import (
	"context"
	"testing"
	"time"
)

func TestDemoReasons(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	l, report, err := Demo(ctx)
	if l != nil {
		t.Cleanup(l.Close)
	}
	if err != nil {
		t.Fatal(err)
	}
	if report.Price != "none" || report.Tenant != "SYN-TENANT-QUORUM" {
		t.Fatalf("report %+v", report.Tenant)
	}
	if len(report.Gates) == 0 {
		t.Fatal("gates empty")
	}
	for _, step := range report.Steps {
		if !step.OK {
			t.Errorf("%s reason %s detail %s", step.Name, step.Reason, step.Detail)
		}
	}
}
