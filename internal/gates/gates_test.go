package gates

import (
	"strings"
	"testing"
)

func TestUnsetIsNotRunAndSetIsBlocked(t *testing.T) {
	idle := Evaluate(map[string]string{})
	if len(idle) != 7 {
		t.Fatalf("gates %d", len(idle))
	}
	for _, gate := range idle {
		if gate.Status != "not-run" {
			t.Fatalf("%s %s", gate.Name, gate.Status)
		}
	}
	secret := "postgres://lab:secret-value@remote/db"
	busy := Evaluate(map[string]string{"LAB_POSTGRES_URL": secret})
	for _, gate := range busy {
		if strings.Contains(gate.Reason, secret) || strings.Contains(gate.Reason, "secret-value") {
			t.Fatalf("value leaked in %s", gate.Name)
		}
		if gate.Name == "postgres-remote" && gate.Status != "blocked" {
			t.Fatalf("status %s", gate.Status)
		}
	}
}
