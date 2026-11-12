package identity

import (
	"strings"
	"testing"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/apperr"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/clock"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ids"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

func TestIssueAndParse(t *testing.T) {
	clk := clock.New(time.Date(2026, 11, 12, 16, 0, 0, 0, time.UTC))
	issuer, err := NewIssuer(clk)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := issuer.Issue(ids.Worker, ids.Audience, ids.Tenant, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := issuer.ParseFor(raw, ids.Audience, ids.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Subject != ids.Worker || claims.TenantID != ids.Tenant {
		t.Fatalf("claims %+v", claims)
	}
	clk.Advance(2 * time.Hour)
	_, err = issuer.Parse(raw)
	requireCode(t, err, reason.TokenExpired)
}

func TestRejectsBadSignatureAndAudience(t *testing.T) {
	clk := clock.New(time.Date(2026, 11, 12, 16, 0, 0, 0, time.UTC))
	issuer, err := NewIssuer(clk)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := issuer.Issue(ids.Worker, ids.Audience, ids.Tenant, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(raw, ".")
	bad := parts[0] + "." + parts[1] + ".AAAA"
	_, err = issuer.Parse(bad)
	requireCode(t, err, reason.SignatureInvalid)
	other, err := issuer.Issue(ids.Worker, "lab://other", ids.Tenant, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, err = issuer.ParseFor(other, ids.Audience, ids.Tenant)
	requireCode(t, err, reason.AudienceMismatch)
	_, err = issuer.Parse("not-a-token")
	requireCode(t, err, reason.TokenInvalid)
}

func TestBearer(t *testing.T) {
	if got := Bearer("Bearer secret-value"); got != "secret-value" {
		t.Fatal(got)
	}
	if Bearer("basic secret-value") != "" {
		t.Fatal("scheme should not match")
	}
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	ae, ok := apperr.As(err)
	if !ok || ae.Code != code {
		t.Fatalf("got %v want %s", err, code)
	}
}
