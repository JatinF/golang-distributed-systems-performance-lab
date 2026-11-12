package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/clock"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ids"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/lab"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

func TestHealthGatesAndJobs(t *testing.T) {
	clk := clock.New(time.Date(2026, 11, 12, 16, 0, 0, 0, time.UTC))
	service, err := lab.Wire(clk, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	token, err := service.Token(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	handler := New(service)
	health := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	handler.ServeHTTP(health, req)
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"price":"none"`) {
		t.Fatal(health.Body.String())
	}
	gates := httptest.NewRecorder()
	handler.ServeHTTP(gates, httptest.NewRequest(http.MethodGet, "/v1/gates", nil))
	if gates.Code != http.StatusOK || strings.Contains(gates.Body.String(), "secret-value") {
		t.Fatal(gates.Body.String())
	}
	body, _ := json.Marshal(map[string]string{"id": "SYN-JOB-HTTP", "tenant_id": ids.Tenant, "kind": "light", "payload": "hello"})
	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body)))
	if missing.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", missing.Code)
	}
	authed := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	authed.Header.Set("Authorization", "Bearer "+token)
	ok := httptest.NewRecorder()
	handler.ServeHTTP(ok, authed)
	if ok.Code != http.StatusOK {
		t.Fatal(ok.Body.String())
	}
	var out model.Outcome
	if err := json.Unmarshal(ok.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Reason != reason.Done {
		t.Fatal(out.Reason)
	}
	if RedactedAuthorization("Bearer "+token) != "Bearer [redacted]" {
		t.Fatal("redaction")
	}
}
