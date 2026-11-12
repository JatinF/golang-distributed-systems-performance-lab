// Package httpapi is the small HTTP surface for health, gates, and job submission.
package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/identity"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/lab"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

const maxBody = 1 << 20

type jobRequest struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	Kind     string `json:"kind"`
	Payload  string `json:"payload"`
}

// New returns the HTTP handler. Logs do not include the bearer token.
func New(l *lab.Lab) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"service": "golang-distributed-systems-performance-lab",
			"price":   "none",
		})
	})
	mux.HandleFunc("GET /v1/gates", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"gates": l.Gates, "price": "none"})
	})
	mux.HandleFunc("POST /v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		var req jobRequest
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, model.Outcome{State: reason.Rejected, Reason: reason.Rejected})
			return
		}
		out := l.Handle(r.Context(), model.Job{
			ID:       req.ID,
			TenantID: req.TenantID,
			Kind:     req.Kind,
			Payload:  []byte(req.Payload),
			Token:    identity.Bearer(r.Header.Get("Authorization")),
		})
		writeJSON(w, statusFor(out.Reason), out)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

func statusFor(name string) int {
	switch name {
	case reason.Done, reason.Duplicate, reason.CacheHit, reason.Replayed, reason.Retried:
		return http.StatusOK
	case reason.Backpressure:
		return http.StatusTooManyRequests
	case reason.DeadlineExceeded:
		return http.StatusRequestTimeout
	case reason.Cancelled, reason.Rejected:
		return http.StatusBadRequest
	case reason.Unauthenticated, reason.TokenExpired, reason.TokenInvalid, reason.SignatureInvalid, reason.AudienceMismatch:
		return http.StatusUnauthorized
	case reason.ShuttingDown, reason.StoreUnavailable, reason.MessagingUnavailable, reason.CacheUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadRequest
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// RedactedAuthorization is a stand-in for logs. The raw header is not returned.
func RedactedAuthorization(header string) string {
	if strings.TrimSpace(header) == "" {
		return ""
	}
	return "Bearer [redacted]"
}

// Drain is unused by the server and kept so tests can bound a reader.
func Drain(r io.Reader) ([]byte, error) { return io.ReadAll(io.LimitReader(r, maxBody)) }
