package bench

import (
	"math"
	"sort"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/gates"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/profile"
)

// Experiment is one attempted configuration and the measured outcome.
type Experiment struct {
	Name           string         `json:"name"`
	Config         string         `json:"config"`
	Offered        int            `json:"offered"`
	Completed      int            `json:"completed"`
	AchievedPerSec float64        `json:"achieved_per_sec"`
	P50Us          float64        `json:"p50_us"`
	P95Us          float64        `json:"p95_us"`
	SampleMeanUs   float64        `json:"sample_mean_us"`
	ZeroSamples    int            `json:"zero_samples"`
	LatencyMethod  string         `json:"latency_method"`
	Saturated      bool           `json:"saturated"`
	Failed         bool           `json:"failed"`
	Reason         string         `json:"reason"`
	Notes          string         `json:"notes,omitempty"`
	Reasons        map[string]int `json:"reasons,omitempty"`
	Plan           string         `json:"plan,omitempty"`
}

// Report is the workstation benchmark record.
type Report struct {
	Go          string          `json:"go"`
	OS          string          `json:"os"`
	Arch        string          `json:"arch"`
	CPUs        int             `json:"cpus"`
	GOMAXPROCS  int             `json:"gomaxprocs"`
	Processor   string          `json:"processor"`
	Price       string          `json:"price"`
	Measurement string          `json:"measurement"`
	Store       string          `json:"store"`
	Bus         string          `json:"bus"`
	Cache       string          `json:"cache"`
	Experiments []Experiment    `json:"experiments"`
	Profile     profile.Summary `json:"profile"`
	Gates       []gates.Gate    `json:"gates"`
}

func round3(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*1000) / 1000
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	rank := int(math.Ceil((p/100)*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func summarize(samples []time.Duration) (p50, p95, mean float64, zeros int) {
	if len(samples) == 0 {
		return 0, 0, 0, 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50 = float64(percentile(sorted, 50).Nanoseconds()) / 1000
	p95 = float64(percentile(sorted, 95).Nanoseconds()) / 1000
	var total int64
	for _, sample := range samples {
		if sample == 0 {
			zeros++
		}
		total += sample.Nanoseconds()
	}
	mean = float64(total) / 1000 / float64(len(samples))
	return p50, p95, mean, zeros
}

const quantized = "per-request samples include zero readings because the call finished inside one Windows clock tick. achieved_per_sec is elapsed completions. p50_us is the raw sample and is not a claim that the work was instant"

const perRequest = "per-request wall time"

func latencyMethod(samples int, zeros int) string {
	if samples > 0 && zeros*2 > samples {
		return quantized
	}
	return perRequest
}

// LoadSaturated reports whether an offered-rate attempt saturated.
// A zero wall clock with every request completed is quantization, not saturation.
func LoadSaturated(completed, offered, backpressure int, paced bool, wall time.Duration, achieved, target float64) bool {
	if completed == offered && backpressure == 0 && wall == 0 {
		return false
	}
	if offered > 0 && completed*10 < offered*9 {
		return true
	}
	if backpressure > 0 {
		return true
	}
	if !paced && wall > 0 && achieved < 0.9*target {
		return true
	}
	return false
}

// MarkWorkerSaturation flags levels that did not improve throughput by more than 10 percent.
func MarkWorkerSaturation(rows []Experiment, name string) {
	best := 0.0
	seen := false
	for i := range rows {
		if rows[i].Name != name || rows[i].Failed || rows[i].Completed != rows[i].Offered {
			continue
		}
		if !seen || rows[i].AchievedPerSec > best*1.10 {
			if !seen {
				rows[i].Reason = "baseline"
			} else {
				rows[i].Reason = "throughput increased"
			}
			seen = true
			best = rows[i].AchievedPerSec
			rows[i].Saturated = false
			continue
		}
		rows[i].Saturated = true
		rows[i].Reason = "adding workers stopped helping"
	}
}
