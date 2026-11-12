// Package profile captures pprof data and returns a text summary.
package profile

import (
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
)

// Summary is the text top of the profiles captured around one run.
type Summary struct {
	CPUTop        string `json:"cpu_top"`
	HeapTop       string `json:"heap_top"`
	AllocTop      string `json:"alloc_top"`
	GoroutineTop  string `json:"goroutine_top"`
	Goroutines    int    `json:"goroutines"`
	HeapAlloc     uint64 `json:"heap_alloc"`
	TotalAlloc    uint64 `json:"total_alloc"`
	NumGC         uint32 `json:"num_gc"`
	CPUProfileErr string `json:"cpu_profile_err,omitempty"`
}

// Capture runs fn under a CPU profile and then writes heap, alloc, and goroutine profiles.
// Profile files are removed after the text summary is read.
func Capture(fn func()) (Summary, error) {
	var mem runtime.MemStats
	cpu, err := os.CreateTemp("", "syn-perf-cpu-*.pprof")
	var cpuErr string
	if err != nil {
		cpuErr = err.Error()
	} else {
		if err := pprof.StartCPUProfile(cpu); err != nil {
			cpuErr = err.Error()
			cpu.Close()
			os.Remove(cpu.Name())
			cpu = nil
		}
	}
	fn()
	if cpu != nil {
		pprof.StopCPUProfile()
		cpu.Close()
	}
	runtime.ReadMemStats(&mem)
	heap, heapErr := writeProfile("heap")
	alloc, allocErr := writeProfile("allocs")
	goro, goroErr := writeProfile("goroutine")
	summary := Summary{
		Goroutines:    runtime.NumGoroutine(),
		HeapAlloc:     mem.HeapAlloc,
		TotalAlloc:    mem.TotalAlloc,
		NumGC:         mem.NumGC,
		CPUProfileErr: cpuErr,
	}
	if cpu != nil {
		summary.CPUTop, _ = Top(cpu.Name())
		os.Remove(cpu.Name())
	}
	summary.HeapTop = textOrErr(heap, heapErr)
	summary.AllocTop = textOrErr(alloc, allocErr)
	summary.GoroutineTop = textOrErr(goro, goroErr)
	return summary, nil
}

func writeProfile(name string) (string, error) {
	f, err := os.CreateTemp("", "syn-perf-"+name+"-*.pprof")
	if err != nil {
		return "", err
	}
	path := f.Name()
	prof := pprof.Lookup(name)
	if prof == nil {
		f.Close()
		os.Remove(path)
		return "", os.ErrNotExist
	}
	err = prof.WriteTo(f, 0)
	f.Close()
	if err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

func textOrErr(path string, err error) string {
	if err != nil {
		return err.Error()
	}
	text, readErr := Top(path)
	os.Remove(path)
	if readErr != nil {
		return readErr.Error() + "\n" + text
	}
	return text
}

// Top runs go tool pprof -top on a profile.
func Top(path string) (string, error) {
	cmd := exec.Command("go", "tool", "pprof", "-top", "-nodecount=12", path)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
