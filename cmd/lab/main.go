package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/bench"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/clock"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/gates"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/httpapi"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/lab"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/rpc"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "health":
		fmt.Println(`{"status":"ok","service":"golang-distributed-systems-performance-lab","price":"none"}`)
	case "check":
		if err := check(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "demo":
		if err := demo(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "bench":
		if err := runBench(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	case "serve":
		if err := serve(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: lab [health|check|demo|bench|serve]")
}

func check() error {
	body := map[string]any{
		"go":        runtime.Version(),
		"price":     "none",
		"gates":     gates.Evaluate(lab.Environ()),
		"service":   "golang-distributed-systems-performance-lab",
		"remote":    "not contacted",
		"benchmark": "go run ./cmd/lab bench writes reports/bench_results.json",
	}
	return printJSON(body)
}

func demo() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	l, report, err := lab.Demo(ctx)
	if l != nil {
		defer l.Close()
	}
	if err != nil {
		return err
	}
	if err := writeJSON("reports/demo_results.json", report); err != nil {
		return err
	}
	failed := 0
	for _, step := range report.Steps {
		state := "ok"
		if !step.OK {
			state = "FAIL"
			failed++
		}
		fmt.Printf("%s %s %s\n", state, step.Name, step.Reason)
	}
	if failed > 0 {
		return fmt.Errorf("%d demo steps failed", failed)
	}
	fmt.Println("demo wrote reports/demo_results.json")
	return nil
}

func runBench() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	report, err := bench.Run(ctx)
	if err != nil {
		return err
	}
	if err := writeJSON("reports/bench_results.json", report); err != nil {
		return err
	}
	fmt.Printf("bench wrote reports/bench_results.json experiments=%d price=%s\n", len(report.Experiments), report.Price)
	return nil
}

func serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	l, err := lab.Open(ctx, clock.New(time.Now().UTC()), 8, 64)
	if err != nil {
		return err
	}
	defer l.Close()
	lis, gs, err := rpc.Listen(l)
	if err != nil {
		return err
	}
	defer gs.Stop()
	fmt.Printf("grpc %s\n", lis.Addr().String())
	server := &http.Server{Addr: "127.0.0.1:8096", Handler: httpapi.New(l), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shut)
	}()
	fmt.Println("http 127.0.0.1:8096")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func writeJSON(path string, value any) error {
	if err := os.MkdirAll("reports", 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return os.WriteFile(path, body, 0o644)
}

func printJSON(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}
