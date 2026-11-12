package bench

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/cache"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/clock"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/fault"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/gates"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ids"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/lab"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ledger"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/pool"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/profile"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/rpc"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/rpc/labpb"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/store"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/stream"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/work"
)

// Run attempts every benchmark configuration and records what this process achieved.
func Run(ctx context.Context) (Report, error) {
	report := Report{
		Go:          runtime.Version(),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		CPUs:        runtime.NumCPU(),
		GOMAXPROCS:  runtime.GOMAXPROCS(0),
		Processor:   processor(),
		Price:       "none",
		Cache:       "in-process Redis protocol (miniredis)",
		Measurement: "Each row is an attempt on this workstation. Achieved throughput is completed work divided by wall time. Saturation and failure are outcomes of the attempt, not targets. CI runs go test and does not reproduce this matrix. Remote PostgreSQL, Redis, NATS, OIDC, Docker, and Kubernetes were not contacted.",
		Gates:       gates.Evaluate(lab.Environ()),
	}
	log("worker concurrency")
	for _, workers := range []int{1, 8, 32, 128} {
		report.Experiments = append(report.Experiments, runPool("worker-light", fmt.Sprintf("workers=%d queue=%d jobs=3000 rounds=1", workers, workers), workers, workers, 3000, 1))
	}
	for _, workers := range []int{1, 8, 32, 128} {
		report.Experiments = append(report.Experiments, runPool("worker-heavy", fmt.Sprintf("workers=%d queue=%d jobs=192 rounds=8192", workers, workers), workers, workers, 192, 8192))
	}
	MarkWorkerSaturation(report.Experiments, "worker-light")
	MarkWorkerSaturation(report.Experiments, "worker-heavy")

	log("request load")
	for _, rate := range []int{100, 1000, 10000} {
		report.Experiments = append(report.Experiments, runLoad("load-light", rate, 1))
		report.Experiments = append(report.Experiments, runLoad("load-cpu", rate, 2048))
	}

	log("grpc")
	grpcRows, err := runGRPC(ctx)
	if err != nil {
		report.Experiments = append(report.Experiments, Experiment{Name: "grpc", Config: "unary-and-streaming", Failed: true, Reason: "failed", Notes: err.Error()})
	} else {
		report.Experiments = append(report.Experiments, grpcRows...)
	}

	log("cache")
	report.Experiments = append(report.Experiments, runCache(ctx)...)

	log("messaging and failures")
	dir, err := os.MkdirTemp("", "syn-perf-lab-bench-nats-")
	if err != nil {
		return report, err
	}
	defer os.RemoveAll(dir)
	bus, err := stream.Start(ctx, dir)
	if err != nil {
		report.Bus = "not-started"
		note := err.Error()
		report.Experiments = append(report.Experiments, Experiment{Name: "messaging", Config: "embedded-nats", Reason: reason.NotStarted, Notes: note})
		report.Experiments = append(report.Experiments, Experiment{Name: "failures", Config: "embedded-nats", Reason: reason.NotStarted, Notes: note})
	} else {
		report.Bus = "embedded nats jetstream, file store, 127.0.0.1"
		defer bus.Close()
		report.Experiments = append(report.Experiments, runMessaging(ctx, bus)...)
		report.Experiments = append(report.Experiments, runFailures(ctx, bus)...)
	}

	log("database")
	st, err := store.Start(ctx)
	if err != nil {
		report.Store = "not-started"
		report.Experiments = append(report.Experiments, Experiment{Name: "database", Config: "embedded-postgresql", Reason: reason.NotStarted, Notes: err.Error()})
	} else {
		report.Store = "embedded postgresql, 127.0.0.1, sslmode=disable"
		defer st.Stop()
		report.Experiments = append(report.Experiments, runDatabase(ctx, st)...)
	}

	log("resource pressure")
	pressure, summary, err := runPressure()
	if err != nil {
		pressure.Failed = true
		pressure.Reason = "failed"
		pressure.Notes = err.Error()
	}
	report.Experiments = append(report.Experiments, pressure)
	report.Profile = summary
	return report, nil
}

func log(name string) { fmt.Fprintf(os.Stderr, "bench %s\n", name) }

func processor() string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name")
	out, err := cmd.Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(out))
}

func runPool(name, config string, workers, queue, jobs, rounds int) Experiment {
	p := pool.New(workers, queue, work.Handler(rounds))
	defer p.Shutdown(context.Background())
	outcomes := make([]model.Outcome, jobs)
	samples := make([]time.Duration, jobs)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			t0 := time.Now()
			outcomes[i] = p.Submit(ctx, model.Job{ID: fmt.Sprintf("%s-%d", name, i), Payload: []byte("payload")})
			samples[i] = time.Since(t0)
		}()
	}
	wg.Wait()
	exp := finish(name, config, outcomes, samples, time.Since(start))
	if exp.Completed != exp.Offered {
		if exp.Completed == 0 {
			exp.Failed = true
			exp.Reason = "zero completions"
		} else {
			exp.Saturated = true
			exp.Reason = "partial"
		}
	}
	return exp
}

func runLoad(name string, rate, rounds int) Experiment {
	const workers = 8
	const queue = 32
	p := pool.New(workers, queue, work.Handler(rounds))
	defer p.Shutdown(context.Background())
	n := rate
	outcomes := make([]model.Outcome, n)
	samples := make([]time.Duration, n)
	interval := time.Second / time.Duration(n)
	paced := interval >= 2*time.Millisecond
	var wg sync.WaitGroup
	launchStart := time.Now()
	for i := 0; i < n; i++ {
		if paced {
			target := launchStart.Add(time.Duration(i) * interval)
			if d := time.Until(target); d > 0 {
				time.Sleep(d)
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			t0 := time.Now()
			outcomes[i] = p.Submit(ctx, model.Job{ID: fmt.Sprintf("%s-%d-%d", name, rate, i), Payload: []byte("load")})
			samples[i] = time.Since(t0)
		}()
	}
	launch := time.Since(launchStart)
	wg.Wait()
	wall := time.Since(launchStart)
	exp := finish(name, fmt.Sprintf("target_per_sec=%d rounds=%d workers=8 queue=32 deadline=1s", rate, rounds), outcomes, samples, wall)
	bp := exp.Reasons[reason.Backpressure] + exp.Reasons[reason.DeadlineExceeded] + exp.Reasons[reason.Cancelled]
	offerRate := 0.0
	if launch > 0 {
		offerRate = float64(n) / launch.Seconds()
	}
	if launch == 0 {
		exp.Notes = fmt.Sprintf("paced=%t launch finished inside one Windows clock tick, so offer_per_sec is not computed", paced)
	} else {
		exp.Notes = fmt.Sprintf("paced=%t launch_s=%.3f offer_per_sec=%.1f", paced, launch.Seconds(), offerRate)
	}
	if wall == 0 && exp.Completed == exp.Offered && bp == 0 {
		exp.Notes += "; completion wall clock did not advance, so achieved_per_sec is not computed"
	}
	exp.Saturated = LoadSaturated(exp.Completed, exp.Offered, bp, paced, wall, exp.AchievedPerSec, float64(rate))
	if exp.Completed == 0 {
		exp.Failed = true
		exp.Reason = "zero completions"
	} else if exp.Saturated {
		exp.Reason = "saturated"
	} else {
		exp.Reason = "completed"
	}
	return exp
}

func finish(name, config string, outcomes []model.Outcome, samples []time.Duration, wall time.Duration) Experiment {
	reasons := map[string]int{}
	completed := 0
	for _, out := range outcomes {
		if out.Reason == "" {
			out.Reason = "empty"
		}
		reasons[out.Reason]++
		if out.Reason == reason.Done {
			completed++
		}
	}
	p50, p95, mean, zeros := summarize(samples)
	achieved := 0.0
	if wall > 0 {
		achieved = float64(completed) / wall.Seconds()
	}
	exp := Experiment{
		Name: name, Config: config, Offered: len(outcomes), Completed: completed,
		AchievedPerSec: round3(achieved), P50Us: round3(p50), P95Us: round3(p95),
		SampleMeanUs: round3(mean), ZeroSamples: zeros, Reasons: reasons,
		LatencyMethod: latencyMethod(len(samples), zeros), Reason: "completed",
	}
	if exp.P50Us == 0 && len(samples) > 0 {
		exp.LatencyMethod = quantized
	}
	return exp
}

func runGRPC(ctx context.Context) ([]Experiment, error) {
	l, err := lab.Wire(clock.New(time.Now().UTC()), 8, 64)
	if err != nil {
		return nil, err
	}
	defer l.Close()
	token, err := l.Token(time.Hour)
	if err != nil {
		return nil, err
	}
	lis, gs, err := rpc.Listen(l)
	if err != nil {
		return nil, err
	}
	defer gs.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := labpb.NewWorkClient(conn)
	const n = 400
	unary := runUnary(ctx, client, token, n)
	streamed := runStream(ctx, client, token, n)
	return []Experiment{unary, streamed}, nil
}

func runUnary(ctx context.Context, client labpb.WorkClient, token string, n int) Experiment {
	outcomes := make([]model.Outcome, n)
	samples := make([]time.Duration, n)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			call, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			call = metadata.AppendToOutgoingContext(call, "authorization", "Bearer "+token)
			t0 := time.Now()
			reply, err := client.Submit(call, &labpb.JobRequest{Id: fmt.Sprintf("unary-%d", i), TenantId: ids.Tenant, Kind: "light", Payload: []byte("u")})
			samples[i] = time.Since(t0)
			if err != nil {
				outcomes[i] = model.Outcome{Reason: status.Convert(err).Message()}
				return
			}
			outcomes[i] = model.Outcome{Reason: reply.GetReason(), Accepted: reply.GetAccepted()}
		}()
	}
	wg.Wait()
	exp := finish("grpc", "unary requests=400 client_goroutines=400 server_workers=8", outcomes, samples, time.Since(start))
	if exp.Completed != exp.Offered {
		exp.Failed = exp.Completed == 0
		if exp.Completed == 0 {
			exp.Reason = "zero completions"
		} else {
			exp.Reason = "partial"
		}
	}
	return exp
}

func runStream(ctx context.Context, client labpb.WorkClient, token string, n int) Experiment {
	call := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	st, err := client.Stream(call)
	if err != nil {
		return Experiment{Name: "grpc", Config: "bidi-stream", Failed: true, Reason: "failed", Notes: err.Error()}
	}
	outcomes := make([]model.Outcome, n)
	samples := make([]time.Duration, n)
	errc := make(chan error, 1)
	start := time.Now()
	go func() {
		for i := 0; i < n; i++ {
			t0 := time.Now()
			reply, err := st.Recv()
			samples[i] = time.Since(t0)
			if err != nil {
				outcomes[i] = model.Outcome{Reason: status.Convert(err).Message()}
				errc <- err
				return
			}
			outcomes[i] = model.Outcome{Reason: reply.GetReason(), Accepted: reply.GetAccepted()}
		}
		errc <- nil
	}()
	var sendErr error
	for i := 0; i < n; i++ {
		if err := st.Send(&labpb.JobRequest{Id: fmt.Sprintf("stream-%d", i), TenantId: ids.Tenant, Kind: "light", Payload: []byte("s")}); err != nil {
			sendErr = err
			break
		}
	}
	_ = st.CloseSend()
	if err := <-errc; err != nil && sendErr == nil {
		sendErr = err
	}
	exp := finish("grpc", "bidi_stream requests=400 pipelined server_workers=8", outcomes, samples, time.Since(start))
	if exp.Notes == "" {
		exp.Notes = "samples are time blocked in Recv on one pipelined stream, not an isolated round trip"
	}
	if sendErr != nil && exp.Completed != exp.Offered {
		exp.Notes = sendErr.Error()
		exp.Failed = exp.Completed == 0
		if exp.Completed == 0 {
			exp.Reason = "zero completions"
		} else {
			exp.Reason = "partial"
		}
	}
	return exp
}

func runCache(ctx context.Context) []Experiment {
	c, err := cache.Start()
	if err != nil {
		return []Experiment{{Name: "caching", Config: "miniredis", Failed: true, Reason: "failed", Notes: err.Error()}}
	}
	defer c.Close()
	rows := []Experiment{
		cacheOps(ctx, "hit", func(i int) string {
			if i == 0 {
				_ = c.Set(ctx, "hit", "v")
			}
			_, code := c.Get(ctx, "hit")
			return code
		}, reason.CacheHit),
		cacheOps(ctx, "miss", func(i int) string {
			_, code := c.Get(ctx, fmt.Sprintf("missing-%d", i))
			return code
		}, reason.CacheMiss),
		cacheOps(ctx, "invalidation", func(i int) string {
			key := fmt.Sprintf("inv-%d", i)
			if err := c.Set(ctx, key, "v"); err != nil {
				return reason.CacheUnavailable
			}
			if code := c.Invalidate(ctx, key); code != reason.CacheInvalidated {
				return code
			}
			_, code := c.Get(ctx, key)
			if code == reason.CacheMiss {
				return reason.CacheInvalidated
			}
			return code
		}, reason.CacheInvalidated),
	}
	c.Stop()
	_, down := c.Get(ctx, "hit")
	recovered, recErr := c.Recover()
	_, miss := c.Get(ctx, "hit")
	failRow := Experiment{
		Name: "caching", Config: "unavailable-then-recover", Offered: 2, LatencyMethod: perRequest,
		Reasons: map[string]int{down: 1, miss: 1},
	}
	if recErr == nil && down == reason.CacheUnavailable && recovered == reason.CacheRecovered && miss == reason.CacheMiss {
		failRow.Completed = 2
		failRow.Reason = reason.CacheRecovered
	} else {
		failRow.Failed = true
		failRow.Reason = "failed"
		if recErr != nil {
			failRow.Notes = recErr.Error()
		}
	}
	return append(rows, failRow)
}

func cacheOps(ctx context.Context, config string, fn func(int) string, want string) Experiment {
	const n = 1000
	reasons := map[string]int{}
	start := time.Now()
	samples := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		code := fn(i)
		samples[i] = time.Since(t0)
		reasons[code]++
	}
	wall := time.Since(start)
	p50, p95, mean, zeros := summarize(samples)
	completed := reasons[want]
	achieved := 0.0
	if wall > 0 {
		achieved = float64(completed) / wall.Seconds()
	}
	exp := Experiment{
		Name: "caching", Config: config, Offered: n, Completed: completed,
		AchievedPerSec: round3(achieved), P50Us: round3(p50), P95Us: round3(p95),
		SampleMeanUs: round3(mean), ZeroSamples: zeros, Reasons: reasons,
		LatencyMethod: latencyMethod(n, zeros), Reason: "completed",
	}
	if exp.P50Us == 0 {
		exp.LatencyMethod = quantized
	}
	if completed != n {
		exp.Failed = true
		exp.Reason = "failed"
	}
	return exp
}

func runMessaging(ctx context.Context, bus *stream.Bus) []Experiment {
	rows := []Experiment{publishConsume(ctx, bus, "normal", 200, nil)}
	rows = append(rows, serverDup(ctx, bus))
	rows = append(rows, appDup(ctx, bus))
	rows = append(rows, delayed(ctx, bus))
	rows = append(rows, lossy(ctx, bus))
	rows = append(rows, slowConsumer(ctx, bus))
	return rows
}

func publishConsume(ctx context.Context, bus *stream.Bus, config string, n int, inj *fault.Injector) Experiment {
	start := time.Now()
	published := 0
	reasons := map[string]int{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bench-%s-%d", config, i)
		body, err := stream.Encode(id, "light", []byte(id))
		if err != nil {
			return Experiment{Name: "messaging", Config: config, Failed: true, Reason: "failed", Notes: err.Error()}
		}
		code, err := bus.Publish(ctx, id, body, inj)
		if err != nil {
			reasons[reason.MessagingUnavailable]++
			continue
		}
		reasons[code]++
		if code == reason.Done {
			published++
		}
	}
	msgs, err := pull(ctx, bus, published, 5*time.Second)
	if err != nil {
		return Experiment{Name: "messaging", Config: config, Offered: n, Failed: true, Reason: "failed", Notes: err.Error(), Reasons: reasons}
	}
	led := ledger.New()
	completed := 0
	for _, msg := range msgs {
		out, err := stream.Apply(ctx, msg, led, nil)
		if err != nil {
			reasons["ack_error"]++
			continue
		}
		reasons["apply_"+out.Reason]++
		if out.Reason == reason.Done {
			completed++
		}
	}
	wall := time.Since(start)
	exp := Experiment{
		Name: "messaging", Config: config, Offered: n, Completed: completed, Reasons: reasons,
		AchievedPerSec: round3(float64(completed) / wall.Seconds()), LatencyMethod: perRequest,
	}
	if completed != published || published == 0 {
		exp.Failed = completed == 0
		exp.Reason = "partial"
		if completed == 0 {
			exp.Reason = "zero completions"
		}
	} else {
		exp.Reason = "completed"
	}
	exp.Notes = fmt.Sprintf("published=%d fetched=%d", published, len(msgs))
	return exp
}

func serverDup(ctx context.Context, bus *stream.Bus) Experiment {
	const n = 100
	dups := 0
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bench-dedup-%d", i)
		body, _ := stream.Encode(id, "light", []byte(id))
		if _, err := bus.Publish(ctx, id, body, nil); err != nil {
			return Experiment{Name: "messaging", Config: "duplicate-server", Failed: true, Reason: "failed", Notes: err.Error()}
		}
		code, err := bus.Publish(ctx, id, body, nil)
		if err != nil {
			return Experiment{Name: "messaging", Config: "duplicate-server", Failed: true, Reason: "failed", Notes: err.Error()}
		}
		if code == reason.Duplicate {
			dups++
		}
	}
	msgs, err := pull(ctx, bus, n, 5*time.Second)
	if err == nil {
		for _, msg := range msgs {
			_ = msg.Ack()
		}
	}
	exp := Experiment{Name: "messaging", Config: "duplicate-server", Offered: n, Completed: dups, Reason: "completed", LatencyMethod: perRequest}
	if err != nil || dups != n || len(msgs) != n {
		exp.Failed = true
		exp.Reason = "failed"
		exp.Notes = fmt.Sprintf("duplicate_acks=%d fetched=%d", dups, len(msgs))
	} else {
		exp.Notes = "second publish with the same Msg-Id was a server duplicate; one copy was fetched"
	}
	return exp
}

func appDup(ctx context.Context, bus *stream.Bus) Experiment {
	const n = 40
	inj := &fault.Injector{}
	inj.SetDuplicate(true)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bench-appdup-%d", i)
		body, _ := stream.Encode(id, "light", []byte(id))
		code, err := bus.Publish(ctx, id, body, inj)
		if err != nil || code != reason.Duplicate {
			note := code
			if err != nil {
				note = err.Error()
			}
			return Experiment{Name: "messaging", Config: "duplicate-application", Failed: true, Reason: "failed", Notes: note}
		}
	}
	msgs, err := pull(ctx, bus, n*2, 5*time.Second)
	if err != nil {
		return Experiment{Name: "messaging", Config: "duplicate-application", Failed: true, Reason: "failed", Notes: err.Error()}
	}
	led := ledger.New()
	done, dup := 0, 0
	for _, msg := range msgs {
		out, err := stream.Apply(ctx, msg, led, nil)
		if err != nil {
			continue
		}
		if out.Reason == reason.Done {
			done++
		}
		if out.Reason == reason.Duplicate {
			dup++
		}
	}
	exp := Experiment{
		Name: "messaging", Config: "duplicate-application", Offered: n * 2, Completed: done + dup,
		Reason: "completed", LatencyMethod: perRequest,
		Notes:   fmt.Sprintf("done=%d duplicate=%d fetched=%d", done, dup, len(msgs)),
		Reasons: map[string]int{reason.Done: done, reason.Duplicate: dup},
	}
	if done != n || dup != n {
		exp.Failed = true
		exp.Reason = "failed"
	}
	return exp
}

func delayed(ctx context.Context, bus *stream.Bus) Experiment {
	const n = 20
	inj := &fault.Injector{}
	inj.SetDelay(40 * time.Millisecond)
	start := time.Now()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bench-delay-%d", i)
		body, _ := stream.Encode(id, "light", []byte(id))
		code, err := bus.Publish(ctx, id, body, inj)
		if err != nil || code != reason.Done {
			note := code
			if err != nil {
				note = err.Error()
			}
			return Experiment{Name: "messaging", Config: "delayed", Failed: true, Reason: "failed", Notes: note}
		}
	}
	elapsed := time.Since(start)
	msgs, err := pull(ctx, bus, n, 5*time.Second)
	if err == nil {
		for _, msg := range msgs {
			_ = msg.Ack()
		}
	}
	exp := Experiment{
		Name: "messaging", Config: "delayed", Offered: n, Completed: len(msgs),
		AchievedPerSec: round3(float64(len(msgs)) / elapsed.Seconds()),
		SampleMeanUs:   round3(float64(elapsed.Microseconds()) / float64(n)),
		LatencyMethod:  "delay is a wall sleep before publish; this clock can resolve tens of milliseconds",
		Notes:          fmt.Sprintf("injected_delay=40ms total_ms=%.3f", float64(elapsed.Microseconds())/1000),
	}
	if err != nil || len(msgs) != n || elapsed < time.Duration(n)*40*time.Millisecond*9/10 {
		exp.Failed = true
		exp.Reason = "failed"
	} else {
		exp.Reason = "completed"
	}
	return exp
}

func lossy(ctx context.Context, bus *stream.Bus) Experiment {
	const n = 30
	inj := &fault.Injector{}
	inj.SetLossEvery(2)
	lost, published := 0, 0
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bench-loss-%d", i)
		body, _ := stream.Encode(id, "light", []byte(id))
		code, err := bus.Publish(ctx, id, body, inj)
		if err != nil {
			return Experiment{Name: "messaging", Config: "loss", Failed: true, Reason: "failed", Notes: err.Error()}
		}
		if code == reason.Lost {
			lost++
			inj.Clear()
			inj.SetLossEvery(2)
			code, err = bus.Publish(ctx, id, body, inj)
			if err != nil || code != reason.Done {
				return Experiment{Name: "messaging", Config: "loss", Failed: true, Reason: "failed", Notes: "retry did not publish"}
			}
			published++
			continue
		}
		published++
	}
	msgs, err := pull(ctx, bus, published, 5*time.Second)
	if err == nil {
		for _, msg := range msgs {
			_ = msg.Ack()
		}
	}
	exp := Experiment{
		Name: "messaging", Config: "loss-then-retry", Offered: n, Completed: len(msgs),
		Reason: "completed", LatencyMethod: perRequest,
		Notes:   fmt.Sprintf("lost_attempts=%d published=%d fetched=%d dropped_counter=%d", lost, published, len(msgs), inj.Dropped()),
		Reasons: map[string]int{reason.Lost: lost, reason.Retried: published},
	}
	if err != nil || len(msgs) != published || lost == 0 {
		exp.Failed = true
		exp.Reason = "failed"
	}
	return exp
}

func slowConsumer(ctx context.Context, bus *stream.Bus) Experiment {
	body, _ := stream.Encode("bench-slow", "light", []byte("slow"))
	if _, err := bus.Publish(ctx, "bench-slow", body, nil); err != nil {
		return Experiment{Name: "messaging", Config: "slow-consumer", Failed: true, Reason: "failed", Notes: err.Error()}
	}
	first, err := bus.Fetch(ctx, 1, time.Second, 300*time.Millisecond)
	if err != nil || len(first) != 1 {
		return Experiment{Name: "messaging", Config: "slow-consumer", Failed: true, Reason: "failed", Notes: "first delivery missing"}
	}
	time.Sleep(700 * time.Millisecond)
	second, err := bus.Fetch(ctx, 1, time.Second, 300*time.Millisecond)
	_ = first[0].Ack()
	deliveries := first[0].Deliveries()
	if err == nil && len(second) == 1 {
		deliveries = second[0].Deliveries()
		_ = second[0].Ack()
	}
	exp := Experiment{
		Name: "messaging", Config: "slow-consumer", Offered: 1, Completed: 1,
		LatencyMethod: perRequest, Notes: fmt.Sprintf("ack_wait=300ms handler_sleep=700ms deliveries=%d", deliveries),
	}
	if deliveries < 2 {
		exp.Failed = true
		exp.Reason = "redelivery was not observed"
		exp.Completed = 0
	} else {
		exp.Reason = "completed"
		exp.Notes += "; slow ack caused redelivery"
	}
	return exp
}

func runFailures(ctx context.Context, bus *stream.Bus) []Experiment {
	return []Experiment{reconnect(ctx, bus), restart(ctx, bus), replay(ctx, bus)}
}

func reconnect(ctx context.Context, bus *stream.Bus) Experiment {
	const n = 3
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bench-reconnect-%d", i)
		body, _ := stream.Encode(id, "light", []byte(id))
		if _, err := bus.Publish(ctx, id, body, nil); err != nil {
			return failExp("failures", "reconnect", err)
		}
	}
	msgs, err := pull(ctx, bus, n, 3*time.Second)
	if err != nil || len(msgs) != n {
		return Experiment{Name: "failures", Config: "reconnect", Failed: true, Reason: "failed", Notes: "publish fetch failed"}
	}
	if err := msgs[0].Ack(); err != nil {
		return failExp("failures", "reconnect", err)
	}
	if err := bus.Reconnect(ctx); err != nil {
		return failExp("failures", "reconnect", err)
	}
	rest, err := pull(ctx, bus, n-1, 3*time.Second)
	if err == nil {
		for _, msg := range rest {
			_ = msg.Ack()
		}
	}
	exp := Experiment{Name: "failures", Config: "reconnect", Offered: n - 1, Completed: len(rest), Reason: "completed", LatencyMethod: perRequest, Notes: "client closed and reopened; durable consumer returned unacked jobs"}
	if err != nil || len(rest) != n-1 {
		exp.Failed = true
		exp.Reason = "failed"
		exp.Notes = fmt.Sprintf("unacked fetched=%d err=%v", len(rest), err)
	}
	return exp
}

func restart(ctx context.Context, bus *stream.Bus) Experiment {
	const n = 3
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bench-restart-%d", i)
		body, _ := stream.Encode(id, "light", []byte(id))
		if _, err := bus.Publish(ctx, id, body, nil); err != nil {
			return failExp("failures", "restart", err)
		}
	}
	msgs, err := pull(ctx, bus, n, 3*time.Second)
	if err != nil || len(msgs) != n {
		return Experiment{Name: "failures", Config: "restart", Failed: true, Reason: "failed", Notes: "messages were not fetched before restart"}
	}
	if err := bus.Restart(ctx); err != nil {
		return Experiment{Name: "failures", Config: "restart", Offered: n, Failed: true, Reason: "failed", Notes: err.Error()}
	}
	again, err := pull(ctx, bus, n, 5*time.Second)
	if err == nil {
		led := ledger.New()
		for _, msg := range again {
			_, _ = stream.Apply(ctx, msg, led, nil)
		}
	}
	exp := Experiment{Name: "failures", Config: "restart", Offered: n, Completed: len(again), LatencyMethod: perRequest}
	if err != nil || len(again) != n {
		exp.Failed = true
		exp.Reason = "failed"
		exp.Notes = fmt.Sprintf("after file-store restart fetched=%d err=%v", len(again), err)
	} else {
		exp.Reason = "completed"
		exp.Notes = "embedded server stopped and started on the same file store; unacked jobs were fetched"
	}
	return exp
}

func replay(ctx context.Context, bus *stream.Bus) Experiment {
	body, _ := stream.Encode("bench-replay", "light", []byte("replay"))
	if _, err := bus.Publish(ctx, "bench-replay", body, nil); err != nil {
		return failExp("failures", "replay", err)
	}
	msgs, err := pull(ctx, bus, 1, 3*time.Second)
	if err != nil || len(msgs) != 1 {
		return Experiment{Name: "failures", Config: "replay", Failed: true, Reason: "failed", Notes: "first delivery missing"}
	}
	led := ledger.New()
	env, err := msgs[0].Envelope()
	if err != nil {
		return failExp("failures", "replay", err)
	}
	led.Do(env.ID, func() model.Outcome {
		return model.Outcome{ID: env.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	if err := msgs[0].Nak(); err != nil {
		return failExp("failures", "replay", err)
	}
	again, err := pull(ctx, bus, 1, 3*time.Second)
	if err != nil || len(again) != 1 {
		return Experiment{Name: "failures", Config: "replay", Failed: true, Reason: "failed", Notes: "redelivery missing"}
	}
	out, err := stream.Apply(ctx, again[0], led, nil)
	if err != nil {
		return failExp("failures", "replay", err)
	}
	exp := Experiment{Name: "failures", Config: "replay", Offered: 1, Completed: 1, Reason: out.Reason, LatencyMethod: perRequest, Notes: "ledger committed, ack was skipped, redelivery did not apply twice"}
	if out.Reason != reason.Replayed {
		exp.Failed = true
		exp.Completed = 0
		exp.Reason = out.Reason
	}
	return exp
}

func runDatabase(ctx context.Context, st *store.Store) []Experiment {
	if err := st.SeedLookup(ctx, 4000); err != nil {
		return []Experiment{{Name: "database", Config: "seed", Failed: true, Reason: "failed", Notes: err.Error()}}
	}
	kind, err := st.ExplainTenantKind(ctx, "SYN-TENANT-1", "kind-1", 30, false)
	if err != nil {
		return []Experiment{{Name: "database", Config: "tenant-kind", Failed: true, Reason: "failed", Notes: err.Error()}}
	}
	token, err := st.ExplainToken(ctx, "tok-1", 30)
	if err != nil {
		return []Experiment{{Name: "database", Config: "token", Failed: true, Reason: "failed", Notes: err.Error()}}
	}
	forced, err := st.ExplainTenantKind(ctx, "SYN-TENANT-1", "kind-1", 30, true)
	if err != nil {
		return []Experiment{{Name: "database", Config: "tenant-kind-seqscan-off", Failed: true, Reason: "failed", Notes: err.Error()}}
	}
	rows := []Experiment{planExp(kind), planExp(token), planExp(forced)}
	id := "bench-pg-once"
	type result struct {
		n   int
		won bool
		err error
	}
	ch := make(chan result, 8)
	for i := 0; i < 8; i++ {
		go func() {
			n, won, err := st.Complete(ctx, id, ids.Tenant, "light", []byte("x"), "", "x")
			ch <- result{n: n, won: won, err: err}
		}()
	}
	winners := 0
	applies := 0
	var failed string
	for i := 0; i < 8; i++ {
		item := <-ch
		if item.err != nil {
			failed = item.err.Error()
			continue
		}
		if item.won {
			winners++
		}
		if item.n > applies {
			applies = item.n
		}
	}
	conc := Experiment{Name: "database", Config: "concurrent-complete", Offered: 8, Completed: 8, Reason: "completed", LatencyMethod: perRequest, Notes: fmt.Sprintf("calls=8 winners=%d applies=%d", winners, applies)}
	if failed != "" || winners != 1 || applies != 1 {
		conc.Failed = true
		conc.Completed = winners
		conc.Reason = "failed"
		if failed != "" {
			conc.Notes = failed
		}
	}
	return append(rows, conc)
}

func planExp(plan store.Plan) Experiment {
	text := plan.Plan
	if len(text) > 1500 {
		text = text[:1500]
	}
	return Experiment{
		Name: "database", Config: plan.Name, Offered: plan.Iterations, Completed: plan.Iterations,
		SampleMeanUs:  round3(float64(plan.QueryMean.Nanoseconds()) / 1000),
		LatencyMethod: "mean of timed SELECT statements inside one transaction, separate from EXPLAIN ANALYZE",
		Reason:        "measured", Plan: text,
		Notes: fmt.Sprintf("seqscan_off=%t iterations=%d", plan.SeqScanOff, plan.Iterations),
	}
}

func runPressure() (Experiment, profile.Summary, error) {
	var exp Experiment
	summary, err := profile.Capture(func() {
		exp = runPool("resource-pressure", "workers=8 queue=8 jobs=160 rounds=8192 cpu_profile=on", 8, 8, 160, 8192)
	})
	exp.Name = "resource-pressure"
	exp.Notes = fmt.Sprintf("goroutines=%d heap_alloc=%d total_alloc=%d num_gc=%d", summary.Goroutines, summary.HeapAlloc, summary.TotalAlloc, summary.NumGC)
	if exp.Completed != exp.Offered {
		exp.Saturated = true
		exp.Reason = "partial"
	} else if exp.Reason == "" {
		exp.Reason = "measured"
	}
	return exp, summary, err
}

func pull(ctx context.Context, bus *stream.Bus, n int, wait time.Duration) ([]stream.Msg, error) {
	if n == 0 {
		return nil, nil
	}
	deadline := time.Now().Add(wait)
	var all []stream.Msg
	for len(all) < n && time.Now().Before(deadline) {
		batch, err := bus.Fetch(ctx, n-len(all), 400*time.Millisecond, 2*time.Second)
		if err != nil {
			return all, err
		}
		all = append(all, batch...)
	}
	return all, nil
}

func failExp(name, config string, err error) Experiment {
	return Experiment{Name: name, Config: config, Failed: true, Reason: "failed", Notes: err.Error()}
}
