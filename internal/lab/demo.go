package lab

import (
	"context"
	"strings"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/apperr"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/clock"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/fault"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/gates"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ids"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ledger"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/pool"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/stream"
)

// Step is one deterministic demonstration. ObservedMs is set only when a wait was measured.
type Step struct {
	Name       string  `json:"name"`
	Reason     string  `json:"reason"`
	OK         bool    `json:"ok"`
	Detail     string  `json:"detail,omitempty"`
	ObservedMs float64 `json:"observed_ms,omitempty"`
}

// DemoReport is the scripted run. It does not claim a remote system was contacted.
type DemoReport struct {
	Tenant string       `json:"tenant"`
	Price  string       `json:"price"`
	Store  string       `json:"store"`
	Bus    string       `json:"bus"`
	Cache  string       `json:"cache"`
	Steps  []Step       `json:"steps"`
	Gates  []gates.Gate `json:"gates"`
}

// Demo runs the reason-code script. The clock starts at 2026-11-12T16:00:00Z.
func Demo(ctx context.Context) (*Lab, DemoReport, error) {
	clk := clock.New(time.Date(2026, 11, 12, 16, 0, 0, 0, time.UTC))
	report := DemoReport{Tenant: ids.Tenant, Price: "none", Cache: "in-process Redis protocol"}
	l, err := Open(ctx, clk, 4, 16)
	if err != nil {
		if l != nil {
			l.Close()
		}
		return nil, report, err
	}
	report.Gates = l.Gates
	if l.Store != nil {
		report.Store = "embedded postgresql"
	} else {
		report.Store = "not-started"
	}
	if l.Bus != nil {
		report.Bus = "embedded nats jetstream"
	} else {
		report.Bus = "not-started"
	}
	token, err := l.Token(time.Hour)
	if err != nil {
		return l, report, err
	}
	report.Steps = append(report.Steps, scriptJobs(ctx, l, token)...)
	report.Steps = append(report.Steps, scriptCache(ctx, l)...)
	report.Steps = append(report.Steps, scriptPool()...)
	report.Steps = append(report.Steps, scriptBus(ctx, l)...)
	report.Steps = append(report.Steps, scriptAuth(l, token)...)
	report.Steps = append(report.Steps, scriptStore(ctx, l)...)
	report.Steps = append(report.Steps, scriptGates(l.Gates))
	report.Steps = append(report.Steps, Step{Name: "price", Reason: "none", OK: true, Detail: "no price was measured or applied"})
	return l, report, nil
}

func scriptJobs(ctx context.Context, l *Lab, token string) []Step {
	var steps []Step
	ok := true
	for i := 1; i <= 8; i++ {
		id := "SYN-JOB-" + itoa(i)
		out := l.Handle(ctx, model.Job{ID: id, TenantID: ids.Tenant, Kind: "light", Payload: []byte(id), Token: token})
		if out.Reason != reason.Done {
			ok = false
			steps = append(steps, Step{Name: "jobs", Reason: out.Reason, OK: false, Detail: id})
		}
	}
	if ok {
		steps = append(steps, Step{Name: "jobs", Reason: reason.Done, OK: true, Detail: "8 jobs"})
	}
	dup := l.Handle(ctx, model.Job{ID: "SYN-JOB-1", TenantID: ids.Tenant, Kind: "light", Payload: []byte("SYN-JOB-1"), Token: token})
	steps = append(steps, Step{Name: "duplicate", Reason: dup.Reason, OK: dup.Reason == reason.Duplicate})
	return steps
}

func scriptCache(ctx context.Context, l *Lab) []Step {
	var steps []Step
	if l.Cache == nil {
		return []Step{{Name: "cache", Reason: reason.CacheUnavailable, OK: false, Detail: "cache did not start"}}
	}
	_, hit := l.Cache.Get(ctx, "result:SYN-JOB-1")
	steps = append(steps, Step{Name: "cache-hit", Reason: hit, OK: hit == reason.CacheHit})
	inv := l.Cache.Invalidate(ctx, "result:SYN-JOB-1")
	steps = append(steps, Step{Name: "cache-invalidate", Reason: inv, OK: inv == reason.CacheInvalidated})
	_, miss := l.Cache.Get(ctx, "result:SYN-JOB-1")
	steps = append(steps, Step{Name: "cache-miss", Reason: miss, OK: miss == reason.CacheMiss})
	l.Cache.Stop()
	_, down := l.Cache.Get(ctx, "result:SYN-JOB-1")
	steps = append(steps, Step{Name: "cache-down", Reason: down, OK: down == reason.CacheUnavailable})
	recovered, err := l.Cache.Recover()
	detail := ""
	if err != nil {
		detail = err.Error()
	}
	_, missAfter := l.Cache.Get(ctx, "result:SYN-JOB-1")
	steps = append(steps, Step{
		Name: "cache-recover", Reason: recovered, OK: err == nil && recovered == reason.CacheRecovered && missAfter == reason.CacheMiss,
		Detail: detail,
	})
	return steps
}

func scriptPool() []Step {
	var steps []Step
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	p := pool.New(1, 1, func(ctx context.Context, job model.Job) model.Outcome {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return model.Outcome{ID: job.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	go p.Submit(context.Background(), model.Job{ID: "block"})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		_ = p.Shutdown(context.Background())
		return []Step{{Name: "backpressure", Reason: reason.Cancelled, OK: false, Detail: "handler did not start"}}
	}
	go p.Submit(context.Background(), model.Job{ID: "queued"})
	waitUntil(func() bool { return p.QueueLen() == 1 }, time.Second)
	if p.QueueLen() != 1 {
		close(release)
		_ = p.Shutdown(context.Background())
		return []Step{{Name: "backpressure", Reason: reason.Backpressure, OK: false, Detail: "queue did not fill"}}
	}
	short, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	blocked := p.Submit(short, model.Job{ID: "third"})
	cancel()
	steps = append(steps, Step{Name: "backpressure", Reason: blocked.Reason, OK: blocked.Reason == reason.Backpressure})

	cancelledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	cancelled := p.Submit(cancelledCtx, model.Job{ID: "cancel"})
	steps = append(steps, Step{Name: "cancel", Reason: cancelled.Reason, OK: cancelled.Reason == reason.Cancelled})
	close(release)
	_ = p.Shutdown(context.Background())

	hold := make(chan struct{})
	slow := pool.New(1, 1, func(ctx context.Context, job model.Job) model.Outcome {
		<-hold
		return model.Outcome{ID: job.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	dead, stop := context.WithTimeout(context.Background(), 60*time.Millisecond)
	late := slow.Submit(dead, model.Job{ID: "late"})
	stop()
	close(hold)
	_ = slow.Shutdown(context.Background())
	steps = append(steps, Step{Name: "deadline", Reason: late.Reason, OK: late.Reason == reason.DeadlineExceeded})

	donePool := pool.New(1, 1, func(ctx context.Context, job model.Job) model.Outcome {
		return model.Outcome{ID: job.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	_ = donePool.Shutdown(context.Background())
	shut := donePool.Submit(context.Background(), model.Job{ID: "after"})
	steps = append(steps, Step{Name: "shutdown", Reason: shut.Reason, OK: shut.Reason == reason.ShuttingDown})
	return steps
}

func scriptBus(ctx context.Context, l *Lab) []Step {
	if l.Bus == nil {
		detail := l.BusErr
		if detail == "" {
			detail = "embedded nats did not start"
		}
		return []Step{{Name: "jetstream", Reason: reason.NotStarted, OK: true, Detail: detail}}
	}
	var steps []Step
	steps = append(steps, publishCounted(ctx, l, 4)...)
	steps = append(steps, scriptDuplicate(ctx, l)...)
	steps = append(steps, scriptDelay(ctx, l)...)
	steps = append(steps, scriptLoss(ctx, l)...)
	steps = append(steps, scriptReplay(ctx, l)...)
	return steps
}

func publishCounted(ctx context.Context, l *Lab, n int) []Step {
	for i := 0; i < n; i++ {
		id := "SYN-BUS-" + itoa(i+1)
		body, err := stream.Encode(id, "light", []byte(id))
		if err != nil {
			return []Step{{Name: "jetstream-publish", Reason: reason.MessagingUnavailable, OK: false, Detail: err.Error()}}
		}
		code, err := l.Bus.Publish(ctx, id, body, nil)
		if err != nil || code != reason.Done {
			detail := code
			if err != nil {
				detail = err.Error()
			}
			return []Step{{Name: "jetstream-publish", Reason: reason.MessagingUnavailable, OK: false, Detail: detail}}
		}
	}
	msgs, err := pull(ctx, l.Bus, n, 3*time.Second)
	if err != nil || len(msgs) != n {
		return []Step{{Name: "jetstream-publish", Reason: reason.MessagingUnavailable, OK: false, Detail: "fetch did not return the published jobs"}}
	}
	for i := 0; i < 2; i++ {
		if err := msgs[i].Ack(); err != nil {
			return []Step{{Name: "jetstream-ack", Reason: reason.MessagingUnavailable, OK: false, Detail: err.Error()}}
		}
	}
	if err := l.Bus.Reconnect(ctx); err != nil {
		return []Step{{Name: "jetstream-reconnect", Reason: reason.MessagingUnavailable, OK: false, Detail: err.Error()}}
	}
	rest, err := pull(ctx, l.Bus, n-2, 3*time.Second)
	if err != nil || len(rest) != n-2 {
		return []Step{{Name: "jetstream-reconnect", Reason: reason.MessagingUnavailable, OK: false, Detail: "durable consumer did not return the unacked jobs"}}
	}
	for _, msg := range rest {
		_ = msg.Ack()
	}
	return []Step{{Name: "jetstream-reconnect", Reason: reason.Done, OK: true, Detail: "acked 2, reconnected, pulled the rest"}}
}

func scriptDuplicate(ctx context.Context, l *Lab) []Step {
	var steps []Step
	body, _ := stream.Encode("SYN-BUS-DEDUP", "light", []byte("dedup"))
	if _, err := l.Bus.Publish(ctx, "SYN-BUS-DEDUP", body, nil); err != nil {
		return []Step{{Name: "server-duplicate", Reason: reason.MessagingUnavailable, OK: false, Detail: err.Error()}}
	}
	code, err := l.Bus.Publish(ctx, "SYN-BUS-DEDUP", body, nil)
	steps = append(steps, Step{Name: "server-duplicate", Reason: code, OK: err == nil && code == reason.Duplicate})
	got, err := pull(ctx, l.Bus, 1, 2*time.Second)
	if err == nil {
		for _, msg := range got {
			_ = msg.Ack()
		}
	}

	inj := &fault.Injector{}
	inj.SetDuplicate(true)
	appBody, _ := stream.Encode("SYN-BUS-APP", "light", []byte("app"))
	appCode, err := l.Bus.Publish(ctx, "SYN-BUS-APP", appBody, inj)
	if err != nil || appCode != reason.Duplicate {
		steps = append(steps, Step{Name: "app-duplicate", Reason: reason.MessagingUnavailable, OK: false})
		return steps
	}
	copies, err := pull(ctx, l.Bus, 2, 2*time.Second)
	if err != nil || len(copies) != 2 {
		steps = append(steps, Step{Name: "app-duplicate", Reason: reason.MessagingUnavailable, OK: false, Detail: "expected two copies"})
		return steps
	}
	led := ledger.New()
	first, err1 := stream.Apply(ctx, copies[0], led, nil)
	second, err2 := stream.Apply(ctx, copies[1], led, nil)
	ok := err1 == nil && err2 == nil && first.Reason == reason.Done && second.Reason == reason.Duplicate
	steps = append(steps, Step{Name: "app-duplicate", Reason: second.Reason, OK: ok})
	return steps
}

func scriptDelay(ctx context.Context, l *Lab) []Step {
	inj := &fault.Injector{}
	inj.SetDelay(50 * time.Millisecond)
	body, _ := stream.Encode("SYN-BUS-DELAY", "light", []byte("delay"))
	start := time.Now()
	code, err := l.Bus.Publish(ctx, "SYN-BUS-DELAY", body, inj)
	elapsed := time.Since(start)
	if err == nil && code == reason.Done {
		msgs, ferr := pull(ctx, l.Bus, 1, 2*time.Second)
		if ferr == nil {
			for _, msg := range msgs {
				_ = msg.Ack()
			}
		}
	}
	return []Step{{
		Name: "delay", Reason: code, OK: err == nil && code == reason.Done && elapsed >= 40*time.Millisecond,
		ObservedMs: float64(elapsed.Microseconds()) / 1000,
	}}
}

func scriptLoss(ctx context.Context, l *Lab) []Step {
	inj := &fault.Injector{}
	inj.SetLossEvery(1)
	body, _ := stream.Encode("SYN-BUS-LOSS", "light", []byte("loss"))
	lost, err := l.Bus.Publish(ctx, "SYN-BUS-LOSS", body, inj)
	if err != nil || lost != reason.Lost {
		detail := lost
		if err != nil {
			detail = err.Error()
		}
		return []Step{{Name: "loss", Reason: reason.Lost, OK: false, Detail: detail}}
	}
	inj.Clear()
	again, err := l.Bus.Publish(ctx, "SYN-BUS-LOSS", body, inj)
	if err != nil || again != reason.Done {
		return []Step{{Name: "loss", Reason: reason.Retried, OK: false, Detail: "retry did not publish"}}
	}
	msgs, err := pull(ctx, l.Bus, 1, 2*time.Second)
	if err != nil || len(msgs) != 1 {
		return []Step{{Name: "loss", Reason: reason.Retried, OK: false, Detail: "retry was not delivered once"}}
	}
	_ = msgs[0].Ack()
	return []Step{{Name: "loss", Reason: reason.Retried, OK: true, Detail: "first attempt lost, retry delivered once"}}
}

func scriptReplay(ctx context.Context, l *Lab) []Step {
	body, _ := stream.Encode("SYN-BUS-REPLAY", "light", []byte("replay"))
	if _, err := l.Bus.Publish(ctx, "SYN-BUS-REPLAY", body, nil); err != nil {
		return []Step{{Name: "replay", Reason: reason.MessagingUnavailable, OK: false, Detail: err.Error()}}
	}
	msgs, err := pull(ctx, l.Bus, 1, 2*time.Second)
	if err != nil || len(msgs) != 1 {
		return []Step{{Name: "replay", Reason: reason.MessagingUnavailable, OK: false, Detail: "replay message was not delivered"}}
	}
	led := ledger.New()
	env, err := msgs[0].Envelope()
	if err != nil {
		return []Step{{Name: "replay", Reason: reason.Rejected, OK: false, Detail: err.Error()}}
	}
	led.Do(env.ID, func() model.Outcome {
		return model.Outcome{ID: env.ID, Accepted: true, State: reason.Done, Reason: reason.Done}
	})
	if err := msgs[0].Nak(); err != nil {
		return []Step{{Name: "replay", Reason: reason.MessagingUnavailable, OK: false, Detail: err.Error()}}
	}
	again, err := pull(ctx, l.Bus, 1, 3*time.Second)
	if err != nil || len(again) != 1 {
		return []Step{{Name: "replay", Reason: reason.Replayed, OK: false, Detail: "redelivery did not arrive"}}
	}
	out, err := stream.Apply(ctx, again[0], led, l.Store)
	if err != nil {
		return []Step{{Name: "replay", Reason: reason.Replayed, OK: false, Detail: err.Error()}}
	}
	return []Step{{Name: "replay", Reason: out.Reason, OK: out.Reason == reason.Replayed, Detail: "ack was skipped, redelivery applied once"}}
}

func scriptAuth(l *Lab, token string) []Step {
	var steps []Step
	l.Clock.Advance(2 * time.Hour)
	_, err := l.Issuer.Parse(token)
	code := codeOf(err, reason.Done)
	steps = append(steps, Step{Name: "expired-token", Reason: code, OK: code == reason.TokenExpired})

	fresh, err := l.Token(time.Hour)
	if err != nil {
		return append(steps, Step{Name: "fresh-token", Reason: reason.TokenInvalid, OK: false, Detail: err.Error()})
	}
	parts := strings.Split(fresh, ".")
	bad := fresh
	if len(parts) == 3 && len(parts[2]) > 2 {
		bad = parts[0] + "." + parts[1] + "." + "AA" + parts[2][2:]
	}
	_, err = l.Issuer.Parse(bad)
	sig := codeOf(err, reason.Done)
	steps = append(steps, Step{Name: "bad-signature", Reason: sig, OK: sig == reason.SignatureInvalid})

	otherAud, err := l.Issuer.Issue(ids.Worker, "lab://other", ids.Tenant, time.Hour)
	if err != nil {
		steps = append(steps, Step{Name: "audience", Reason: reason.AudienceMismatch, OK: false, Detail: err.Error()})
	} else {
		_, err = l.Issuer.ParseFor(otherAud, ids.Audience, "")
		aud := codeOf(err, reason.Done)
		steps = append(steps, Step{Name: "audience", Reason: aud, OK: aud == reason.AudienceMismatch})
	}

	missing := l.Handle(context.Background(), model.Job{ID: "SYN-JOB-NOAUTH", TenantID: ids.Tenant, Kind: "light"})
	steps = append(steps, Step{Name: "unauthenticated", Reason: missing.Reason, OK: missing.Reason == reason.Unauthenticated})

	other, err := l.Issuer.Issue(ids.Worker, ids.Audience, "SYN-TENANT-OTHER", time.Hour)
	if err != nil {
		steps = append(steps, Step{Name: "other-tenant", Reason: reason.TokenInvalid, OK: false, Detail: err.Error()})
	} else {
		out := l.Handle(context.Background(), model.Job{ID: "SYN-JOB-OTHER", TenantID: "SYN-TENANT-OTHER", Kind: "light", Token: other})
		steps = append(steps, Step{Name: "other-tenant", Reason: out.Reason, OK: out.Reason == reason.TokenInvalid})
	}
	return steps
}

func scriptStore(ctx context.Context, l *Lab) []Step {
	if l.Store == nil || !l.Store.Started() {
		detail := l.StoreErr
		if detail == "" {
			detail = "embedded postgresql did not start"
		}
		return []Step{{Name: "postgres", Reason: reason.NotStarted, OK: true, Detail: detail}}
	}
	id := "SYN-JOB-PG"
	type result struct {
		applies int
		won     bool
		err     error
	}
	ch := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			n, won, err := l.Store.Complete(ctx, id, ids.Tenant, "light", []byte("pg"), "", "pg")
			ch <- result{applies: n, won: won, err: err}
		}()
	}
	won := 0
	applies := 0
	var failed string
	for i := 0; i < 2; i++ {
		item := <-ch
		if item.err != nil {
			failed = item.err.Error()
			continue
		}
		if item.won {
			won++
		}
		if item.applies > applies {
			applies = item.applies
		}
	}
	ok := failed == "" && won == 1 && applies == 1
	detail := "one transaction won; applies stayed 1"
	if failed != "" {
		detail = failed
	}
	return []Step{{Name: "postgres", Reason: reason.Done, OK: ok, Detail: detail}}
}

func scriptGates(items []gates.Gate) Step {
	if len(items) == 0 {
		return Step{Name: "remote-gates", Reason: reason.NotStarted, OK: false, Detail: "no gates"}
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		if item.Status != "not-run" && item.Status != "blocked" {
			return Step{Name: "remote-gates", Reason: item.Status, OK: false, Detail: item.Name}
		}
		parts = append(parts, item.Name+"="+item.Status)
	}
	return Step{Name: "remote-gates", Reason: reason.NotStarted, OK: true, Detail: strings.Join(parts, ", ")}
}

func pull(ctx context.Context, bus *stream.Bus, n int, wait time.Duration) ([]stream.Msg, error) {
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

func waitUntil(ok func() bool, limit time.Duration) {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func codeOf(err error, okCode string) string {
	if err == nil {
		return okCode
	}
	if ae, ok := apperr.As(err); ok {
		return ae.Code
	}
	return reason.TokenInvalid
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [4]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
