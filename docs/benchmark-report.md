# Benchmark report

This report quotes [`reports/bench_results.json`](../reports/bench_results.json) from one `go run ./cmd/lab bench` on the authoring workstation. The profile text in that file is timestamped 2026-11-12 11:51:38 PST. The pprof `File` and `Build ID` lines in the JSON name a local temp build. The sample rows below are the measurement. No `*.pprof` file is committed.

The configurations (worker counts, offered rates, unary versus streaming, cache states, message faults, query plans, resource pressure, restart, reconnect, replay) were attempts. The tables record achieved throughput and whether that attempt saturated or failed. They are not targets, and they are not copied from another repository.

Price is `none`. No price was measured or applied.

CI runs `go test -race`, `go vet`, and `go build` on `ubuntu-24.04`. Those jobs do not run this matrix and do not reproduce these timings.

## Hardware

| Field | Value in the JSON |
| --- | --- |
| Go | `go1.26.4` |
| OS / arch | `windows` / `amd64` |
| `cpus` | 8 |
| `gomaxprocs` | 8 |
| Processor | `11th Gen Intel(R) Core(TM) i7-1185G7 @ 3.00GHz` |
| Store | `embedded postgresql, 127.0.0.1, sslmode=disable` |
| Bus | `embedded nats jetstream, file store, 127.0.0.1` |
| Cache | `in-process Redis protocol (miniredis)` |
| Experiments | 34 |
| `failed: true` | none |

`cpus` is `runtime.NumCPU` on this machine, which is 8 logical processors.

## Methodology

`achieved_per_sec` is completed work divided by wall time when the wall clock advanced. It is a completion rate. It is not a latency.

Per-request samples use the Go wall clock. On this Windows machine a call that finishes inside one tick is recorded as 0 µs. When `p50_us` is 0 and samples exist, `latency_method` is:

> per-request samples include zero readings because the call finished inside one Windows clock tick. achieved_per_sec is elapsed completions. p50_us is the raw sample and is not a claim that the work was instant.

A p50 of 0 is reported as 0. It is not rewritten into a positive latency. Rows that do not take per-request samples leave `achieved_per_sec` at 0 when they are functional checks (duplicate handling, restart, replay, plan means). Those zeros are absent rates, not a workload that ran at zero throughput.

Worker saturation is `MarkWorkerSaturation`. A level that completes every offered job updates the best only when its throughput is more than 10% above the current best. Otherwise `saturated` is true and the reason is `adding workers stopped helping`. A saturated level does not become the new best. The first completed level is `baseline`. The 10% threshold is unit-tested and was not changed after this run.

Load saturation is `LoadSaturated`:

- A zero wall clock, with every offered job completed and no backpressure, is clock quantization. It is not saturation.
- Saturation is also true when completed work is under 90% of offered work, or when any call returns backpressure.
- An unpaced burst is saturated when the wall clock advanced and `achieved_per_sec` is under 90% of the named target.
- Pacing is used only when the inter-arrival gap is at least 2 ms. This clock cannot pace a shorter gap. The 100/s rows are paced. The 1,000/s and 10,000/s rows are bursts launched as fast as the loop can submit.

`deadline` on the load rows is 1 second per request. Workers are 8 and the queue depth is 32.

The gRPC rows use different clients. Unary uses 400 client goroutines. Streaming puts 400 requests on one pipelined bidirectional stream. Streaming samples are time blocked in `Recv`. They are not isolated round trips.

Database `sample_mean_us` is the mean of timed `SELECT` statements inside one transaction. `EXPLAIN (ANALYZE)` execution time is a separate number from the planner. Both are quoted. They are not averaged together.

The work function is SHA-256 repeated for the round count in the row config. Light rows use 1 round. Heavy and `load-cpu` rows use the round count printed in the config (8,192 or 2,048).

## Worker concurrency

Light work: 3,000 jobs, 1 round, queue depth equal to the worker count.

| Workers | Achieved/s | p50 µs | p95 µs | Sample mean µs | Zero samples | Saturated | Reason |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 1 | 221,814.74 | 2,140.4 | 4,363.8 | 1,946.642 | 742 | false | baseline |
| 8 | 349,450.78 | 0 | 2,769 | 411.727 | 2,197 | false | throughput increased |
| 32 | 403,160.781 | 0 | 0 | 37.793 | 2,878 | false | throughput increased |
| 128 | 400,325.598 | 0 | 0 | 51.647 | 2,866 | true | adding workers stopped helping |

All 3,000 jobs completed at every level. None failed. From 1 to 8 workers the rate rose from 221,814.74/s to 349,450.78/s. From 8 to 32 it rose to 403,160.781/s, which clears 10% over 349,450.78. 128 workers completed at 400,325.598/s, which is below 403,160.781, so the rule marks 128 saturated. p50 is 0 at 8, 32, and 128 because most samples finished inside one tick (2,197, 2,878, and 2,866 zeros). The 1-worker p50 of 2,140.4 µs still includes 742 zero samples. The headline is `achieved_per_sec`.

Heavy work: 192 jobs, 8,192 rounds, queue depth equal to the worker count. Every p50 below is non-zero, so the latency method is per-request wall time.

| Workers | Achieved/s | p50 µs | p95 µs | Sample mean µs | Zero samples | Saturated | Reason |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 1 | 1,132.25 | 75,714.9 | 148,989.9 | 75,083.064 | 0 | false | baseline |
| 8 | 5,982.93 | 9,015.9 | 18,667.2 | 8,429.458 | 23 | false | throughput increased |
| 32 | 6,493.528 | 1,502.6 | 2,966.3 | 1,218.601 | 72 | true | adding workers stopped helping |
| 128 | 7,415.389 | 1,002.5 | 2,013 | 1,065.785 | 67 | false | throughput increased |

All 192 jobs completed. None failed. 8 workers are the first level that clears 10% (5,982.93/s against the 1,132.25/s baseline), so the best stays 5,982.93. 32 workers at 6,493.528/s are about 8.5% above 5,982.93, which misses the 10% bar (the bar is 6,581.223/s). That row is saturated, and it does not replace the best. 128 workers at 7,415.389/s are about 23.9% above 5,982.93, so the rule records `throughput increased` and `saturated: false`. The 128-worker point is also above the 32-worker point. This is one run on 8 logical processors. The threshold was not moved to change either label.

## Request load

Eight workers, queue 32, deadline 1 second. Offered counts are 100, 1,000, and 10,000. `load-light` uses 1 round. `load-cpu` uses 2,048 rounds.

| Row | Achieved/s | p50 µs | p95 µs | Sample mean µs | Zero samples | Notes | Saturated |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| light 100 | 100.948 | 0 | 0 | 0 | 100 | paced, launch 0.991 s, offer 100.9/s | false |
| cpu 100 | 100.96 | 0 | 376.7 | 30.299 | 94 | paced, launch 0.990 s, offer 101.0/s | false |
| light 1,000 | 283,865.107 | 0 | 0 | 11.952 | 996 | burst, launch 0.001 s, offer 1,869,857.9/s | false |
| cpu 1,000 | 33,462.051 | 28,880.6 | 29,884.6 | 28,637.264 | 0 | burst, launch finished inside one tick, offer rate not computed | false |
| light 10,000 | 340,401.197 | 6,002 | 12,506.2 | 5,825.456 | 2,949 | burst, launch 0.021 s, offer 478,073.2/s | false |
| cpu 10,000 | 29,304.888 | 222,116.5 | 300,054.3 | 194,751.329 | 45 | burst, launch 0.036 s, offer 281,237.0/s | false |

Every row completed every offered job. Reason `completed`. `failed` is false. No backpressure is recorded on these rows.

The 100/s rows are the paced attempts. Offer rate was about 101/s and achieved completions were about 101/s. Their p50 is 0 because each call finished inside one tick.

The 1,000/s and 10,000/s rows were not paced. A 1,000/s gap is 1 ms and a 10,000/s gap is 0.1 ms, both under the 2 ms pacing floor. The light 1,000 burst submitted in 0.001 s and then completed at 283,865.107/s of wall time. That number is the completion rate of 1,000 one-round jobs. It is not evidence that the process accepted a paced 1,000/s arrival process. The same reading applies to light 10,000 at 340,401.197/s.

The CPU bursts did real work. One thousand jobs of 2,048 rounds completed at 33,462.051/s with p50 28,880.6 µs. Ten thousand jobs completed at 29,304.888/s with p50 222,116.5 µs and p95 300,054.3 µs, inside the 1 second deadline. The coded rule does not mark them saturated: every job completed, and the completion rate is above 90% of the named target. This handler and deadline finished the offered CPU work. The worker-count series is where the 10% rule fired.

## gRPC

Server workers are 8. Both rows completed 400 of 400. Neither saturated. Neither failed.

| Client | Achieved/s | p50 µs | p95 µs | Sample mean µs | Zero samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| Unary, 400 client goroutines | 25,905.058 | 14,659.2 | 15,441 | 14,945.349 | 0 |
| One pipelined bidi stream | 15,098.556 | 0 | 999.1 | 66.232 | 374 |

Unary p50 is a per-request wall time. Bidi p50 is 0 because 374 of 400 `Recv` waits finished inside one tick. The bidi note in the JSON says those samples are time blocked in `Recv` on one pipelined stream, not an isolated round trip. On this run the unary fanout completed faster than the single pipelined stream. That comparison is these two client shapes.

## Caching

One thousand operations each. All completed. None saturated. None failed. p50 is 0 on the three rate rows.

| Config | Achieved/s | p95 µs | Sample mean µs | Zero samples | Reason counts |
| --- | ---: | ---: | ---: | ---: | --- |
| hit | 20,652.068 | 0 | 46.917 | 971 | `cache_hit` 1,000 |
| miss | 24,503.198 | 0 | 40.811 | 970 | `cache_miss` 1,000 |
| invalidation | 7,553.412 | 512.7 | 131.974 | 934 | `cache_invalidated` 1,000 |

On this run the miss row completed faster than the hit row (24,503.198/s versus 20,652.068/s). Invalidation was slower (7,553.412/s). The p50 of 0 is the clock. The headline is operations per second. This run does not show a hit advantage.

`unavailable-then-recover` offered 2 and completed 2. `achieved_per_sec` is 0 because the row does not record a wall rate. Reason `cache_recovered`. Counts: `cache_unavailable` 1, `cache_miss` 1. The closed server failed closed. The replacement server started empty, so the next read was a miss.

## Messaging

Embedded JetStream on `127.0.0.1` with a file store. Functional rows that do not divide completions by a wall clock keep `achieved_per_sec` at 0. That is an absent rate.

| Config | Offered | Completed | Achieved/s | What the row recorded |
| --- | ---: | ---: | ---: | --- |
| normal | 200 | 200 | 8,093.234 | published 200, fetched 200. Counts `done` 200 and `apply_done` 200. |
| duplicate-server | 100 | 100 | 0 | A second publish with the same `Msg-Id` was a server duplicate. One copy was fetched. |
| duplicate-application | 80 | 80 | 0 | `done` 40, `duplicate` 40, fetched 80. |
| delayed | 20 | 20 | 24.485 | Injected delay 40 ms. Total 816.822 ms. Sample mean 40,841.1 µs. The clock resolved tens of milliseconds. |
| loss-then-retry | 30 | 30 | 0 | Lost attempts 29, published 30, fetched 30, drop counter 29. Counts `lost` 29 and `retried` 30. |
| slow-consumer | 1 | 1 | 0 | Ack wait 300 ms, handler sleep 700 ms, deliveries 2. The slow ack was redelivered. |

None of these rows are saturated. None failed.

## Failures

| Config | Offered | Completed | Reason | Notes |
| --- | ---: | ---: | --- | --- |
| reconnect | 2 | 2 | completed | The client closed and reopened. The durable consumer returned unacked jobs. |
| restart | 3 | 3 | completed | The embedded server stopped and started on the same file store. Unacked jobs were fetched. |
| replay | 1 | 1 | replayed | The ledger had committed. Ack was skipped. Redelivery did not apply twice. |

`achieved_per_sec` is 0 on all three because they are outcome checks. Restart completed. It did not fail. Reconnect completed. Replay returned `replayed` and did not apply the job a second time.

## Database

Seed: 4,000 rows. Tenant id is `SYN-TENANT-{i % 50}`. Kind is `kind-{i % 10}`. The token column is `tok-{i}`, a synthetic lookup key, not a bearer. Because 50 is a multiple of 10, every `SYN-TENANT-1` row has kind `kind-1`. That is 80 rows out of 4,000. The plan output says `rows=80` and the token scan says `Rows Removed by Filter: 3999`.

Thirty timed selects ran inside one transaction for each plan. `achieved_per_sec` is 0 because these rows are means, not rates. None failed.

| Config | Sample mean µs | Plan the JSON recorded | EXPLAIN execution time |
| --- | ---: | --- | --- |
| tenant-kind | 148.88 | Index Only Scan using `jobs_tenant_kind_idx`. Index condition tenant `SYN-TENANT-1` and kind `kind-1`. Heap fetches 80. | 0.292 ms |
| token | 501.79 | Seq Scan. Filter `token = tok-1`. Rows removed by filter 3,999. | 0.364 ms |
| tenant-kind-seqscan-off | 131.603 | The same Index Only Scan, with `enable_seqscan` off. | 0.054 ms |

The indexed predicate's sample mean (148.88 µs, and 131.603 µs with sequential scans disabled) was lower than the token sequential scan (501.79 µs) on this seed. The planner chose the index for tenant+kind even before sequential scans were disabled. That is this 4,000-row set, with 80 matching rows. It is not a claim that the index wins on every table.

`concurrent-complete` offered 8 and completed 8. Notes: `calls=8 winners=1 applies=1`. Completed is 8 because every call returned. One transaction won, and `applies` stayed 1.

## Resource pressure

Config: 8 workers, queue 8, 160 jobs, 8,192 rounds, CPU profile on. Completed 160 of 160 at 5,582.031/s. p50 1,547.8 µs. p95 2,750.3 µs. Sample mean 1,353.874 µs. Zero samples 43. Not saturated. Not failed. Reason counts `done` 160.

MemStats on that row: goroutines 32, heap alloc 8,662,608 bytes, total alloc 133,740,312 bytes, num GC 21.

The CPU profile window is 28.66 ms, with total samples 160 ms (558.20% because samples accumulated on more than one core). Flat and cumulative samples:

| Flat | Cumulative | Function |
| ---: | ---: | --- |
| 100 ms (62.50%) | 100 ms (62.50%) | `crypto/internal/fips140/sha256.blockSHANI` |
| 20 ms (12.50%) | 120 ms (75.00%) | `crypto/internal/fips140/sha256.(*Digest).checkSum` |
| 20 ms (12.50%) | 150 ms (93.75%) | `crypto/sha256.Sum256` |
| 10 ms (6.25%) | 130 ms (81.25%) | `crypto/internal/fips140/sha256.(*Digest).Sum` |
| 10 ms (6.25%) | 160 ms (100%) | `internal/work.Digest` |

CPU time in this profiled window is SHA-256.

Heap `inuse_space` total 20,578.74 kB at the top of the profile:

| Flat | Function |
| ---: | --- |
| 8,192 kB (39.81%) | `github.com/xi2/xz.xzDecLZMA2Reset` |
| 7,173.58 kB (34.86%) | `runtime.mallocgc` |
| 553.04 kB (2.69%) | `github.com/nats-io/nats-server/v2/server.newClosedRingBuffer` |
| 553.04 kB (2.69%) | `time.newTimer` |
| 515.32 kB (2.50%) | `github.com/nats-io/nats-server/v2/server.(*Server).createInternalClient` |

The largest retained block after the run is the PostgreSQL archive decoder (`xz`), then runtime allocation. The worker pool is not the retained-heap leader.

Allocation `alloc_space` showed 134.12 MB. The top flat samples include `io.copyBuffer` at 41.78 MB (31.15%), `internal/pool.(*Pool).Submit` at 17 MB flat (12.68%, cumulative 19 MB), `context.WithDeadlineCause` at 10 MB flat, `xz.xzDecLZMA2Reset` at 8 MB, and `encoding/hex.EncodeToString` at 6 MB.

The goroutine profile shows 32 goroutines, 31 of them in `runtime.gopark`.

## Gates

All seven gates are `not-run`:

| Gate | Reason recorded |
| --- | --- |
| postgres-remote | `LAB_POSTGRES_URL` is unset. A remote PostgreSQL was not contacted. |
| redis-remote | `LAB_REDIS_URL` is unset. A remote Redis was not contacted. |
| nats-remote | `LAB_NATS_URL` is unset. A remote NATS cluster was not contacted. |
| oidc-provider | `LAB_OIDC_ISSUER` is unset. No OIDC provider was contacted. |
| docker-local | Docker is not installed on this workstation. A local image build was not run. |
| kubernetes | kubectl is not installed. No manifest was applied. |
| customer-deployment | `SYN-TENANT-QUORUM` is synthetic. No customer deployment was made. |

## What this run does not say

It does not say the service sustains a paced 1,000/s or 10,000/s arrival process. Those attempts were bursts, and the report says so.

It does not say 128 workers are a general optimum. On light work, 128 saturated against 32. On heavy work, 32 saturated against 8, and 128 later cleared the 10% bar against the last improving level.

It does not say a cache hit is faster than a miss. This run measured the opposite.

It does not say the index is faster on every schema. It was faster here, on 4,000 rows with 80 matches for the indexed predicate.

It does not transfer to a remote PostgreSQL, Redis, NATS cluster, OIDC provider, or a deployed container. Those gates stayed `not-run`.
