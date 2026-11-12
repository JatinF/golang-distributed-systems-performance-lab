# Test evidence

Evidence is split by what the command actually did. Unit and race tests do not replace the workstation matrix. The matrix does not replace GitHub Actions. Actions evidence is recorded only after a run finishes and its annotation list has been read.

## Race and vet on the authoring workstation

Command, from the module root, on 2026-11-12:

```text
go vet ./...
go test -race -count=1 -timeout 600s ./...
```

The combined command exited 0. Wall time was 165.34 seconds. `go vet` prints nothing when it passes, and the race suite then ran.

| Package | Result |
| --- | --- |
| `internal/bench` | ok, 2.763s |
| `internal/cache` | ok, 1.984s |
| `internal/clock` | ok, 1.904s |
| `internal/fault` | ok, 1.607s |
| `internal/gates` | ok, 2.300s |
| `internal/httpapi` | ok, 4.565s |
| `internal/identity` | ok, 2.220s |
| `internal/lab` | ok, 80.003s |
| `internal/ledger` | ok, 1.968s |
| `internal/pool` | ok, 4.481s |
| `internal/redact` | ok, 4.289s |
| `internal/rpc` | ok, 3.453s |
| `internal/store` | ok, 140.111s |
| `internal/stream` | ok, 5.266s |

Packages with no test files: `cmd/lab`, `internal/apperr`, `internal/ids`, `internal/model`, `internal/profile`, `internal/reason`, `internal/rpc/labpb`, `internal/work`.

`internal/store` and `internal/lab` start embedded PostgreSQL. They use separate runtime directories and a shared download cache guarded by a lock file, so the two packages can run in the same `go test ./...`.

## Unit and integration coverage

| Area | Test | What it checks |
| --- | --- | --- |
| Pool | `TestCompletesWork`, `TestBackpressureCancelAndDeadline` | Admitted work finishes. A full queue returns `backpressure`. Cancel and deadline return `cancelled` and `deadline_exceeded`. |
| Ledger | `TestDoOnce`, `TestRetryAfterBackpressure` | One success is remembered. A non-success is not remembered, so a later call can run. |
| Identity | `TestIssueAndParse`, `TestRejectsBadSignatureAndAudience`, `TestBearer` | Signature, expiry, audience, and tenant. Bearer parsing. |
| Cache | `TestHitMissInvalidateAndRecover` | Miss, hit, invalidation, closed server, recovery onto an empty server. |
| Faults | `TestLossDelayAndDuplicate` | Drop, delay, and duplicate at the publish boundary. |
| Gates | `TestUnsetIsNotRunAndSetIsBlocked` | An unset variable is `not-run`. A set value is blocked and is not echoed. |
| Redaction | `TestSecret` | Bearer and compact JWT material is redacted. |
| Clock | `TestAdvance` | The test clock advances. |
| Bench rules | `TestWorkerSaturationRule`, `TestLoadSaturationIgnoresZeroWall` | The 10% worker rule. A zero wall clock with a full completion is not load saturation. |
| JetStream | `TestPublishFetchAckAndDuplicate`, `TestLossDoesNotPublish`, `TestReplayAfterNak` | A second publish of the same id returns `duplicate` and one message is fetched. A dropped publish is not fetched. Nak after the ledger commits returns `replayed`. |
| gRPC | `TestUnaryAndStream` | Unary and bidirectional streaming on `127.0.0.1`, including a missing bearer. |
| HTTP | `TestHealthGatesAndJobs` | Health, gates, and job admission. |
| PostgreSQL | `TestConcurrentCompleteAndPlans` | Concurrent `Complete`, and the tenant+kind plan versus the token plan, on embedded PostgreSQL. |
| Demo | `TestDemoReasons` | The scripted reason codes, including shutdown, reconnect, and the remote gates. |

## Authorization

Authorization checks in the race suite and in `reports/demo_results.json`:

| Case | Reason |
| --- | --- |
| Missing bearer | `unauthenticated` |
| Expired lab token | `token_expired` |
| Bad signature | `signature_invalid` |
| Wrong audience | `audience_mismatch` |
| Other tenant | `token_invalid` |

No OPA bundle is loaded. These checks are the lab Ed25519 verifier from [ADR 007](adr/007-identity.md). A failed check does not fall open.

## Failure

| Case | Where it was observed | Result |
| --- | --- | --- |
| Full queue | Pool test and demo | `backpressure` |
| Cancelled caller | Pool test and demo | `cancelled` |
| Deadline | Pool test and demo | `deadline_exceeded` |
| Shutdown | Pool test and demo | After `Shutdown`, a new submit returns `shutting_down`. |
| Closed cache | Cache test and demo | `cache_unavailable` |
| Cache process replaced | Cache test and demo | `cache_recovered`, next read is a miss |
| Publish loss | Fault test, stream test, demo, bench | First attempt `lost` or not published. Retry delivers once. |
| Slow consumer | Bench `messaging` / `slow-consumer` | Deliveries 2. One completion. |
| Client reconnect | Demo and bench | Unacked jobs returned to the new client. |
| Embedded broker restart | Bench `failures` / `restart` | Completed. The same file store returned unacked jobs. |
| Ack skipped after commit | Stream test, demo, bench | `replayed`. The ledger did not apply twice. |
| Eight SQL completers | Store test, demo, bench | One winner. `applies` stayed 1. |

## Performance

`go test` does not run the matrix. The matrix is `go run ./cmd/lab bench`, which wrote 34 rows to `reports/bench_results.json`. The human report is [benchmark-report.md](benchmark-report.md).

Short form of that JSON: price `none`, zero rows with `failed: true`, saturated rows are light work at 128 workers and heavy work at 32 workers. Load rows at 100, 1,000, and 10,000 offered requests all completed. Only the 100/s rows were paced.

`go run ./cmd/lab demo` wrote `reports/demo_results.json`. Every step is `ok: true`. The delay step records `observed_ms` 50.906. The seven remote gates are `not-run`.

## GitHub Actions

The workflow files are `.github/workflows/ci.yml` and `.github/workflows/security.yml`.

- CI: `ubuntu-24.04`, `actions/checkout@v7`, `actions/setup-go@v7` with Go 1.26.4, `go test -race -count=1 ./... -timeout 600s`, `go vet ./...`, `go build -o bin/ ./cmd/...`, and a separate job that runs `docker build`.
- Security: `actions/setup-python@v6` with Python 3.12 and `python scripts/reject_private_keys.py`.

Those jobs are not evidence until they finish. Annotation lists are read from the check-run API. An empty list is claimed only when that API returns `[]`. The workstation benchmark is not part of either workflow.
