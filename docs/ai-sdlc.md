# AI SDLC evidence

This repository was written in a Grok Build session. Claude Code was not used.

## What was reviewed

- Pool admission, backpressure, deadline, and shutdown.
- Ledger single-apply behavior, including a retry after a non-success result.
- Token signature, expiry, audience, and tenant checks.
- Cache miss, hit, invalidation, closed server, and recovery.
- JetStream publish, server duplicate, application duplicate, nak redelivery, and ack after the ledger.
- PostgreSQL `Complete` under concurrent callers, and the index versus unindexed plans.
- gRPC status mapping and the HTTP status mapping.
- The worker saturation rule: a level saturates when throughput does not improve by more than 10 percent over the best earlier level that itself improved. A saturated level does not become the new best.
- The load rule: a zero wall clock, with every request completed and no backpressure, is clock quantization. It is not saturation. `TestLoadSaturationIgnoresZeroWall` covers that case.

## What validated the code

On the authoring workstation, in the commit that published this repository:

- `go vet ./...` exited 0.
- `go test -race -count=1 -timeout 600s ./...` exited 0. Command wall time was 165.34 seconds. Package times are in [test-evidence.md](test-evidence.md). `internal/store` was 140.111 seconds. `internal/lab` was 80.003 seconds.
- `go run ./cmd/lab bench` wrote `reports/bench_results.json` (34 experiments, price none).
- `go run ./cmd/lab demo` wrote `reports/demo_results.json` with every step ok.

GitHub Actions runs `go test -race`, `go vet`, `go build`, a Docker build, and the private-key scanner. Those runs are evidence only after they finish. The workstation benchmark is not re-run in CI.

The session did not invent throughput numbers. [benchmark-report.md](benchmark-report.md) quotes `reports/bench_results.json`.
