# Limitations

This repository is a self-directed reference. It is not a vendor product and not a customer deployment. Price is none.

## What ran on the workstation

- Go worker pool, ledger, context cancellation, and shutdown.
- gRPC unary and bidirectional streaming on `127.0.0.1`.
- An in-process Redis-protocol server (miniredis), not a remote Redis.
- An embedded NATS Server with JetStream file storage, not a remote cluster.
- Embedded PostgreSQL through `pgx` when `Start` succeeds. The benchmark report states the outcome.
- Fault injection for loss, delay, duplicate publish, a slow consumer, client reconnect, and a broker restart attempt.
- `go test -race` for the packages in this module.
- One workstation benchmark written to `reports/bench_results.json`.

## What did not run

- A remote PostgreSQL, Redis, NATS cluster, or OIDC provider. Set variables are blocked and are not dialed.
- Docker on the authoring workstation. It is not installed. The image build is the GitHub Actions job, when that job is green.
- `kubectl apply`. kubectl is not installed. `deploy/k8s/lab.yaml` was not applied.
- OPA bundle activation.
- TLS or SPIFFE certificate issue.
- A production deploy, a customer tenant, or external users.
- The full benchmark matrix inside CI. CI runs `go test -race`, `go vet`, and `go build`. It does not claim the workstation timings.

## Simulations versus integrations

| Piece | What it is |
| --- | --- |
| miniredis | In-process Redis protocol. Not Redis Cluster. |
| Embedded NATS | One local JetStream server. Not a three-node cluster. |
| Embedded PostgreSQL | A real PostgreSQL process on localhost when it starts. Not a managed database. |
| Lab JWT | In-memory Ed25519. Not an external IdP. |
| Compose and Kubernetes files | Sketches. Not started here. |
| Gates | Statements that a remote integration was not run. |

Numbers in the benchmark report are from the run that produced the JSON. They are not targets and they are not copied from another project.
