# Threat model

This is a single-process lab on a workstation. It is not a customer deployment and it has no production users.

## Protected assets

- The in-memory Ed25519 private key. It is not written to disk.
- Bearer tokens. They are not stored in PostgreSQL and are redacted if they appear in captured text.
- Job results and the idempotency ledger for `SYN-TENANT-QUORUM`.
- The embedded database files under the temporary data directory. They are removed when the store stops.
- The benchmark report. It must describe this run and must not copy timings from another repository.

## Trust assumptions

- The workstation user can read process memory. The lab does not claim to hide the key from that user.
- Callers that present a valid token for `SYN-TENANT-QUORUM` and audience `lab://SYN-TENANT-QUORUM` may submit jobs.
- The embedded NATS server, miniredis, and embedded PostgreSQL are inside the same trust boundary as the process. They are not remote production services.
- Remote URLs in the environment are untrusted as dial targets. The process does not connect to them.
- OPA is not a dependency of this process. A missing policy bundle is not treated as allow.

## Abuse cases

- Missing bearer, expired token, flipped signature, wrong audience, or another tenant. These are rejected.
- Replayed job id. The ledger returns `duplicate`. The database update matches only a row that is not `done`, so `applies` stays 1.
- Duplicate JetStream publish with the same `Nats-Msg-Id`. The broker reports a duplicate. A second copy with a new message id is collapsed by the ledger after one apply.
- Queue filled by a slow handler. A later submit with a deadline returns `backpressure` and does not run the handler.
- Cache server closed. Reads return `cache_unavailable` and do not return a previous value.
- Lost publish. The injector drops the attempt before it reaches JetStream. A later attempt can publish once.
- Oversized HTTP body. The handler limits the body to 1 MiB.

## Security controls

- Default path is authenticated. `Handle` rejects an empty token before the pool.
- Fail closed when identity verification fails.
- Bounded pool and explicit shutdown. After shutdown, submit returns `shutting_down`.
- JetStream ack happens after the ledger, and after the database commit when PostgreSQL is up.
- Private-key scanner in CI rejects PEM markers.
- Gates do not echo environment values.
- No price is attached to any call.

## Out of scope

- A remote attacker on another machine. The servers bind to `127.0.0.1`.
- TLS certificates. None were issued.
- Multi-tenant isolation beyond the single synthetic tenant check.
- Customer data. The tenant and job ids are synthetic.
