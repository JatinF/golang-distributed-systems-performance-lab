# ADR 003: In-process NATS JetStream

## Status

Accepted as a local broker. A remote cluster is not contacted.

## Context

The lab needs a durable consumer, explicit ack, server-side de-duplication, and a way to reconnect a worker. `nats-server` is not installed on the authoring workstation, and Docker is not installed, so a separate broker process was not started.

## Decision

The process embeds `github.com/nats-io/nats-server/v2` with JetStream file storage on `127.0.0.1`. The stream is `SYN_STREAM_JOBS`, the subject is `lab.jobs`, and the durable consumer is `SYN-DURABLE-POOL`. Publish uses `Nats-Msg-Id` for the broker duplicate window. A second copy with a different message id is an application duplicate and is collapsed by the ledger. The handler commits the ledger, and the database when it is up, before `Ack`.

`LAB_NATS_URL`, when set, is blocked. The process does not dial it.

## Consequences

This is one embedded server, not a three-node JetStream cluster. File-store restart is attempted in the benchmark and the result is whatever that run recorded. If the restart fails, the report says so. A map is not labeled JetStream.
