# ADR 002: gRPC unary and bidirectional streaming

## Status

Accepted for the data-plane comparison.

## Context

The benchmark has to compare one request/response RPC with a stream of the same job message. HTTP remains the health and gate surface because those calls are not the throughput comparison.

## Decision

`proto/lab/v1/work.proto` defines `Work.Submit` and `Work.Stream`. Both carry `JobRequest` and `JobReply`. The server listens on `127.0.0.1` with an ephemeral port. Authentication is a bearer token in gRPC metadata, not a field in the protobuf.

Unary calls map lab reasons onto gRPC status codes. The stream sends the reply and closes the stream on authentication or validation failures.

## Consequences

The benchmark records unary and streaming separately and says how the client was shaped. Generated Go stubs are committed so CI does not need `protoc`.
