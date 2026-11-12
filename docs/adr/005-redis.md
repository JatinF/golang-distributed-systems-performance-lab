# ADR 005: Redis protocol via an in-process server

## Status

Accepted as a cache protocol test. A remote Redis was not contacted.

## Context

The lab needs cache hit, miss, invalidation, and a failure where the server is gone and the client does not answer with a stale value. `redis-server` is not installed, and Docker is not installed.

## Decision

`miniredis` speaks the Redis protocol on `127.0.0.1`. The client is `go-redis`. `GET` maps `redis.Nil` to `cache_miss` and any other error to `cache_unavailable`. `Stop` closes the server. `Recover` starts a new empty server. The next read is a miss.

`LAB_REDIS_URL` is not dialed.

## Consequences

This is not Redis Cluster, persistence, or a production cache. The benchmark says the server is in-process.
