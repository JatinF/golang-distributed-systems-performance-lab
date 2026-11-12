# Architecture

The lab is one process. Callers that need a measurement use the pool, gRPC, the cache, JetStream, or PostgreSQL inside that process. Remote systems are gates, not dials.

```mermaid
flowchart LR
  caller[Caller]
  identity[Identity]
  ledger[Ledger]
  pool[WorkerPool]
  cacheNode[Cache]
  jetNode[JetStream]
  storeNode[Postgres]
  caller --> identity
  identity --> ledger
  ledger --> pool
  pool --> cacheNode
  pool --> storeNode
  jetNode --> ledger
```

Node ids avoid the reserved word `graph`. The diagrams in `docs/diagrams/` are the pictures for the README. This Mermaid block is a supplement.

Trust boundary: the process, the loopback listeners, and the temporary data directories. Outside that boundary, remote URLs are not contacted.
