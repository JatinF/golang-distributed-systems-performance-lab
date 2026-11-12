# ADR 004: PostgreSQL for transactions and index plans

## Status

Accepted when the embedded server starts. Remote PostgreSQL stays not-run.

## Context

The lab needs a real transaction that lets one of several concurrent callers mark a job done, plus an `EXPLAIN` comparison of an indexed predicate and an unindexed predicate. `psql` is not on the workstation PATH, and Docker is not installed.

## Decision

`github.com/fergusstrange/embedded-postgres` starts a PostgreSQL server on `127.0.0.1` with a random port. SQL goes through `pgx`. `Complete` inserts the row if needed and updates it only while `state <> 'done'`, so one transaction wins and `applies` stays 1.

The indexed query filters `tenant_id` and `kind`. The unindexed query filters the synthetic `token` lookup key. That column is not a bearer token. A third plan runs the indexed query with `enable_seqscan` off. The benchmark records the plan text and the measured mean. It does not assume the index is faster.

Each start uses its own extracted runtime directory. The downloaded archive is cached. `LAB_POSTGRES_URL` is not dialed.

## Consequences

If `Start` fails, the experiment reason is `not-started` and no in-memory map is called PostgreSQL. The DSN is not logged.
