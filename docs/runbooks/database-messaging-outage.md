# Database or messaging outage

## Database

If embedded PostgreSQL did not start, job handling still runs the pool and the ledger. The benchmark row reason is `not-started`. Do not describe an in-memory map as PostgreSQL.

If PostgreSQL started and a later `Complete` returns an error, the handler returns `store_unavailable`, does not ack a JetStream message, and does not remember the failure as a successful apply. A later attempt can retry.

`LAB_POSTGRES_URL` is not a failover target. The process does not dial it.

## Messaging

If the embedded server is down, `Publish` returns `messaging_unavailable` and the message is not acknowledged.

A client disconnect is the worker-disconnect runbook. A broker restart uses the same file store. The benchmark records whether unacked messages were fetched after that restart. If they were not, the row is failed and the note has the error.

A dropped publish (`lost`) never entered the stream. Retry publishes once after the injector is cleared. Do not ack a job that was not delivered.

## Cache

A closed cache returns `cache_unavailable`. It does not serve the last value. Recovery is a new empty server and a miss, then a refill after the next successful job.
