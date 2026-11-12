# Worker or stream disconnect

This lab does not run an edge agent. A disconnected worker is a JetStream client that closed before `Ack`.

## What you see

- The durable consumer `SYN-DURABLE-POOL` still has the message.
- A new client to the same embedded server can `Fetch` the unacked jobs.
- If the handler already committed the ledger and then failed to ack, the redelivery is `replayed` and is not applied twice.

## What to do

1. Reconnect the client to the same server URL. Do not publish the job again unless the first publish returned `lost`.
2. Fetch with the same durable name.
3. Apply, then ack. Ack only after the ledger, and the database when it is up.
4. If the embedded server itself stopped, use the messaging outage runbook. A client reconnect does not restart the broker.

## What this is not

This is not a site edge going offline, and it is not a Kubernetes pod disruption. Those were not run.
