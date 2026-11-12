# ADR 007: Lab Ed25519 identity

## Status

Accepted. No external identity provider is contacted.

## Context

Jobs need a caller, a tenant, and an expiry so the lab can reject missing, forged, and expired credentials. No OIDC issuer was available for a live test, and `LAB_OIDC_ISSUER` must not be dialed.

## Decision

The issuer is `https://syn.lab/perf-lab`. The audience is `lab://SYN-TENANT-QUORUM`. The tenant is `SYN-TENANT-QUORUM`. The worker subject is `SYN-WORKER-INTAKE`.

The key is Ed25519, generated in memory for the process, and not written to disk. Tokens are compact JWTs (`alg` EdDSA). Verification checks the signature, issuer, subject, audience, tenant, and `exp` against an injectable clock. The demo clock starts at `2026-11-12T16:00:00Z`.

No X.509 certificate is issued. Expired-certificate drills in the runbook are expired lab tokens, and the runbook says that directly.

## Consequences

These tokens are lab credentials. They are not production workforce identity and they are not SPIFFE IDs. A different process has a different key, so a token from one run does not verify in another.
