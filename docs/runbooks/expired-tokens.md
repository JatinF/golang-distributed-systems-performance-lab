# Expired credentials

No X.509 certificate was issued by this lab. The credential that expires is the in-memory Ed25519 JWT.

## What you see

- `Parse` returns `token_expired` when the injectable clock is at or past `exp`.
- gRPC maps that reason to `Unauthenticated`.
- HTTP maps it to 401.
- The private key is still only in memory. There is no certificate file to rotate.

## What to do

1. Issue a new token with `Issuer.Issue` for `SYN-WORKER-INTAKE`, audience `lab://SYN-TENANT-QUORUM`, and tenant `SYN-TENANT-QUORUM`.
2. Do not lengthen a token by editing `exp`. The signature check will fail.
3. Do not commit the token. The scanner looks for PEM private keys; a pasted JWT in a report is still a secret and should be redacted.

## What this is not

An expired SPIFFE or TLS certificate. Those materials were not created.
