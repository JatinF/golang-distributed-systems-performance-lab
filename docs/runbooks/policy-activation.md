# Policy activation failure

This process does not activate an OPA bundle. See `docs/adr/006-opa.md`.

## What you see

- Token verification returns `token_expired`, `signature_invalid`, `audience_mismatch`, `token_invalid`, or `unauthenticated`.
- The job is not admitted to the pool.
- There is no second policy that allows the call when verification fails.

## What to do

1. Treat the rejection as fail-closed. Do not add an allow default.
2. Mint a new lab token from the current process key if the clock has moved past `exp`. A token from a previous process will not verify.
3. Confirm the audience is `lab://SYN-TENANT-QUORUM` and the tenant is `SYN-TENANT-QUORUM`.
4. Do not point this lab at an OIDC issuer to "fix" activation. `LAB_OIDC_ISSUER` is blocked and is not dialed.

## What this is not

A control-plane bundle push was not performed here. Failure of that push is outside this repository.
