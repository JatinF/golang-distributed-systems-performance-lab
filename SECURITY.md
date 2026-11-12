# Security

This repository is a self-directed lab. It is not a production control plane.

Tokens are minted in memory with an Ed25519 key that is not written to disk. Do not commit a private key, a bearer token, or a filled `.env`. The scanner in `scripts/reject_private_keys.py` rejects PEM private-key markers.

Remote PostgreSQL, Redis, NATS, and OIDC URLs are not dialed. A set variable is reported as blocked. The process does not print the value.

The HTTP body limit is 1 MiB. Job records do not store bearer tokens. The PostgreSQL `token` column is a synthetic lookup key for the index experiment.

Report a vulnerability by opening a GitHub issue that does not include live credentials.
