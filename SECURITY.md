# Security policy

## Supported versions

Security fixes are applied to the latest published minor release. Pre-1.0 public
APIs may change when a safe fix requires it.

## Report a vulnerability

Use GitHub's
[private vulnerability reporting](https://github.com/VivekReddy10-08-2004/sync-sdk/security/advisories/new).
Do not include credentials, production data, or an undisclosed vulnerability in
a public issue.

Include the affected version, deployment configuration, reproduction steps, and
expected impact. A maintainer will acknowledge the report and coordinate a fix
and disclosure.

## Application responsibilities

The host application owns authentication, authorization, tenant isolation,
database credentials, TLS termination, schema migrations, backups, and retention.
The SDK's FastAPI routes do not provide those controls automatically.
