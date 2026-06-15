# External JWT signing keys

Squirix authenticates **external clients** (application SDKs, operators, remote `/metrics` scrapes) on the **primary
HTTPS listener** with JWT bearer tokens when auth is enabled. This document covers **JWT signing and rotation** only.

Inter-node cluster forwarding uses **mTLS**, not JWT. See [inter-node-mtls.md](inter-node-mtls.md).

```text
External client  --HTTPS + JWT/OIDC-->  primary listener
Squirix node     <--mTLS (cluster CA)-->  internal listener (no JWT)
```

Environment variable reference: [configuration.md](../configuration.md#environment-variables).

## Two external auth modes

| Mode | Configuration | Who rotates signing keys |
| --- | --- | --- |
| **OIDC / JWKS (recommended for production)** | `SQUIRIX_JWT_AUTHORITY` + `SQUIRIX_JWT_AUDIENCE` (optional `SQUIRIX_JWT_ISSUER`) | Your identity provider publishes JWKS; Squirix loads keys from authority metadata |
| **Symmetric (dev / small deployments)** | `SQUIRIX_JWT_SIGNING_KEY` + `SQUIRIX_JWT_ISSUER` + `SQUIRIX_JWT_AUDIENCE` (when required) | You — shared secret in every node and client that mints tokens |

Implementation (symmetric mode): `SquirixSecurityServiceRegistration` accepts **one** symmetric key
(`IssuerSigningKey`). In v0.1, symmetric mode has no built-in support for multiple active symmetric keys or
`kid`-based rollover.

## Blast radius (symmetric mode)

`SQUIRIX_JWT_SIGNING_KEY` is a **shared secret**. Any party that knows the key can mint JWTs that every Squirix node
configured with the same key will accept on the primary listener.

A compromised symmetric signing key allows forgery of **external** API access (cache gRPC/REST, remote metrics when JWT
is required). It does **not** bypass inter-node mTLS: cluster forwarding still requires valid node certificates on the
internal listener.

Nodes that share the same symmetric key share the same blast radius. Use distinct keys per environment (dev/staging/prod)
and store production secrets in a secret manager, not in compose files or images.

## Production recommendation

Prefer **OIDC** (`SQUIRIX_JWT_AUTHORITY`) so signing keys live in your IdP and rotate through JWKS without redeploying
a shared env secret to every node.

Symmetric `SQUIRIX_JWT_SIGNING_KEY` is appropriate for:

- local development and CI
- single-node loopback experiments (when auth is configured)
- Docker compose examples (fixed dev keys — **not** for production)

Docker and getting-started examples ship **test-only** signing keys. Do not reuse them outside local machines.

## Generate a per-environment symmetric key

When you configure symmetric JWT auth (`SQUIRIX_JWT_SIGNING_KEY`), generate a **unique** secret for each environment.
The Docker dev fixture `dev-squirix-docker-jwt-key!!!!!!` is public in this repository.

```sh
# At least 32 random bytes (example)
openssl rand -base64 48
```

Store production values in a secret manager. Set the same key on every validating node and every token issuer
(clients, scrapers, CI). Pair with environment-specific `SQUIRIX_JWT_ISSUER` and `SQUIRIX_JWT_AUDIENCE`.

Cluster mTLS material is separate — generate per-environment node certificates as described in
[inter-node-mtls.md#generate-a-local-test-ca-and-node-certificates-openssl](inter-node-mtls.md#generate-a-local-test-ca-and-node-certificates-openssl)
and [containerization.md](../containerization.md#generate-per-environment-secrets).

## Rotating symmetric keys

Squirix validates tokens against the **single** configured symmetric key at process startup. There is no overlap window
where old and new keys are both accepted.

**Hard cutover procedure:**

1. Generate a new signing key (long random secret or base64 bytes).
2. Update `SQUIRIX_JWT_SIGNING_KEY` on **every** node that validates JWTs.
3. Update every client, scraper, and token issuer (`BearerTokenProvider`, sidecars, CI secrets) to mint tokens with the
   new key.
4. Restart all Squirix nodes (rolling restart is fine once each node has the new env value).
5. Revoke or destroy the old secret in your secret store.

Expect brief invalid-token errors for in-flight clients until they pick up the new secret. Plan a maintenance window or
coordinate client updates before node restarts.

**Not supported in v0.1:**

- Multiple concurrent symmetric signing keys
- Zero-downtime symmetric rotation without coordinating all issuers and validators
- Automatic key rollover inside Squirix

If you need graceful signing-key rotation without a hard cutover, use OIDC/JWKS or terminate JWT validation at a gateway
that supports multi-key rollover.

## Rotating OIDC / JWKS keys

When `SQUIRIX_JWT_AUTHORITY` is set, Squirix uses the standard JWT bearer middleware to fetch signing keys from the
authority metadata endpoint. Key rotation is owned by your IdP:

1. Publish new keys in JWKS (often with a new `kid`).
2. Allow validators and issuers to overlap during the IdP's rotation window.
3. Retire old keys per your IdP runbook.

Ensure `SQUIRIX_JWT_AUDIENCE` (and optional `SQUIRIX_JWT_ISSUER`) stay aligned with tokens your clients receive.
`SQUIRIX_JWT_ALLOW_HTTP_METADATA` is for development only; production authorities must use HTTPS metadata.

## Separate secrets: JWT vs cluster mTLS

Multi-node clusters configure **two independent trust domains**:

| Secret / material | Purpose |
| --- | --- |
| `SQUIRIX_JWT_*` | External client authentication on the primary listener |
| `SQUIRIX_CLUSTER_MTLS_*` | Inter-node machine identity on the internal listener |

Rotating JWT signing material does not rotate node certificates, and vice versa. Document and operate them separately.
Cluster certificate rotation: [inter-node-mtls.md#certificate-rotation-high-level](inter-node-mtls.md#certificate-rotation-high-level).

## Related documentation

- [configuration.md](../configuration.md) — JWT environment variables
- [containerization.md](../containerization.md#security) — Docker dev keys
- [inter-node-mtls.md](inter-node-mtls.md) — inter-node trust (not JWT)
- [diagnostics.md](../diagnostics.md) — remote `/metrics` JWT requirements
- [operational-runbook.md](../operational-runbook.md) — security checks during triage
