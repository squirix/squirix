# External JWT signing keys

Squirix authenticates **external clients** (application SDKs, operators, remote `/metrics` scrapes) on the **primary
HTTPS listener** with JWT bearer tokens when auth is enabled. This document covers **JWT signing and rotation** only.

Internode cluster forwarding uses **mTLS**, not JWT. See [internode-mtls.md](internode-mtls.md).

```text
External client  --HTTPS + JWT/OIDC-->  primary listener
Squirix node     <--mTLS (cluster CA)-->  internal listener (no JWT)
```

Environment variable reference: [configuration.md](../configuration.md#environment-variables).

## Two external auth modes

| Mode | Configuration | Who rotates signing keys |
| --- | --- | --- |
| **OIDC / JWKS (recommended for production)** | `SQUIRIX_JWT_AUTHORITY` + `SQUIRIX_JWT_AUDIENCE` (optional `SQUIRIX_JWT_ISSUER`) | Your identity provider publishes JWKS; Squirix loads keys from authority metadata |
| **Symmetric (dev / small deployments)** | `SQUIRIX_JWT_SIGNING_KEY` + `SQUIRIX_JWT_ISSUER` + `SQUIRIX_JWT_AUDIENCE` | You — shared secret in every node and client that mints tokens |

Implementation (symmetric mode): `SecurityServiceRegistration` accepts **one** symmetric key
(`IssuerSigningKey`). In v0.1, symmetric mode has no built-in support for multiple active symmetric keys or
`kid`-based rollover.

## Blast radius (symmetric mode)

`SQUIRIX_JWT_SIGNING_KEY` is a **shared secret**. Any party that knows the key can mint JWTs that every Squirix node
configured with the same key will accept on the primary listener.

A compromised symmetric signing key allows forgery of **external** API access (cache gRPC, remote metrics / readiness
details when JWT is required). It does **not** bypass internode mTLS: cluster forwarding still requires valid node
certificates on the internal listener.

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
The Docker dev fixture `ZGV2LXNxdWlyaXgtZG9ja2VyLWp3dC1rZXkhISEhISE=` is public in this repository.

```bash
# At least 32 random bytes (example)
openssl rand -base64 48
```

The key must be base64 that decodes to at least 32 bytes; startup fails otherwise. `SQUIRIX_JWT_AUDIENCE` is required,
so a token minted for another service that shares the key is rejected.

Store production values in a secret manager. Set the same key on every validating node and every token issuer.
Clients, scrapers, and CI jobs that only obtain and send tokens do not need the signing secret. Pair the key with
environment-specific `SQUIRIX_JWT_ISSUER` and `SQUIRIX_JWT_AUDIENCE`.

The server validates token lifetime with a two-minute clock-skew allowance. Token expiry is therefore not an immediate
revocation mechanism; allow for that tolerance when planning token lifetimes and key rotation.

Cluster mTLS material is separate — generate per-environment node certificates as described in
[internode-mtls.md#generate-a-local-test-ca-and-node-certificates-openssl](internode-mtls.md#generate-a-local-test-ca-and-node-certificates-openssl)
and [containerization.md](../containerization.md#generate-per-environment-secrets).

## Rotating symmetric keys

Squirix validates tokens against the **single** configured symmetric key at process startup. There is no overlap window
where old and new keys are both accepted.

**Hard cutover procedure:**

1. Generate a new signing key (base64 of at least 32 random bytes).
2. Update `SQUIRIX_JWT_SIGNING_KEY` on **every** node that validates JWTs.
3. Update every token issuer to sign with the new key. Update clients and scrapers to obtain tokens from that issuer;
   only components that actually mint tokens need the signing secret. `BearerTokenProvider` supplies a token, not a signing key.
4. Restart all validating Squirix nodes to load the new key, coordinating the cutover with token issuers and clients.
5. Stop using tokens signed with the old key, then revoke or destroy the old secret in your secret store.

Plan a maintenance window for this cutover. During a rolling restart, old-key and new-key validators coexist;
neither accepts the other's tokens. Issuing a new token or configuring bootstrap failover does not make this a
zero-downtime procedure. Expect authentication failures until clients and all validating nodes use the new key.

**Not supported in v0.1:**

- Multiple concurrent symmetric signing keys
- Zero-downtime symmetric rotation without coordinating all issuers and validators
- Automatic key rollover inside Squirix

If you need graceful signing-key rotation without a hard cutover, use OIDC/JWKS with an overlapping key publication
window. A gateway in front of Squirix does not by itself remove the server's JWT requirements.

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
| `SQUIRIX_CLUSTER_MTLS_*` | Internode machine identity on the internal listener |

Rotating JWT signing material does not rotate node certificates, and vice versa. Document and operate them separately.
Cluster certificate rotation: [internode-mtls.md#certificate-rotation-high-level](internode-mtls.md#certificate-rotation-high-level).

## Related documentation

- [configuration.md](../configuration.md) — JWT environment variables
- [containerization.md](../containerization.md#security) — Docker dev keys
- [internode-mtls.md](internode-mtls.md) — internode trust (not JWT)
- [diagnostics.md](../diagnostics.md) — remote `/metrics` JWT requirements
- [operational-runbook.md](../operational-runbook.md) — security checks during triage
