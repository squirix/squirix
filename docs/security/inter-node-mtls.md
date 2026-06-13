# Inter-node mTLS

Squirix separates **external client authentication** from **inter-node cluster authentication**. Applications connect
to the primary HTTPS listener with JWT/OIDC (or loopback-only unauthenticated access in development). Cluster nodes
forward owner operations to peers over a **dedicated internal HTTPS listener** that requires **mutual TLS** signed by a
shared cluster trust root.

```text
External client  --HTTPS + JWT/OIDC-->  Squirix node (primary listener)
Squirix node     <--mTLS (cluster CA)-->  Squirix node (internal listener)
```

Squirix **consumes** node certificates and a cluster CA that you provision externally. It does **not** issue production
certificates and must not be treated as a cluster certificate authority.

Environment variable reference: [configuration.md](../configuration.md#environment-variables).

## Why external auth and inter-node auth are separate

| Surface | Listener | Client identity | Typical caller |
| --- | --- | --- | --- |
| External cache API | Primary `Cluster.Url` port | JWT bearer (when auth is enabled) | Application SDK, operators |
| Inter-node forwarding | `SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT` | mTLS client certificate (`CN` = peer `NodeId`, cluster CA) | Other Squirix nodes |

Reasons for the split:

- Application credentials must not be forwarded hop-by-hop inside the cluster.
- Inter-node calls are machine-to-machine; trust is established with node certificates, not end-user JWTs.
- Operational routes (`/health`, `/metrics`) stay on the primary listener with the existing auth model.
- Spoofing internal owner-routing metadata on the primary listener is rejected unless the call arrives on the internal
  mTLS listener with a trusted peer certificate.

External **client mTLS** (presenting a client certificate to the primary listener) is a separate, optional TLS concern.
The default application auth path is **JWT via `BearerTokenProvider`**, not client certificates.

## When inter-node mTLS is required

Inter-node mTLS is **mandatory at startup** when the cluster topology includes at least one **remote peer** (a peer
entry whose `NodeId` differs from the local `Cluster.NodeId`). Standalone nodes with no remote peers do not load cluster
mTLS material.

Multi-node settings must use **HTTPS** `Cluster.Url` values. Plaintext `http://` cluster URLs are rejected.

## Production model

1. Operate a **cluster CA** (or subordinate CA) through your PKI — for example an internal CA, cert-manager, or cloud
   private CA. Squirix only needs the PEM trust anchor at `SQUIRIX_CLUSTER_MTLS_CA_PATH`.
2. Issue a **unique node certificate** per Squirix node, signed by that CA. The certificate **common name (CN)** must
   equal that node's `Cluster.NodeId` (the same string as in `Peers[].NodeId`). Store the private key in a secret
   manager or mounted volume.
3. Configure **the same internal port number on every node** (`SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT`). It must differ from
   the primary listener port on each node. Outbound peer connections target `https://<peer-host>:<internal-port>`.
4. Configure **JWT/OIDC** for external clients on the primary listener using the existing `SQUIRIX_JWT_*` variables.
5. Rotate node certificates before expiry using your PKI workflow (see [Certificate rotation](#certificate-rotation-high-level)).

Squirix validates inter-node certificates in two steps:

1. **Trust** — build a chain to the configured cluster CA (`CustomRootTrust`), independent of the machine trust store.
2. **Identity** — require the certificate **CN** to match the expected cluster `NodeId` (ordinal string comparison).

At **startup**, loading fails when the local node certificate CN does not match `Cluster.NodeId`. On the **internal
listener**, inbound client certificates must chain to the cluster CA and present a CN equal to one of the configured
remote peer `NodeId` values. On **outbound** `ClientPool` calls, the peer server certificate must chain to the cluster
CA and its CN must equal the target peer's `NodeId`. A valid cluster CA signature alone is not sufficient when the CN
does not match the expected node identity.

Squirix reads certificate identity from the **subject CN** (`X509NameType.SimpleName`). SANs and other name forms are
not used for inter-node peer binding in v0.1.

### What Squirix does not support

- Trusting arbitrary self-signed certificates without an explicit cluster CA file at `SQUIRIX_CLUSTER_MTLS_CA_PATH`.
- Global `ServerCertificateCustomValidationCallback` overrides in product configuration.
- Using Squirix as the production certificate authority.
- Accepting a peer certificate that chains to the cluster CA but presents a different node's `NodeId` in the CN.

## Configuration

Cluster mTLS is configured only through environment variables (also used by containers and the standalone host):

| Variable | Purpose |
| --- | --- |
| `SQUIRIX_CLUSTER_MTLS_CA_PATH` | PEM cluster CA / trust root (required with remote peers) |
| `SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH` | Node PKCS#12/PFX (mutually exclusive with PEM pair) |
| `SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD` | Optional PFX password |
| `SQUIRIX_CLUSTER_MTLS_CERT_PATH` | PEM node certificate (requires key path) |
| `SQUIRIX_CLUSTER_MTLS_KEY_PATH` | PEM node private key |
| `SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT` | Dedicated internal HTTPS listener for inter-node gRPC mTLS |

Provide **either** a PFX **or** PEM cert + key, not both. Startup validation fails fast when files are missing, when
the internal port equals the primary port, or when the loaded node certificate CN does not match `Cluster.NodeId`.

The primary Kestrel certificate (ASP.NET Core development certificate, `ASPNETCORE_Kestrel__Certificates__Default__*`
in containers, or your ingress TLS termination) is **independent** from cluster mTLS material.

Optional `Peer.InterNodeUrl` in settings overrides the computed internal URL for advanced topologies; when omitted,
peers are reached at the same host as `Peer.Url` with the configured internal port.

## Inter-node JWT (not used)

Inter-node JWT propagation is **not** part of the product model. Cluster forwarding uses **mTLS on the internal
listener** instead:

- Forwarding nodes present a **trusted client certificate** on the internal transport.
- The internal gRPC surface allows anonymous authorization policy because **TLS client authentication** is the gate.
- External JWTs are **not** required for peer-to-peer forwarding when inter-node mTLS is enforced.
- External callers still need JWT (when auth is enabled) on the **primary** listener; they cannot satisfy internal
  forwarding checks with JWT alone.

## Certificate rotation (high level)

1. Issue replacement node certificates from the same cluster CA (or migrate to a new CA deliberately). Each
   replacement certificate must keep `CN` equal to the node's `Cluster.NodeId`.
2. Distribute new PFX/PEM files to each node (rolling update).
3. Restart nodes one at a time. Peers must trust the same CA during the rollout.
4. Revoke or retire old certificates in your PKI when no node presents them.

For CA rotation, dual-trust or a staged CA bundle change must be handled in your PKI; Squirix accepts a **single**
`SQUIRIX_CLUSTER_MTLS_CA_PATH` file at a time.

## Local and development clusters

Use **test-only** certificates that are never copied into production examples.

### Generate a local test CA and node certificates (OpenSSL)

The commands below create a dev CA and two node certificates with `CN=node-a` and `CN=node-b`. Those CNs must match
`Cluster.NodeId` / `Peers[].NodeId` in settings. Adjust paths, passwords, and distinguished names for your environment.

```powershell
# Dev-only — do not use in production
$dir = Join-Path $env:TEMP "squirix-dev-mtls"
New-Item -ItemType Directory -Force -Path $dir | Out-Null
Set-Location $dir

# Cluster CA
openssl genrsa -out cluster-ca.key 4096
openssl req -x509 -new -nodes -key cluster-ca.key -sha256 -days 30 `
  -out cluster-ca.crt -subj "/CN=Squirix Dev Cluster CA"

# Node A
openssl genrsa -out node-a.key 2048
openssl req -new -key node-a.key -out node-a.csr -subj "/CN=node-a"
openssl x509 -req -in node-a.csr -CA cluster-ca.crt -CAkey cluster-ca.key -CAcreateserial `
  -out node-a.crt -days 30 -sha256
openssl pkcs12 -export -out node-a.pfx -inkey node-a.key -in node-a.crt `
  -certfile cluster-ca.crt -passout pass:dev-mtls

# Node B (repeat with node-b names)
openssl genrsa -out node-b.key 2048
openssl req -new -key node-b.key -out node-b.csr -subj "/CN=node-b"
openssl x509 -req -in node-b.csr -CA cluster-ca.crt -CAkey cluster-ca.key -CAcreateserial `
  -out node-b.crt -days 30 -sha256
openssl pkcs12 -export -out node-b.pfx -inkey node-b.key -in node-b.crt `
  -certfile cluster-ca.crt -passout pass:dev-mtls
```

### Configure two local nodes

Assume primary URLs `https://localhost:5001` (node A) and `https://localhost:5002` (node B), each with `Peers[]`
listing both nodes in `Squirix.settings.json`. Use internal port **5101** on both nodes (must differ from primary
ports).

Node A:

```powershell
$env:SQUIRIX_CLUSTER_MTLS_CA_PATH = "$dir\cluster-ca.crt"
$env:SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH = "$dir\node-a.pfx"
$env:SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD = "dev-mtls"
$env:SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT = "5101"
```

Node B:

```powershell
$env:SQUIRIX_CLUSTER_MTLS_CA_PATH = "$dir\cluster-ca.crt"
$env:SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH = "$dir\node-b.pfx"
$env:SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD = "dev-mtls"
$env:SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT = "5101"
```

Mount the same files in containers at stable paths (for example `/mtls/cluster-ca.crt`, `/mtls/node.pfx`) and set the
`SQUIRIX_CLUSTER_MTLS_*` variables accordingly. See [containerization.md](../containerization.md#multi-node-inter-node-mtls).

### In-process and automated tests

`Squirix.Server.TestKit` generates ephemeral test certificates (for example `MtlsTestCertificates` and multi-node test
host helpers) for integration and E2E suites. That machinery is **test-only** and not a supported production workflow.

## Related documentation

- [configuration.md](../configuration.md) — full environment variable table
- [security/jwt-signing-keys.md](jwt-signing-keys.md) — external JWT signing and rotation
- [clustering.md](../clustering.md) — static topology and routing
- [server-mode.md](../server-mode.md) — standalone and embedded hosts
- [containerization.md](../containerization.md) — Docker layouts
