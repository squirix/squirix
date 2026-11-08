# Containerization

This repository includes two Dockerfiles and sample `docker-compose` files for running a small local squirix cluster.
Run all commands below from the repository root. Multiline commands use Bash syntax.

Contents:

- `docker/Dockerfile`: multi-stage build from repository sources (matches the current checkout)
- `docker/Dockerfile.release`: installs `squirix-server` from the `squirix.server.tool` NuGet package
- `Squirix.Server.Host`: standalone server executable that starts a node and waits
- `docker/docker-compose.yml`: three-node RF=3 HA example (sources image)
- `docker/docker-compose.release.yml`: three-node RF=3 example (release image, local package drop; secrets via `.env`)
- `docker/.env.example`: template for release compose (copy to `.env` for local testing only)
- `docker/node-a/Squirix.settings.json`
- `docker/node-b/Squirix.settings.json`
- `docker/node-c/Squirix.settings.json`

## Build (from sources)

```bash
docker compose -f docker/docker-compose.yml build
```

To build the source image directly:

```bash
docker build -f docker/Dockerfile -t squirix-server .
```

## Build (release — from NuGet tool)

Published releases (nuget.org). Image tag is arbitrary; the Dockerfile `SQUIRIX_VERSION` ARG defaults to the published
package version (override with `--build-arg` when needed):

```bash
docker build -f docker/Dockerfile.release -t squirix-server:release .
```

Verify a not-yet-published tool package locally. Packing from source requires the SDK selected by
[`global.json`](../global.json). The version below matches the current `SquirixPackageVersion`; if you change that
property, update the build argument to the same version:

```bash
dotnet clean src/squirix.server.host/Squirix.Server.Host.csproj -c Release
dotnet pack src/squirix.server.host/Squirix.Server.Host.csproj -c Release -o docker/nuget-packages
docker build -f docker/Dockerfile.release -t squirix-server:local \
  --build-arg LOCAL_PACKAGES=true \
  --build-arg SQUIRIX_VERSION=0.1.0-preview.10 .
```

Or use the release compose file (expects packed `.nupkg` files in `docker/nuget-packages/`). Its build arguments pin
`SQUIRIX_VERSION` separately; keep that value aligned with the package you packed. Runtime JWT settings and the mTLS
PFX password are required from the environment. Copy `docker/.env.example` to `docker/.env` for local testing:

The images still contain development HTTPS and mTLS private keys. Loading values from `.env` does not replace those
certificates or make the image suitable for production. Changing the PFX password variable alone cannot change the
password of the bundled PFX.

```bash
dotnet clean src/squirix.server.host/Squirix.Server.Host.csproj -c Release
dotnet pack src/squirix.server.host/Squirix.Server.Host.csproj -c Release -o docker/nuget-packages
cp docker/.env.example docker/.env
docker compose -f docker/docker-compose.release.yml build
```

## Run

> **Development only.** Compose files and `docker run` snippets below use **public test JWT and mTLS secrets**.
> They are fine on a local machine; copying them unchanged into staging or production lets anyone who knows the
> fixtures mint valid tokens. See [Security](#security) and [Generate per-environment secrets](#generate-per-environment-secrets).

Three-node RF=3 HA cluster (sets sample JWT env vars for all nodes; containers run with `Production` hosting environment):

```bash
docker compose -f docker/docker-compose.yml up -d
```

Release-image three-node RF=3 cluster (requires `docker/.env` — copy from `.env.example` for local testing):

```bash
cp docker/.env.example docker/.env
docker compose -f docker/docker-compose.release.yml up -d
```

For a single local development node (ephemeral, in-memory):

```bash
docker build -f docker/Dockerfile -t squirix-server .
docker run --rm \
  -p 5000:5000 \
  -e SQUIRIX_JWT_SIGNING_KEY=ZGV2LXNxdWlyaXgtZG9ja2VyLWp3dC1rZXkhISEhISE= \
  -e SQUIRIX_JWT_ISSUER=https://squirix.docker.dev \
  -e SQUIRIX_JWT_AUDIENCE=squirix \
  squirix-server run --urls https://0.0.0.0:5000
```

Durable single node with a named Docker volume mounted at `/data`:

```bash
docker run --rm \
  -p 5000:5000 \
  -v squirix-data:/data \
  -e SQUIRIX_JWT_SIGNING_KEY=ZGV2LXNxdWlyaXgtZG9ja2VyLWp3dC1rZXkhISEhISE= \
  -e SQUIRIX_JWT_ISSUER=https://squirix.docker.dev \
  -e SQUIRIX_JWT_AUDIENCE=squirix \
  squirix-server run --urls https://0.0.0.0:5000 --persist --data-dir /data
```

The primary listener on port **5000** inside the container is HTTPS (HTTP/1.1 and HTTP/2). Map `-p 5000:5000` for host
access to gRPC, health, and metrics routes.

Endpoints (three-node RF=3 `docker compose` example):

- Node A HTTPS (gRPC/health/metrics): `https://localhost:5001` (host port maps to container **5000**)
- Node B HTTPS: `https://localhost:5002`
- Node C HTTPS: `https://localhost:5003`
- Inside each container, the listen URL is port **5000** from the mounted `Squirix.settings.json`.

Mounted settings use **Docker DNS hostnames** for cluster traffic (`https://squirix-node-a:5000`,
`https://squirix-node-b:5000`, `https://squirix-node-c:5000`). Host applications use the **published** ports
(`5001`, `5002`, `5003`) instead. Each node's `Cluster.Uri` must match its local peer entry
(see [configuration.md](configuration.md)).

`Peers[].Uri` always carries the primary listener origin (port **5000** in the samples). The internode mTLS
gRPC transport endpoint is derived from that origin by swapping in `SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT`
(**5100**): keep each local peer entry aligned with its configured peer URI and never enter port `5100`
in `Peers[].Uri` — fingerprint and peer matching expect the primary origin.

## Replica factors

- RF=1 is a single copy with single-owner routing and no replication.
- RF=2 is a synchronous mirror only: writes require both replicas (majority is two), so losing either member
  stops RF=2 writes and no replacement is elected. Use RF=2 for mirroring, not for availability.
- The HA demo uses RF=3: three nodes with persistence and mTLS keep committing writes on the remaining majority
  while one follower is down. The example turns on [automatic failover](clustering.md#automatic-failover) and quorum
  reads, so when a node that leads a group goes down, the remaining two elect a new leader. Both switches are part of the
  topology fingerprint: remove the volumes of an earlier run (`docker compose -f docker/docker-compose.yml down -v`; this deletes the sample cluster data) before starting with changed
  settings.
  All three nodes must run the same homogeneous build; mixed package versions fail readiness through the topology
  fingerprint.
- RF>1 additionally requires the explicit replication opt-in (`Squirix:Cluster:ReplicationEnabled` or
  `--enable-replication`); without it RF>1 nodes refuse startup. The compose settings set the opt-in.

## HTTPS in containers

Both images bundle a self-signed development PFX at `/https/aspnetapp.pfx` with an empty export password. The source
image certificate has SANs for `localhost`, `127.0.0.1`, and `squirix-node-a`, `squirix-node-b`, `squirix-node-c`.
The release image additionally includes the `squirix-node-*-release` names. Kestrel loads the PFX via
`ASPNETCORE_Kestrel__Certificates__Default__Path`.

For a real deployment, configure your own trusted certificate for the primary HTTPS listener as well as separate
internode mTLS material. The bundled primary certificate does not cover arbitrary deployment hostnames.

For local development probes, use `curl -k` from the host. To connect with the .NET client from the host, trust the
container's exported public HTTPS certificate on the client machine. The public client API uses normal TLS validation.

Example probes from the host:

```bash
curl -k https://localhost:5001/health
curl -k -H "Authorization: Bearer <jwt>" https://localhost:5001/metrics
```

Remote scrapes (including host → published container port) require authentication with a valid JWT bearer token.
An unauthenticated remote request receives `401`; disabling JWT does not make remote scrapes anonymous.

gRPC clients must send JWT via `options.BearerTokenProvider` or an `Authorization: Bearer` header, and trust the server's
HTTPS certificate. The public client does not expose a certificate-validation override.
For a complete local example, use the [Docker client](../samples/docker-client/README.md): it generates a short-lived JWT
and trusts the exact server certificate inside an isolated client container, without changing the host trust store.
`curl -k` above is only for local diagnostic probes, not the client trust setup.

Health and metrics:

- `GET /health`
- `GET /health/live`
- `GET /health/ready`
- `GET /health/ready/details`
- `GET /metrics` (Prometheus text scrape; enabled by default)

## Multi-node internode mTLS

The three-node RF=3 compose layouts configure **external** JWT auth and a **primary** HTTPS listener (container port **5000**).
When `Peers[]` lists remote nodes, Squirix also requires **cluster mTLS** environment variables. The sample compose files
set `SQUIRIX_CLUSTER_MTLS_*` to **development-only** certificates baked into the image at `/mtls/` (`CN` matches each
node's `Cluster.NodeId`: `A`, `B`, and `C` in the mounted settings). Squirix does not generate production certificates.

Example additions per service (adjust paths to match your image layout):

```yaml
environment:
  SQUIRIX_CLUSTER_MTLS_CA_PATH: /mtls/cluster-ca.crt
  SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH: /mtls/node.pfx
  SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD: dev-mtls
  SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT: "5100"
volumes:
  - ./mtls/node-a:/mtls:ro
```

Use the **same internal port number on every node** (here `5100`). It must differ from the primary listener port
(`5000` in the sample settings). Peers connect to `https://<service-host>:5100` on the Docker network. Generate dev
certificates with OpenSSL as described in [security/internode-mtls.md](security/internode-mtls.md#local-and-development-clusters).

Trust only the PEM cluster CA configured at `SQUIRIX_CLUSTER_MTLS_CA_PATH` and ensure each mounted node certificate
`CN` matches that container's `Cluster.NodeId`; do not add ad hoc certificate validation overrides.

## Security

> **Do not deploy Docker examples unchanged to production.** `docker-compose.yml` and single-container `docker run`
> snippets embed predictable JWT signing keys and reference **development-only** mTLS material baked into images at
> `/mtls/`. Anyone with repository access can forge JWTs against those keys.

- Non-loopback listen URLs require JWT settings at startup. The compose examples use a fixed dev signing key for local
  testing only — see [security/jwt-signing-keys.md](security/jwt-signing-keys.md).
- `docker-compose.release.yml` requires explicit environment values; `docker compose up` fails until you set
  `SQUIRIX_JWT_*` and `SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD` (for local verification, copy
  [docker/.env.example](../docker/.env.example) to `docker/.env` — never commit `.env`).
- Multi-node clusters require cluster mTLS material in addition to JWT. See
  [security/internode-mtls.md](security/internode-mtls.md).
- For production, prefer **OIDC/JWKS** (`SQUIRIX_JWT_AUTHORITY`) over symmetric `SQUIRIX_JWT_SIGNING_KEY`, mount your
  own cluster CA and node certificates, and store secrets in a secret manager — not in compose files or images.
- `/health`, `/health/live`, and `/health/ready` stay anonymous. `/metrics` and `/health/ready/details` follow the
  same auth rules as cache routes for remote clients; host → published port counts as remote inside the container.
  See [configuration.md](configuration.md) and [diagnostics.md](diagnostics.md).

## Generate per-environment secrets

Use **distinct** JWT and mTLS material per environment (dev, staging, production). Never reuse the Docker dev fixtures.

### Symmetric JWT signing key

Generate at least 32 random bytes and base64-encode them. Startup checks only base64 syntax and length, not randomness. Example with OpenSSL:

```bash
openssl rand -base64 48
```

Set the same value on every Squirix node (`SQUIRIX_JWT_SIGNING_KEY`) and on every client or scraper that mints bearer
tokens. Also set `SQUIRIX_JWT_ISSUER` and `SQUIRIX_JWT_AUDIENCE` to match your environment. Details:
[security/jwt-signing-keys.md](security/jwt-signing-keys.md).

### Cluster mTLS certificates

Generate a private cluster CA and per-node PKCS#12 files with OpenSSL. Each certificate `CN` must match that node's
`Cluster.NodeId`. Full walkthrough:
[security/internode-mtls.md#generate-a-local-test-ca-and-node-certificates-openssl](security/internode-mtls.md#generate-a-local-test-ca-and-node-certificates-openssl).

Mount the CA and node PFX (or PEM pair) into containers or hosts and point `SQUIRIX_CLUSTER_MTLS_*` at those paths.
Use a strong, unique PFX password per environment.

## Persistence

The `docker compose` examples mount a named Docker volume at `/data` and start each node with
`run --persist --data-dir /data`. Single-container snippets in [getting-started.md](getting-started.md) default to
ephemeral mode unless you add `--persist` and a data volume as shown above.

The compose layout is for local testing, not a production deployment template.

Before adapting it for a real environment, read:

- [configuration.md](configuration.md)
- [operational-runbook.md](operational-runbook.md)
