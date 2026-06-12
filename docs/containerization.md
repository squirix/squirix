# Containerization

This repository includes two Dockerfiles and sample `docker-compose` files for running a small local squirix cluster.

Contents:

- `docker/Dockerfile`: multi-stage build from repository sources (matches the current checkout)
- `docker/Dockerfile.release`: installs `squirix-server` from the `squirix.server.tool` NuGet package
- `Squirix.Server.Host`: standalone server executable that starts a node and waits
- `docker/docker-compose.yml`: two-node example (sources image)
- `docker/docker-compose.release.yml`: two-node example (release image, local package drop)
- `docker/node-a/Squirix.settings.json`
- `docker/node-b/Squirix.settings.json`

## Build (from sources)

```powershell
cd docker
docker compose build
```

Or from the repository root:

```powershell
docker build -f docker/Dockerfile -t squirix-server .
```

## Build (release — from NuGet tool)

Published releases (nuget.org):

```powershell
docker build -f docker/Dockerfile.release -t squirix-server:0.1.0-preview.4 .
```

Verify a not-yet-published tool package locally:

```powershell
dotnet clean src/squirix.server.host/Squirix.Server.Host.csproj -c Release
dotnet pack src/squirix.server.host/Squirix.Server.Host.csproj -c Release -o docker/nuget-packages
docker build -f docker/Dockerfile.release -t squirix-server:local `
  --build-arg LOCAL_PACKAGES=true `
  --build-arg SQUIRIX_VERSION=0.1.0-preview.4 .
```

Or use the release compose file (expects packed `.nupkg` files in `docker/nuget-packages/`):

```powershell
dotnet clean src/squirix.server.host/Squirix.Server.Host.csproj -c Release
dotnet pack src/squirix.server.host/Squirix.Server.Host.csproj -c Release -o docker/nuget-packages
cd docker
docker compose -f docker-compose.release.yml build
```

## Run

Two-node cluster (sets sample JWT env vars for both nodes; containers run with `Production` hosting environment):

```powershell
cd docker
docker compose up -d
```

For a single local development node (ephemeral, in-memory):

```powershell
docker build -f docker/Dockerfile -t squirix-server .
docker run --rm `
  -p 5000:5000 `
  -e SQUIRIX_JWT_SIGNING_KEY=dev-squirix-docker-jwt-key!!!!!! `
  -e SQUIRIX_JWT_ISSUER=https://squirix.docker.dev `
  -e SQUIRIX_JWT_AUDIENCE=squirix `
  squirix-server run --urls https://0.0.0.0:5000
```

Durable single node with a host-mounted data directory:

```powershell
docker run --rm `
  -p 5000:5000 `
  -v squirix-data:/data `
  -e SQUIRIX_JWT_SIGNING_KEY=dev-squirix-docker-jwt-key!!!!!! `
  -e SQUIRIX_JWT_ISSUER=https://squirix.docker.dev `
  -e SQUIRIX_JWT_AUDIENCE=squirix `
  squirix-server run --urls https://0.0.0.0:5000 --persist --data-dir /data
```

The primary listener on port **5000** inside the container is HTTPS (HTTP/1.1 and HTTP/2). Map `-p 5000:5000` for host
access to gRPC, health, and metrics routes.

Endpoints (two-node `docker compose` example):

- Node A HTTPS (gRPC/health/metrics): `https://localhost:5001` (host port maps to container **5000**)
- Node B HTTPS: `https://localhost:5002`
- Inside each container, the listen URL is port **5000** from the mounted `Squirix.settings.json`.

Mounted settings use **Docker DNS hostnames** for cluster traffic (`https://squirix-node-a:5000`,
`https://squirix-node-b:5000`). Host applications use the **published** ports (`5001`, `5002`) instead. Each node's
`Cluster.Url` must match its local peer entry (see [configuration.md](configuration.md)).

## HTTPS in containers

Images bundle a self-signed development PFX at `/https/aspnetapp.pfx` (no export password) with SANs for `localhost`,
`squirix-node-a`, `squirix-node-b`, and the release compose container names. Kestrel loads it via
`ASPNETCORE_Kestrel__Certificates__Default__Path`.

Use `curl -k` (or equivalent TLS skip/validation override) from the host. For .NET clients on the host, either trust the
exported cert or use development-only certificate validation overrides.

Example probes from the host:

```powershell
curl -k https://localhost:5001/health
curl -k -H "Authorization: Bearer <jwt>" https://localhost:5001/metrics
```

Remote scrapes (including host → published container port) require a JWT bearer token when server auth is enabled.

gRPC clients on the host must send JWT via `options.BearerTokenProvider` or an
`Authorization: Bearer` header. TLS validation against the bundled cert requires `curl -k` or a development-only
certificate validation override in .NET.

Health and metrics:

- `GET /health`
- `GET /health/live`
- `GET /health/ready`
- `GET /health/ready/details`
- `GET /metrics` (Prometheus text scrape; enabled by default)

## Multi-node inter-node mTLS

The two-node compose layouts configure **external** JWT auth and a **primary** HTTPS listener (container port **5000**).
When `Peers[]` lists remote nodes, Squirix also requires **cluster mTLS** environment variables. The sample compose files
set `SQUIRIX_CLUSTER_MTLS_*` to **development-only** certificates baked into the image at `/mtls/` (`CN` matches each
node's `Cluster.NodeId`: `A` and `B` in the mounted settings). Squirix does not generate production certificates.

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
certificates with OpenSSL as described in [security/inter-node-mtls.md](security/inter-node-mtls.md#local-and-development-clusters).

Trust only the PEM cluster CA configured at `SQUIRIX_CLUSTER_MTLS_CA_PATH` and ensure each mounted node certificate
`CN` matches that container's `Cluster.NodeId`; do not add ad hoc certificate validation overrides.

## Security

- Non-loopback listen URLs require JWT settings at startup. The compose examples use a fixed dev signing key for local
  testing only.
- Multi-node clusters require cluster mTLS material in addition to JWT. See
  [security/inter-node-mtls.md](security/inter-node-mtls.md).
- `/health` stays anonymous. `/metrics` follows the same auth rules as cache routes for remote clients;
  host → published port counts as remote inside the container. See [configuration.md](configuration.md) and
  [diagnostics.md](diagnostics.md).

## Persistence

The `docker compose` examples mount a named Docker volume at `/data` and start each node with
`run --persist --data-dir /data`. Single-container snippets in [getting-started.md](getting-started.md) default to
ephemeral mode unless you add `--persist` and a data volume as shown above.

The compose layout is for local testing, not a production deployment template.

Before adapting it for a real environment, read:

- [configuration.md](configuration.md)
- [operational-runbook.md](operational-runbook.md)
