# Containerization

This repository includes two Dockerfiles and sample `docker-compose` files for running a small local squirix cluster.

Contents:

- `Dockerfile.dev`: multi-stage build from repository sources (matches the current checkout)
- `Dockerfile.release`: installs `squirix-server` from the `squirix.server.tool` NuGet package
- `Squirix.Server.Host`: standalone server executable that starts a node and waits
- `docker/docker-compose.yml`: two-node example (dev image)
- `docker/docker-compose.release.yml`: two-node example (release image, local package drop)
- `docker/node-a/Squirix.settings.json`
- `docker/node-b/Squirix.settings.json`

## Build (dev — from sources)

```powershell
cd docker
docker compose build
```

Or from the repository root:

```powershell
docker build -f Dockerfile.dev -t squirix-server:dev .
```

## Build (release — from NuGet tool)

Published releases (nuget.org):

```powershell
docker build -f Dockerfile.release -t squirix-server:0.1.0-preview.4 .
```

Verify a not-yet-published tool package locally:

```powershell
dotnet clean src/squirix.server.host/Squirix.Server.Host.csproj -c Release
dotnet pack src/squirix.server.host/Squirix.Server.Host.csproj -c Release -o docker/nuget-packages
docker build -f Dockerfile.release -t squirix-server:local `
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

Dev cluster (sets `SQUIRIX_API_KEYS=dev-docker-key` for both nodes; containers run with `Production` hosting
environment):

```powershell
cd docker
docker compose up -d
```

For a single local development node:

```powershell
docker build -f Dockerfile.dev -t squirix-server .
docker run --rm `
  -p 5000:5000 `
  -e SQUIRIX_API_KEYS=dev-docker-key `
  squirix-server run --urls https://0.0.0.0:5000
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

Images bundle a self-signed development PFX at `/https/aspnetapp.pfx` (password `dev-docker-cert`) with SANs for
`localhost`, `squirix-node-a`, `squirix-node-b`, and the release compose container names. Kestrel loads it via
`ASPNETCORE_Kestrel__Certificates__Default__Path` and `ASPNETCORE_Kestrel__Certificates__Default__Password`.

Use `curl -k` (or equivalent TLS skip/validation override) from the host. For .NET clients on the host, either trust the
exported cert or use development-only certificate validation overrides.

Example probes from the host:

```powershell
curl -k https://localhost:5001/health
curl -k -H "X-Api-Key: dev-docker-key" https://localhost:5001/metrics
```

Remote scrapes (including host → published container port) require authentication when `SQUIRIX_API_KEYS` is set.

gRPC and REST cache clients on the host must send the same API key (`options.ApiKey = "dev-docker-key"` or
`X-Api-Key: dev-docker-key`). TLS validation against the bundled cert requires `curl -k` or a development-only
certificate validation override in .NET.

Health and metrics:

- `GET /health`
- `GET /health/live`
- `GET /health/ready`
- `GET /health/ready/details`
- `GET /metrics` (Prometheus text scrape; enabled by default)

## Security

- Non-loopback listen URLs require `SQUIRIX_API_KEYS` and/or JWT settings at startup. The compose examples use
  `dev-docker-key` for local testing only.
- `/health` stays anonymous. `/metrics` follows the same auth rules as cache routes for remote clients;
  host → published port counts as remote inside the container. See [configuration.md](configuration.md) and
  [diagnostics.md](diagnostics.md).

## Persistence

Each node mounts a named Docker volume for persisted data. The current example is for local testing, not a production
deployment template.

Before adapting it for a real environment, read:

- [configuration.md](configuration.md)
- [operational-runbook.md](operational-runbook.md)
