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

Dev cluster:

```powershell
cd docker
docker compose up -d
```

For a single local development node (HTTP/1 sidecar on 5001 for browser/curl health checks):

```powershell
docker build -f Dockerfile.dev -t squirix-server .
docker run --rm `
  -p 5000:5000 `
  -p 5001:5001 `
  -e SQUIRIX_HTTP1_PORT=5001 `
  -e SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL=true `
  -e SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL=true `
  squirix-server run --urls https://0.0.0.0:5000
```

The primary listener on port **5000** inside the container is HTTPS HTTP/2 (gRPC).
Map `-p 5000:5000` when client apps on the host need gRPC access.
Without `SQUIRIX_HTTP1_PORT`, plain HTTP/1 tools such as `curl` against the mapped port will fail.

Endpoints (two-node `docker compose` example):

- Node A HTTP/1 sidecar (health/admin/metrics): `http://localhost:5001`
- Node B HTTP/1 sidecar: `http://localhost:5002`
- Inside each container, gRPC/HTTP/2 uses port **5000** from the mounted `Squirix.settings.json`; the sidecar is
  `SQUIRIX_HTTP1_PORT=5001`.

For the single-container `docker run` example above, health and admin are on the HTTP/1 sidecar at port **5001**
(`SQUIRIX_HTTP1_PORT=5001`; map host port 5001 to container 5001).

Health and metrics:

- `GET /health`
- `GET /health/live`
- `GET /health/ready`
- `GET /health/ready/details`
- `GET /metrics` (Prometheus text scrape; enabled by default)

Admin helpers:

- `GET /admin/whoami`
- `GET /admin/owner/{key}`
- `GET /admin/ring`

## Security

- API keys can be enabled by setting `SQUIRIX_API_KEYS` on each service.
- The HTTP/1.1 sidecar created by `SQUIRIX_HTTP1_PORT` is plaintext and intended for local/dev use.

## Persistence

Each node mounts a named Docker volume for persisted data. The current example is for local testing, not a production
deployment template.

Before adapting it for a real environment, read:

- [configuration.md](configuration.md)
- [operational-runbook.md](operational-runbook.md)
