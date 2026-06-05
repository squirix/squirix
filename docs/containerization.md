# Containerization

This repository includes a Dockerfile and a sample `docker-compose` file for running a small local squirix cluster.

Contents:

- `Dockerfile`: multi-stage build for a minimal node host image
- `Squirix.Server.Host`: standalone server executable that starts a node and waits
- `docker/docker-compose.yml`: two-node example
- `docker/node-a/Squirix.settings.json`
- `docker/node-b/Squirix.settings.json`

## Build

```powershell
cd docker
docker compose build
```

## Run

```powershell
docker compose up -d
```

For a single local development node:

```powershell
docker build -t squirix-server .
docker run --rm -p 5001:5001 -e SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL=true squirix-server run --urls http://0.0.0.0:5001
```

Endpoints (two-node `docker compose` example):

- Node A HTTP/1 sidecar (health/admin/metrics): `http://localhost:5001`
- Node B HTTP/1 sidecar: `http://localhost:5002`
- Inside each container, gRPC/HTTP/2 uses port **5000** from the mounted `Squirix.settings.json`; the sidecar is
  `SQUIRIX_HTTP1_PORT=5001`.

For the single-container `docker run` example above, health and admin are on the primary listener at port **5001**
(no separate sidecar env var).

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
