# Getting started

This guide walks through running a squirix server and connecting a .NET client.

Prerequisites:

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) (version pinned in [`global.json`](../global.json))
- For local HTTPS (tests, benchmarks, examples; not Docker): `dotnet dev-certs https --trust`

## 1. Run a development server

### NuGet global tool

```bash
dotnet tool install --global squirix.server.tool --version <version>
squirix-server run
```

Pin `<version>` from the [root README](../README.md) (same value as the NuGet badge).

The host listens on `https://localhost:5001` by default (loopback bind), runs as an in-memory cache, and prints
ready-to-use client and operational endpoint URLs. **No JWT is required** on this default URL — any local process can
access the cache API. This is a development convenience, not production hardening. See
[server-mode.md — Loopback development default](server-mode.md#loopback-development-default-not-production-posture).

For journal/snapshot durability:

```bash
squirix-server run --persist --data-dir ./data
```

Health probes use the same HTTPS listener (local tool default port **5001**):

```bash
curl -k https://localhost:5001/health
curl -k https://localhost:5001/metrics
```

`/metrics` is anonymous on loopback when auth is not configured.

### Docker (fastest if you have Docker Desktop)

> **Development only.** Examples below use the public test JWT key `dev-squirix-docker-jwt-key!!!!!!` and bundled dev
> HTTPS/mTLS certificates. Do not reuse them outside a local machine.
> See [containerization.md](containerization.md#security).

Single-container examples start in the default **ephemeral** mode (in-memory cache). The two-node `docker compose`
example enables persistence with `--persist --data-dir /data` and named volumes.

```bash
docker build -f docker/Dockerfile -t squirix-server .
docker run --rm \
  -p 5000:5000 \
  -e SQUIRIX_JWT_SIGNING_KEY=dev-squirix-docker-jwt-key!!!!!! \
  -e SQUIRIX_JWT_ISSUER=https://squirix.docker.dev \
  -e SQUIRIX_JWT_AUDIENCE=squirix \
  squirix-server run --urls https://0.0.0.0:5000
```

Port **5000** is the primary HTTPS listener (gRPC, `/health`, `/metrics`). Images ship a bundled development
HTTPS certificate; use `curl -k` from the host. When JWT is configured, pass a bearer token for `/metrics` scrapes from
outside the container.

Release image (NuGet tool; Dockerfile default `SQUIRIX_VERSION` matches the published package):

```bash
docker build -f docker/Dockerfile.release -t squirix-server:release .
docker run --rm \
  -p 5000:5000 \
  -e SQUIRIX_JWT_SIGNING_KEY=dev-squirix-docker-jwt-key!!!!!! \
  -e SQUIRIX_JWT_ISSUER=https://squirix.docker.dev \
  -e SQUIRIX_JWT_AUDIENCE=squirix \
  squirix-server:release run --urls https://0.0.0.0:5000
```

Two-node cluster (`docker compose up -d` in `docker/`): node A on `https://localhost:5001`, node B on
`https://localhost:5002` (host ports map to container **5000**). See [containerization.md](containerization.md).

### From this repository

```bash
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run
```

## 2. Add the client SDK

```bash
dotnet add package squirix --version <version>
```

Pin `<version>` from the [root README](../README.md).

## 3. Connect and use a typed cache

Use the HTTPS gRPC endpoint from the host output.

**Local tool or `dotnet run`** (default `https://localhost:5001`, no JWT unless you configure auth):

```csharp
using System;
using System.Threading;
using Squirix.Client;

var cancellationToken = CancellationToken.None;

await using var client = await SquirixClient.ConnectAsync(new Uri("https://localhost:5001"), cancellationToken);

var cache = await client.GetCacheAsync<string>("demo", cancellationToken);
await cache.SetAsync("greeting", "hello", cancellationToken: cancellationToken);

var lookup = await cache.GetValueAsync("greeting", cancellationToken);
Console.WriteLine(lookup.Found ? lookup.Value : "<missing>");
```

**Docker** (JWT env vars in the examples): single-container `https://localhost:5000`; Compose node A
`https://localhost:5001`. Use `options.BearerTokenProvider` with a JWT signed by the docker dev key and a development
TLS validation override when connecting from the host (see [containerization.md](containerization.md#https-in-containers)).

```csharp
using System;
using System.Threading.Tasks;
using Squirix.Client;

await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add(new Uri("https://localhost:5000")); // or :5001 for Compose node A
        options.BearerTokenProvider = _ => new ValueTask<string>(yourJwtBearerToken);
    },
    cancellationToken);
```

Multiple bootstrap endpoints (HA front door, not shards):

```csharp
using System;
using Squirix.Client;

await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add(new Uri("https://cache-a.example.internal:5001"));
        options.Endpoints.Add(new Uri("https://cache-b.example.internal:5002"));
    },
    cancellationToken);
```

See [bootstrap client failover](bootstrap-client-failover.md) and [configuration](configuration.md).

## CLI reference

```bash
squirix-server init [--settings ./Squirix.settings.json]
squirix-server validate-config --settings ./Squirix.settings.json [--strict]
squirix-server doctor [--settings ./Squirix.settings.json] [--strict]
squirix-server version
```

`run` accepts `--urls`, `--persist`, `--data-dir` (with `--persist`), `--settings`, and `--strict`. Without
`--settings`, the host discovers `Squirix.settings.json` or `squirix.settings.json` in the working directory and
application directory.

## Next steps

- Embed the server in ASP.NET Core: [server mode](server-mode.md)
- Tune cluster / memory-pressure / metrics settings: [configuration](configuration.md)
- Understand routing and consistency: [clustering](clustering.md)
