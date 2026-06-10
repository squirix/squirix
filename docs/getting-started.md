# Getting started

This guide walks through running a squirix server and connecting a .NET client.

Prerequisites:

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) (version pinned in [`global.json`](../global.json))
- For local HTTPS (tests, benchmarks, examples; not Docker): `dotnet dev-certs https --trust`

## 1. Run a development server

### NuGet global tool

```powershell
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.4
squirix-server run --data-dir ./data
```

The host listens on `https://localhost:5001` by default and prints ready-to-use client and operational endpoint URLs.

Health probes use the same HTTPS listener (local tool default port **5001**):

```powershell
curl -k https://localhost:5001/health
curl -k https://localhost:5001/metrics
```

`/metrics` is anonymous on loopback when auth is not configured.

### Docker (fastest if you have Docker Desktop)

```powershell
docker build -f Dockerfile.dev -t squirix-server .
docker run --rm `
  -p 5000:5000 `
  -e SQUIRIX_API_KEYS=dev-docker-key `
  -e SQUIRIX_ADMIN_ENABLED=true `
  squirix-server run --urls https://0.0.0.0:5000
```

Port **5000** is the primary HTTPS listener (gRPC, `/health`, `/metrics`, `/admin`). Images ship a bundled development
HTTPS certificate; use `curl -k` from the host. When `SQUIRIX_API_KEYS` is set, pass `X-Api-Key` for `/metrics` scrapes
from outside the container.

Release image (pinned NuGet tool version):

```powershell
docker build -f Dockerfile.release -t squirix-server:0.1.0-preview.4 .
docker run --rm `
  -p 5000:5000 `
  -e SQUIRIX_API_KEYS=dev-docker-key `
  -e SQUIRIX_ADMIN_ENABLED=true `
  squirix-server:0.1.0-preview.4 run --urls https://0.0.0.0:5000
```

Two-node cluster (`docker compose up -d` in `docker/`): node A on `https://localhost:5001`, node B on
`https://localhost:5002` (host ports map to container **5000**). See [containerization.md](containerization.md).

### From this repository

```powershell
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run --data-dir ./data
```

## 2. Add the client SDK

```powershell
dotnet add package squirix --version 0.1.0-preview.4
```

## 3. Connect and use a typed cache

Use the HTTPS gRPC endpoint from the host output.

**Local tool or `dotnet run`** (default `https://localhost:5001`, no API key unless you configure auth):

```csharp
using System.Threading;
using Squirix;

var cancellationToken = CancellationToken.None;

await using var client = await SquirixClient.ConnectAsync("https://localhost:5001", cancellationToken);

var cache = await client.GetCacheAsync<string>("demo", cancellationToken);
await cache.SetAsync("greeting", "hello", cancellationToken: cancellationToken);

var lookup = await cache.GetValueAsync("greeting", cancellationToken);
Console.WriteLine(lookup.Found ? lookup.Value : "<missing>");
```

**Docker** (`SQUIRIX_API_KEYS` in the examples): single-container `https://localhost:5000`; Compose node A
`https://localhost:5001`. Use `options.ApiKey = "dev-docker-key"` and a development TLS validation override when
connecting from the host (see [containerization.md](containerization.md#https-in-containers)).

```csharp
await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add("https://localhost:5000"); // or :5001 for Compose node A
        options.ApiKey = "dev-docker-key";
    },
    cancellationToken);
```

Multiple bootstrap endpoints (HA front door, not shards):

```csharp
await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add("https://cache-a.example.internal:5001");
        options.Endpoints.Add("https://cache-b.example.internal:5002");
    },
    cancellationToken);
```

See [bootstrap client failover](bootstrap-client-failover.md) and [configuration](configuration.md).

## CLI reference

```powershell
squirix-server init [--settings ./Squirix.settings.json]
squirix-server validate-config --settings ./Squirix.settings.json [--strict]
squirix-server doctor [--settings ./Squirix.settings.json] [--strict]
squirix-server version
```

`run` accepts `--urls`, `--data-dir`, `--settings`, and `--strict`. Without `--settings`, the host discovers
`Squirix.settings.json` or `squirix.settings.json` in the working directory and application directory.

## Next steps

- Embed the server in ASP.NET Core: [server mode](server-mode.md)
- Tune cluster and persistence settings: [configuration](configuration.md)
- Understand routing and consistency: [clustering](clustering.md)
