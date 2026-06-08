# Getting started

This guide walks through running a squirix server and connecting a .NET client.

Prerequisites:

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) (version pinned in [`global.json`](../global.json))
- For local HTTPS clients outside the test harness: `dotnet dev-certs https --trust`

## 1. Run a development server

### NuGet global tool

```powershell
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.4
squirix-server run --data-dir ./data
```

The host listens on `https://localhost:5001` by default and prints a ready-to-use client snippet.

Optional HTTP/1 sidecar for `curl`, health, and admin probes:

```powershell
$env:SQUIRIX_HTTP1_PORT = "5002"
squirix-server run --data-dir ./data
```

### Docker (fastest if you have Docker Desktop)

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

Port **5000** is gRPC/HTTP/2 (map it for client apps). Port **5001** is the HTTP/1 sidecar.

Release image (pinned NuGet tool version):

```powershell
docker build -f Dockerfile.release -t squirix-server:0.1.0-preview.4 .
docker run --rm `
  -p 5000:5000 `
  -p 5001:5001 `
  -e SQUIRIX_HTTP1_PORT=5001 `
  -e SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL=true `
  -e SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL=true `
  squirix-server:0.1.0-preview.4 run --urls https://0.0.0.0:5000
```

More layouts: [containerization.md](containerization.md).

### From this repository

```powershell
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run --data-dir ./data
```

## 2. Add the client SDK

```powershell
dotnet add package squirix --version 0.1.0-preview.4
```

## 3. Connect and use a typed cache

Use the HTTPS gRPC endpoint from the host output. Local tool and source runs default to `https://localhost:5001`.
With the Docker example above, connect to `https://localhost:5000` (mapped gRPC port).

```csharp
using System.Threading;
using Squirix;

var cancellationToken = CancellationToken.None;

await using var client = await SquirixClient.ConnectAsync(
    "https://localhost:5001", // or https://localhost:5000 when using the Docker gRPC mapping
    cancellationToken);

var cache = await client.GetCacheAsync<string>("demo", cancellationToken);
await cache.SetAsync("greeting", "hello", cancellationToken: cancellationToken);

var lookup = await cache.GetValueAsync("greeting", cancellationToken);
Console.WriteLine(lookup.Found ? lookup.Value : "<missing>");
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
