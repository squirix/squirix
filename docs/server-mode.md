# Server mode

squirix is a client/server distributed cache engine. The production topology is:

```text
application -> Squirix client SDK -> squirix server cluster
```

Server deployments use the `Squirix.Server.Host` executable (`squirix-server`), which references the `squirix.server`
runtime library on NuGet. Client applications reference the `squirix` package and connect explicitly:

```csharp
await using var client = await SquirixClient.ConnectAsync(
    "https://localhost:5001",
    cancellationToken);

var cache = await client.GetCacheAsync<User>("users", cancellationToken);
await cache.SetAsync("42", user, cancellationToken: cancellationToken);
```

Multiple bootstrap endpoints are configured through client options:

```csharp
await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add("https://squirix-a:5001");
        options.Endpoints.Add("https://squirix-b:5002");
    },
    cancellationToken);
```

Bootstrap URLs must be interchangeable HA views of the same service, not independent shards. Connect uses any-up
semantics; transport failover for v0.1 exported operations is documented in
[bootstrap client failover](bootstrap-client-failover.md).

`Squirix.Server` owns server runtime and hosting: data placement, partition ownership, static cluster topology, owner
routing, server-side mutation execution, Kestrel setup, REST/gRPC host composition, durability services,
journal/snapshot/recovery orchestration, backpressure, memory pressure, and health/admin/security/metrics endpoints.
`Squirix.Server.Host` owns the standalone process lifecycle.

`Squirix` owns the v0.1 exported client SDK (`SquirixClient`, `SquirixOptions`, basic `ICache<T>`, `CacheEntryOptions`,
entry/result types, `ISquirixSerializer`), typed client facade, and client-side connection/routing/retry behavior for
those exported operations. Mutations take `(key, value, options?, cancellationToken)`; expiration metadata uses
`CacheEntryOptions`, not `CacheEntry<T>` write overloads.

## Standalone host CLI

The standalone executable provides a low-friction local node. Install from NuGet, Docker, or run from this repository:

```powershell
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.4
squirix-server run --dev --data-dir ./data
```

From source:

```powershell
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run --dev --data-dir ./data
```

Docker single-node example (dev image from sources):

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

Release image (NuGet tool; requires `squirix.server.tool` on nuget.org):

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

Operational commands:

```powershell
squirix-server init [--settings ./Squirix.settings.json]
squirix-server validate-config --settings ./Squirix.settings.json
squirix-server validate-config --settings ./Squirix.settings.json --strict
squirix-server doctor [--settings ./Squirix.settings.json] [--strict]
squirix-server version
```

`run` accepts `--urls`, `--data-dir`, `--settings`, `--dev`, and `--strict`. Without `--settings`, the host discovers
`Squirix.settings.json` or `squirix.settings.json` in the working directory and application directory. `--dev` starts a
local HTTPS gRPC node at `https://localhost:5001`; set `SQUIRIX_HTTP1_PORT` for a browser-friendly HTTP/1
health/admin sidecar. `--strict` on `validate-config` and `doctor` also validates optional `MemoryPressure` and
`PrometheusMetrics` sections when present.

## Custom ASP.NET Core hosting

Custom hosts embed the server runtime from the **`squirix.server`** NuGet package in an ASP.NET Core process:

```powershell
dotnet add package squirix.server --version 0.1.0-preview.4
```

```csharp
var builder = WebApplication.CreateBuilder(args);

// Loads Squirix.settings.json from the working directory when present.
builder.AddSquirixServer(options =>
{
    options.NodeId = "node-a";
    options.Url = new Uri("https://localhost:5001");
    options.DataDirectory = "./data";
});

var app = builder.Build();
app.MapSquirixServer();
await app.RunAsync();
```

`AddSquirixServer(...)` registers the server runtime and configures the primary Kestrel listener. `MapSquirixServer()`
maps gRPC, REST, health, admin, and metrics endpoints.

This embeds the server runtime in an ASP.NET Core process. Application data access still goes through the `Squirix` client
SDK and `SquirixClient.ConnectAsync(...)`, even when client and server run in the same process.
