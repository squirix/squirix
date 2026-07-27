# squirix.server

`squirix.server` is the server-runtime library on NuGet (`Squirix.Server` assembly). The standalone CLI is published as
**`squirix.server.tool`**; the command is **`squirix-server`** (`Squirix.Server.Host` project).

| Package               | Purpose                                                                                |
|-----------------------|----------------------------------------------------------------------------------------|
| `squirix`             | v0.1 client SDK (`SquirixClient`, basic `ICache<T>`, `CacheEntryOptions`, serializer)  |
| `squirix.server`      | Server runtime, hosting, durability, cluster owner routing, gRPC + health/metrics host |
| `squirix.server.tool` | Standalone `squirix-server` global tool (process host)                                 |

`Squirix.Server` does not reference the `Squirix` client SDK assembly. Server-owned cache model types live under
`Squirix.Server.*`; wire compatibility with clients is through the shared gRPC proto contract only.

Product code must not use `InternalsVisibleTo("Squirix.Server")`.

## Exported API

| Type                                                | Role                                                                                                              |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `SquirixServer`                                     | Test/sample lifetime: `StartAsync` + `DisposeAsync` (no exported configure callback; no listen URI on the handle) |
| `AspNetCoreExtensions`                              | `AddSquirixServerAsync`, `MapSquirixServer` for custom ASP.NET Core hosts                                         |
| `Configurator`                                      | Async load, validate, and map `Squirix.settings.json` (`Squirix:Cluster`)                                         |
| `SquirixServerOptions` / `SquirixServerPeerOptions` | Cluster topology; `UsePersistence()` enables journal/snapshot durability                                          |

Full settings references (memory pressure, snapshots, metrics, and which knobs are JSON vs host defaults): see
[docs/configuration.md](../../docs/configuration.md).

## Custom ASP.NET Core host

```csharp
var builder = WebApplication.CreateBuilder(args);

// Discovered Squirix.settings.json is loaded when loadDiscoveredSettings is true (default).
await builder.AddSquirixServerAsync(options =>
{
    options.NodeId = "node-a";
    options.Uri = new Uri("https://localhost:5001");
    options.UsePersistence("./data");
});

var app = builder.Build();
app.MapSquirixServer();
await app.RunAsync();
```

Explicit settings path or in-memory baseline:

```csharp
await builder.AddSquirixServerAsync(
    options => options.NodeId = "node-a",
    settingsPath: "Squirix.settings.json",
    loadDiscoveredSettings: false);
```

## Tests and samples

`SquirixServer.StartAsync` uses `Configurator.LoadOrCreateDefaultAsync` (discovered settings file, else an ephemeral
**free HTTPS port**). The returned handle does **not** expose the listen URI — do **not** assume
`https://localhost:5001` unless `Cluster.Uri` in an explicit settings file says so. Prefer
`AddSquirixServerAsync` when the client must know the listen URI:

```csharp
using System;
using Microsoft.AspNetCore.Builder;
using Squirix.Client;
using Squirix.Server;

var builder = WebApplication.CreateBuilder(args);
var listenUri = new Uri("https://localhost:5001");
await builder.AddSquirixServerAsync(options => options.Uri = listenUri);
var app = builder.Build();
app.MapSquirixServer();
await app.StartAsync(cancellationToken);

await using var client = await SquirixClient.ConnectAsync(listenUri, cancellationToken);
```

Integration and smoke tests start nodes through `NodeIntegrationTestBase.StartNodeAsync` or
`SmokeTestBase.StartNodeAsync` with optional `TestNodeSecurityOptions`. Smoke tests default to unauthenticated nodes
via an empty override; pass explicit JWT settings for auth scenarios. See
[configuration.md](../../docs/configuration.md#in-process-test-hosts).

Validate settings before deploy:

```bash
squirix-server validate-config --settings Squirix.settings.json --strict
```

## Standalone host

The `squirix-server` executable uses the same `AddSquirixServerAsync` / `MapSquirixServer` pipeline. Local dev defaults
listen on port **5001**:

```bash
squirix-server init
squirix-server run
squirix-server run --persist --data-dir ./data
squirix-server doctor
```

Cache consumers use the `Squirix` client SDK over gRPC, not types from this package.
