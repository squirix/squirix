# squirix.server

`squirix.server` is the server-runtime library on NuGet (`Squirix.Server` assembly). The standalone process host
lives in the separate `Squirix.Server.Host` project (`squirix-server`).

| Package               | Purpose                                                                               |
|-----------------------|---------------------------------------------------------------------------------------|
| `squirix`             | v0.1 client SDK (`SquirixClient`, basic `ICache<T>`, `CacheEntryOptions`, serializer) |
| `squirix.server`      | Server runtime, hosting, durability, cluster owner routing, REST/gRPC host            |
| `squirix.server.tool` | Standalone `squirix-server` global tool (process host)                                |

`Squirix.Server` does not reference the `Squirix` client SDK assembly. Server-owned cache model types live under
`Squirix.Server.*`; wire compatibility with clients is through gRPC/REST contracts only.

Product code must not use `InternalsVisibleTo("Squirix.Server")`.

## Exported API

| Type                                                | Role                                                                                                              |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `SquirixServer`                                     | Test/sample lifetime: `StartAsync` + `DisposeAsync` (no exported configure callback; no listen URL on the handle) |
| `SquirixServerAspNetCoreExtensions`                 | `AddSquirixServer`, `MapSquirixServer` for custom ASP.NET Core hosts                                              |
| `SquirixServerConfiguration`                        | Load, validate, and map `Squirix.settings.json` (`Squirix:Cluster`)                                               |
| `SquirixServerOptions` / `SquirixServerPeerOptions` | Cluster topology; `UsePersistence()` enables WAL/snapshot durability                                              |

Full settings (memory pressure, snapshots, backpressure, metrics) are JSON-only; see
[docs/configuration.md](../../docs/configuration.md).

## Custom ASP.NET Core host

```csharp
// Discovered Squirix.settings.json is loaded when loadDiscoveredSettings is true (default).
builder.AddSquirixServer(options =>
{
    options.NodeId = "node-a";
    options.Url = new Uri("https://localhost:5001");
    options.UsePersistence("./data");
});

app.MapSquirixServer();
```

Explicit settings path or in-memory baseline:

```csharp
builder.AddSquirixServer(
    options => options.NodeId = "node-a",
    settingsPath: "Squirix.settings.json",
    loadDiscoveredSettings: false);
```

## Tests and samples

`SquirixServer.StartAsync` uses `SquirixServerConfiguration.LoadOrCreateDefault()` (discovered settings file, else an
ephemeral free HTTPS port). Pass the same URL to the client:

```csharp
var listenUrl = "https://localhost:5001"; // or from your Squirix.settings.json Cluster.Url
await using var server = await SquirixServer.StartAsync(cancellationToken);
await using var client = await SquirixClient.ConnectAsync(listenUrl, cancellationToken);
```

For options you control in code without a file, use `AddSquirixServer` on a `WebApplicationBuilder` instead of
`SquirixServer.StartAsync`.

Integration and smoke tests start nodes through `IntegrationTestBase.StartNodeAsync` or
`SmokeTestBase.StartNodeAsync` with optional `TestNodeSecurityOptions`. Smoke tests default to unauthenticated nodes via
an empty override; pass explicit API keys or JWT settings for auth scenarios. See
[configuration.md](../../docs/configuration.md#in-process-test-hosts).

Validate settings before deploy:

```powershell
squirix-server validate-config --settings Squirix.settings.json --strict
```

## Standalone host

The `squirix-server` executable uses the same `AddSquirixServer` / `MapSquirixServer` pipeline. Local dev defaults
listen on port **5001**:

```powershell
squirix-server init
squirix-server run
squirix-server run --persist --data-dir ./data
squirix-server doctor
```

Cache consumers use the `Squirix` client SDK over gRPC, not types from this package.
