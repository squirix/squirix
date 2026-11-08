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
| `AspNetCoreExtensions`                              | `AddSquirixServerAsync`, `MapSquirixServerAsync` for custom ASP.NET Core hosts                                    |
| `Configurator`                                      | Async load, validate, and map `Squirix.settings.json` (`Squirix:Cluster`)                                         |
| `SquirixServerOptions` / `SquirixServerPeerOptions` | Cluster topology; `UsePersistence()` enables journal/snapshot durability                                          |

Full settings references (memory pressure, snapshots, metrics, and which knobs are JSON vs host defaults): see
[docs/configuration.md](../../docs/configuration.md).

## Custom ASP.NET Core host

The fragment below is for an ASP.NET Core host with the server package installed. For complete project setup, see
[Server mode](../../docs/server-mode.md#custom-aspnet-core-hosting).

```csharp
using System;
using Microsoft.AspNetCore.Builder;
using Squirix.Server;

var builder = WebApplication.CreateBuilder(args);

// Discovered Squirix.settings.json is loaded when loadDiscoveredSettings is true (default).
_ = await builder.AddSquirixServerAsync(static options =>
{
    options.NodeId = "node-a";
    options.Uri = new Uri("https://localhost:5001");
    options.UsePersistence("data");
});

await using var app = builder.Build();
_ = await app.MapSquirixServerAsync();
await app.RunAsync();
```

To replace the registration above with an explicit cluster settings file:

```csharp
_ = await builder.AddSquirixServerAsync(
    static options => options.NodeId = "node-a",
    settingsPath: "Squirix.settings.json");
```

For a baseline configured entirely in code, pass `loadDiscoveredSettings: false` instead and configure all required
cluster options in the callback. That flag disables loading even an explicit `settingsPath`. Optional runtime sections
still follow their own standard file discovery; see [file discovery](../../docs/configuration.md#file-discovery).

## Tests and samples

`SquirixServer.StartAsync` uses `Configurator.LoadOrCreateDefaultAsync` (discovered settings file, else an ephemeral
free HTTPS port). A discovered settings file that cannot be loaded or fails validation (unparsable JSON, no
`Squirix.Cluster` section, invalid values) throws instead of falling back to defaults.
The returned handle does **not** expose the listen URI. With no discovered file, its port is chosen dynamically;
do not assume port 5001. For a known endpoint, create a standard settings file first with `squirix-server init` in the
working directory. With both client and server packages installed, load that same file to obtain `Cluster.Uri`:

```csharp
using System;
using System.Threading;
using Squirix.Client;
using Squirix.Server;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

var settings = await Configurator.LoadAsync("Squirix.settings.json", cancellationToken);
await using var server = await SquirixServer.StartAsync(cancellationToken);
await using var client = await SquirixClient.ConnectAsync(settings.Uri, cancellationToken);
```

For options you control in code without a file, use `await builder.AddSquirixServerAsync(...)` on a
`WebApplicationBuilder` instead of `SquirixServer.StartAsync`.

Integration and smoke tests start nodes through `NodeIntegrationTestBase.StartClusterAsync` or
`SmokeTestBase.StartClusterAsync`, passing optional `TestNodeSecurityOptions` through the `Security` property of the
start options (`IntegrationStartOptions` in the integration tests, `BlackBoxStartOptions` from
`Squirix.Server.TestKit.Hosting` in the smoke tests). Smoke tests default to unauthenticated nodes via an empty
override; pass explicit JWT settings for auth scenarios. See
[configuration.md](../../docs/configuration.md#in-process-test-hosts).

Validate settings before deploy:

```bash
squirix-server validate-config --settings Squirix.settings.json --strict
```

## Standalone host

The `squirix-server` executable uses the same `AddSquirixServerAsync` / `MapSquirixServerAsync` pipeline. Local dev defaults
listen on port **5001**:

```bash
squirix-server init
squirix-server run
squirix-server run --persist --data-dir data
squirix-server doctor
```

Cache consumers use the `Squirix` client SDK over gRPC, not types from this package.
