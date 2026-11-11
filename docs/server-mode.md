# Server mode

Squirix servers run as a standalone `squirix-server` process or embedded in a custom ASP.NET Core host. Application
data access always goes through the `Squirix` client SDK, even when client and server share a process.

When running either server host on .NET 10 on Linux or macOS, use .NET 10.0.15 or a later .NET 10 patch.
Starting with preview.11, Squirix rejects older .NET 10 runtimes at startup because they can silently lose disk flush errors.

For install, Docker, and first connection steps, see [getting started](getting-started.md). For package roles, see
[client and server model](client-server.md).

## Standalone host

The `squirix-server` global tool uses the same runtime as the library host and listens on
`https://localhost:5001` by default. See [NuGet global tool](getting-started.md#nuget-global-tool) for installation and
startup commands, including durable mode.

Starting with preview.11, the standalone host reads `appsettings.json` and `appsettings.{Environment}.json` at startup.
Restart the host to apply changes to these files.

For Docker examples, see [containerization](containerization.md).

## Custom ASP.NET Core hosting

Embed the server runtime from the **`squirix.server`** NuGet package:

Create an ASP.NET Core project in a terminal:

```bash
dotnet dev-certs https --trust
dotnet new web --framework net10.0 -o SquirixHost
dotnet add SquirixHost/SquirixHost.csproj package squirix.server --version 0.1.0-preview.10
```

These commands use the published `0.1.0-preview.10` packages. See the [root README](../README.md) for the current version.

Replace `SquirixHost/Program.cs` with:

```csharp
using System;
using Microsoft.AspNetCore.Builder;
using Squirix.Server;

var builder = WebApplication.CreateBuilder(args);

// Loads Squirix.settings.json from the working directory when present.
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

Run `dotnet run --project SquirixHost --no-launch-profile`. The server listens at `https://localhost:5001`.
The example enables persistence in `data`, relative to the server's working directory. Stop it with Ctrl+C.
If port 5001 is unavailable, change `options.Uri` and use the same address in the client.

`AddSquirixServerAsync(...)` registers the server runtime and configures the primary Kestrel HTTPS listener (HTTP/1.1 and
HTTP/2 on one port). `MapSquirixServerAsync()` opens node storage (activated-topology check, journal startup repair, replica group logs) after
`Build()` and maps gRPC, health, and metrics endpoints; call it once before `StartAsync` or `RunAsync`.

## Loopback development default (not production posture)

The standalone host defaults to `https://localhost:5001` for local development. Before exposing the primary
listener or using a shared host, check the [loopback and JWT rules](configuration.md#loopback-trust-model).

Multi-node clusters with remote peers also open a **second HTTPS listener** on
`SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT` for internode gRPC with mutual TLS. External clients continue to use the primary
port with JWT/OIDC; cluster forwarding uses the internal port and per-node certificates (`CN` = `NodeId`). See
[security/internode-mtls.md](security/internode-mtls.md).

To use an explicit settings file, replace the registration call in the example above with:

```csharp
_ = await builder.AddSquirixServerAsync(settingsPath: "Squirix.settings.json");
```

The file must exist and contain valid cluster settings. Keep file loading enabled; `loadDiscoveredSettings: false`
skips even an explicit `settingsPath`. Optional runtime sections still use standard discovery as described in
[Configuration](configuration.md#file-discovery).

## Tests and samples

`SquirixServer.StartAsync` loads discovered `Squirix.settings.json` / `squirix.settings.json`, or creates ephemeral
defaults with a **free HTTPS port** when no such file exists. A discovered file that cannot be loaded or fails validation
(unparsable JSON, no `Squirix.Cluster` section, invalid values) makes `StartAsync` throw. The returned handle does **not** expose the listen URI, so do **not** assume
`https://localhost:5001` unless that origin is set as `Cluster.Uri` in an explicit settings file. Prefer
`AddSquirixServerAsync` when the client must know the listen URI:

To run both in one process, add the client package to the same web project:

```bash
dotnet add SquirixHost/SquirixHost.csproj package squirix --version 0.1.0-preview.10
```

Stop the previous server, then replace `SquirixHost/Program.cs` with this complete example. It starts a server,
writes and reads through the SDK, and disposes the client and host before exiting:

```csharp
using System;
using System.Threading;
using Microsoft.AspNetCore.Builder;
using Squirix.Client;
using Squirix.Server;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

var builder = WebApplication.CreateBuilder(args);
var listenUri = new Uri("https://localhost:5001");
_ = await builder.AddSquirixServerAsync(options => options.Uri = listenUri);
await using var app = builder.Build();
_ = await app.MapSquirixServerAsync(cancellationToken);
await app.StartAsync(cancellationToken);

await using var client = await SquirixClient.ConnectAsync(listenUri, cancellationToken);
var cache = await client.GetCacheAsync<string>("embedded-demo", cancellationToken);
await cache.SetAsync("greeting", "hello", null, cancellationToken);
var result = await cache.GetValueAsync("greeting", cancellationToken);
Console.WriteLine(result.Found ? result.Value : "<missing>");
```

Run `dotnet run --project SquirixHost --no-launch-profile`. Alongside the host logs, the example prints `hello`.
