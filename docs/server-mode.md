# Server mode

Run a Squirix node with the `squirix-server` tool or embed it using the `squirix.server` library.
In both modes, applications can use the Squirix client SDK over the binary TLS listener. The
[RESP cache endpoint](resp.md) for Redis clients listens on `127.0.0.1:6379` by default.

For the .NET 10 build on Linux and macOS, use .NET 10.0.15 or a later .NET 10 patch.
Earlier .NET 10 builds can silently lose disk flush errors and are rejected at startup.

## Standalone host

The global tool listens on `https://localhost:5001` for binary clients by default. The HTTP/1.1 management listener
binds to loopback by default. With a certificate for `localhost`, the default origin is `https://localhost:5003`.
Otherwise the default host is `127.0.0.1`. The default port is two above the client port, or two below it near the
end of the TCP port range. An adjacent port is selected if that default would overlap the configured peer port.
Set `SQUIRIX_MANAGEMENT_URI` to select a specific HTTPS origin covered by the server certificate. A DNS name binds
only to its resolved addresses on the node. The server logs a warning if its certificate does not cover the
default management origin.

See [getting started](getting-started.md#nuget-global-tool) for installation and startup and
[containerization](containerization.md) for Docker. The standalone host loads `appsettings.json` and
`appsettings.{Environment}.json` at startup. Set `DOTNET_ENVIRONMENT` to select the environment. The default is
`Production`. Restart the host to apply changes to those files.

The public and management listeners use the server certificate configured by `SQUIRIX_TLS_CERTIFICATE_PATH`
and optionally `SQUIRIX_TLS_CERTIFICATE_PASSWORD`. A local .NET development certificate can be used on loopback.
Multi-node clusters also use a dedicated mutual TLS listener for peer traffic. See
[configuration](configuration.md#environment-variables) and [internode mTLS](security/internode-mtls.md).

## Embedded server

Create a console app and install both packages:

```bash
dotnet new console --framework net10.0 -o SquirixHost
dotnet add SquirixHost/SquirixHost.csproj package squirix.server --version 0.2.0-preview.1
dotnet add SquirixHost/SquirixHost.csproj package squirix --version 0.2.0-preview.1
```

Replace `SquirixHost/Program.cs` with this example. It starts one node, exercises the binary client and disposes
the client before stopping the server:

```csharp
using System;
using System.Threading;
using Squirix.Client;
using Squirix.Server;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var options = new SquirixServerOptions
{
    NodeId = "node-a",
    Uri = new Uri("https://localhost:5001"),
};
await using var server = await SquirixServer.StartAsync(options, timeout.Token);
await using var client = await SquirixClient.ConnectAsync(options.Uri, timeout.Token);
var cache = await client.GetCacheAsync<string>("embedded-demo", timeout.Token);
await cache.SetAsync("greeting", "hello", null, timeout.Token);
var result = await cache.GetValueAsync("greeting", timeout.Token);
Console.WriteLine(result.Found ? result.Value : "<missing>");
```

Run `dotnet run --project SquirixHost`. Use a certificate trusted by the client, and stop other processes using
the same ports first. Call `options.UsePersistence("data")` before startup to keep data across restarts.
Read `server.ManagementUri` after startup to get the bound origin for health and metrics requests.

To supply settings from a specific file, use `Configurator.LoadAsync(path, cancellationToken)` to create the options
and pass them to `SquirixServer.StartAsync(options, cancellationToken)`. The parameterless startup overload discovers
`appsettings.json` and chooses a free client port if the file or its `Squirix:Cluster` section is absent. A discovered file that fails validation
causes startup to fail. Configure a known origin when a client needs to connect to an embedded node.
