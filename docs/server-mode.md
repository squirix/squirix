# Server mode

Run a Squirix node with the `squirix-server` tool or embed it using the `squirix.server` library.
In both modes, applications access data through the Squirix client SDK over the binary TLS listener.

For the .NET 10 build on Linux and macOS, use .NET 10.0.15 or a later .NET 10 patch.
Earlier .NET 10 builds can silently lose disk flush errors and are rejected at startup.

## Standalone host

The global tool listens on `https://localhost:5001` for binary clients by default. The HTTP/1.1 management listener
defaults to `https://127.0.0.1:5003`. Set `SQUIRIX_MANAGEMENT_URI` to select another HTTPS origin. By default its port
is the client port plus two, or minus two if adding two would exceed the TCP port range.

See [getting started](getting-started.md#nuget-global-tool) for installation and startup and
[containerization](containerization.md) for Docker. The standalone host loads `appsettings.json` and
`appsettings.{Environment}.json` at startup. Restart the host to apply changes to those files.

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
`Squirix.settings.json` and chooses a free client port if no file is found. A discovered file that fails validation
causes startup to fail. Configure a known origin when a client needs to connect to an embedded node.
