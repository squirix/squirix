# Getting started

This guide walks through running a squirix server and connecting a .NET client.

Already using ASP.NET Core HybridCache? See [switching from Redis or Garnet to Squirix](distributed-cache.md#switch-from-redis-or-garnet-to-squirix)
for the Squirix adapter.

Prerequisites:

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) to run the examples below.
- Release packages built from the current source tree support both .NET 10 and [.NET 11](https://dotnet.microsoft.com/download/dotnet/11.0).
- Building repository sources uses the SDK selected by [`global.json`](../global.json).
- Docker Desktop with Linux containers for the Docker examples; the client example requires Compose 2.17 or newer.
- For the optional local TLS example, trust the development certificate with `dotnet dev-certs https --trust`.

## 1. Run a development server

Choose one of the server options below. Leave that server running while using the client; stop it before trying another
option on the same port. The local console example uses the global tool. Docker has a separate client example that
handles its development JWT and certificate setup.

### NuGet global tool

```bash
dotnet tool install --global squirix.server.tool --prerelease
squirix-server run
```

The server listens on `tcp://localhost:5001` and keeps entries in memory by default. Any local process can access the cache unless you configure JWT authentication.

To use journal/snapshot persistence instead, stop the in-memory server with Ctrl+C and restart it with:

```bash
squirix-server run --persist --data-dir data
```

Health probes use the loopback HTTP listener (local tool default port **5003**):

```bash
curl http://127.0.0.1:5003/health
```

To expose the Prometheus endpoint, start the server with `squirix-server run --metrics` and enable Prometheus export
in [the settings file](configuration.md#prometheus-metrics-squirixsettingsjson).

```bash
curl http://127.0.0.1:5003/metrics
```

`/metrics` accepts loopback connections without a token even when JWT authentication is enabled.

If port 5001 is in use, run `squirix-server run --urls tcp://localhost:5443` and use that URI in the client. The host prints the management URL at startup.

To encrypt the SDK and RESP connections, restart with `squirix-server run --tls`. Use `https://localhost:5001` in the SDK and the printed HTTPS URL for health probes.

### Docker

For a first client connection in Docker, follow the [complete Compose example](../samples/docker-client/README.md).
It starts three persistent RF=3 nodes and handles the development JWT and HTTPS certificate for the client container.
The sample credentials and certificates are for local development only.

To start a single node or choose between source and release images, see [Containerization](containerization.md#run).

### From this repository

Run this alternative from the repository root with its selected SDK. It starts the server built from the current
checkout, which may contain unreleased changes:

```bash
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run
```

## 2. Add the client SDK

Keep the server running. In a second terminal, create a console app. If you started the server from source,
change to the repository root before creating the app.

```bash
dotnet new console --framework net10.0 -o SquirixDemo
```

For the server tool, install the matching SDK package:

```bash
dotnet add SquirixDemo/SquirixDemo.csproj package squirix --prerelease
```

If you started the server from this checkout, run the following command from the repository root to reference the SDK from the same checkout:

```bash
dotnet add SquirixDemo/SquirixDemo.csproj reference src/squirix/Squirix.csproj
```

## 3. Connect and use a typed cache

The example uses the default TCP endpoint. If you changed the port or enabled TLS, use the client URI printed by the server.

Replace `SquirixDemo/Program.cs` with:

```csharp
using System;
using System.Threading;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

await using var client = await SquirixClient.ConnectAsync(new Uri("tcp://localhost:5001"), cancellationToken);

var cache = await client.GetCacheAsync<string>("demo", cancellationToken);
await cache.SetAsync("greeting", "hello", null, cancellationToken);

var lookup = await cache.GetValueAsync("greeting", cancellationToken);
Console.WriteLine(lookup.Found ? lookup.Value : "<missing>");
```

Run the app:

```bash
dotnet run --project SquirixDemo
```

Expected output: `hello`.

This example uses a string. For application-specific value types, register a source-generated JSON context as shown in
[Serialization](serialization.md#application-value-types).

**Docker Compose:** use the [complete client example](../samples/docker-client/README.md).
It generates a short-lived development JWT and trusts the cluster's HTTPS certificate only inside its own container.
Follow that example's startup, readiness checks, and client command in order. A container being listed as running
does not mean its replica groups are ready.

Expected output: `Found=True; Value=hello from Docker`.
For a reproducible leader-crash scenario, see the [failover demo](../samples/docker-client/FAILOVER.md).
The client connects to Compose DNS names on port 5000. Host applications instead use the published ports 5001–5003,
`options.BearerTokenProvider`, and a properly trusted server certificate. The public client has no certificate-validation
override option; do not disable TLS verification. See [HTTPS in containers](containerization.md#https-in-containers).

Multiple bootstrap endpoints (HA front door, not shards):

```csharp
using System;
using System.Threading;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

await using var client = await SquirixClient.ConnectAsync(
    static options =>
    {
        options.Endpoints.Add(new Uri("https://cache-a.example.internal:5001"));
        options.Endpoints.Add(new Uri("https://cache-b.example.internal:5002"));
    },
    cancellationToken);
```

See [bootstrap client failover](bootstrap-client-failover.md) and [configuration](configuration.md).

## CLI reference

```bash
squirix-server init [--settings appsettings.json]
squirix-server validate-config --settings appsettings.json [--strict]
squirix-server doctor [--settings appsettings.json] [--cache-eviction memory] [--memory-accounting]
squirix-server version
```

Square brackets in the command synopsis mark optional arguments; omit the brackets when running a command.
`init` creates a settings file once and refuses to overwrite an existing file. `--strict` performs optional-section
validation on `validate-config`; it does not add that validation to `doctor` or `run`.

`run` supports `--urls`, `--persist`, `--data-dir` (with `--persist`), `--settings`, `--enable-replication`,
`--metrics`, and `--memory-accounting`. Cache memory accounting is off by default. Enable it to track estimated cache bytes and enforce memory pressure limits.
Run `validate-config --settings appsettings.json --strict` before starting the server; passing `--strict` to
`run` does not perform that validation. Without `--settings`, the host looks for `appsettings.json` in the working directory, then in the application directory.

Use paths such as `appsettings.json` or `config/node.json`; operator-supplied paths containing `.` or `..`
segments are rejected. Relative paths are resolved against the process working directory.

For settings outside the standard discovery locations, `--settings` selects the cluster and Prometheus export settings.
Other optional runtime sections still use standard [file discovery](configuration.md#file-discovery).

## Next steps

- Embed the server in a .NET application: [server mode](server-mode.md)
- Tune cluster / memory-pressure / metrics settings: [configuration](configuration.md)
- Understand routing and consistency: [clustering](clustering.md)
