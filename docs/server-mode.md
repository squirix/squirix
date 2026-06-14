# Server mode

Squirix servers run as a standalone `squirix-server` process or embedded in a custom ASP.NET Core host. Application
data access always goes through the `Squirix` client SDK, even when client and server share a process.

For install, Docker, and first connection steps, see [getting started](getting-started.md). For package roles, see
[client and server model](client-server.md).

## Standalone host

The `squirix-server` global tool wraps the same runtime as the library host. Default gRPC listen URL:
`https://localhost:5001`.

```powershell
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.4
squirix-server run
```

Durable mode:

```powershell
squirix-server run --persist --data-dir ./data
```

CLI reference and Docker examples: [getting-started.md](getting-started.md), [containerization.md](containerization.md).

## Custom ASP.NET Core hosting

Embed the server runtime from the **`squirix.server`** NuGet package:

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
    options.UsePersistence("./data");
});

var app = builder.Build();
app.MapSquirixServer();
await app.RunAsync();
```

`AddSquirixServer(...)` registers the server runtime and configures the primary Kestrel HTTPS listener (HTTP/1.1 and
HTTP/2 on one port). `MapSquirixServer()` maps gRPC, health, and metrics endpoints.

## Loopback development default (not production posture)

The default standalone URL `https://localhost:5001` binds the primary listener to a **loopback interface**
(`localhost` / `127.0.0.1`). In that configuration, Squirix **does not require JWT** at startup unless you configure
`SQUIRIX_JWT_*` explicitly. Any process on the same machine can call gRPC cache routes and scrape `/metrics` without
credentials. This is intentional for local development and tests — it is **not** a hardened production posture.

Production and shared-network deployments must:

- bind the primary listener on a **non-loopback** address (`0.0.0.0`, a service hostname, or a public interface) and
  configure JWT or OIDC — startup **refuses** non-loopback binds without auth; or
- keep a loopback bind but treat the host as a trusted single-user machine (rare outside local dev).

There is no opt-out that allows unauthenticated cache access on non-loopback interfaces. See
[configuration.md — Security notes](configuration.md#security-notes) and
[diagnostics.md — metrics route](diagnostics.md#metrics-route).

Multi-node clusters with remote peers also open a **second HTTPS listener** on
`SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT` for inter-node gRPC with mutual TLS. External clients continue to use the primary
port with JWT/OIDC; cluster forwarding uses the internal port and per-node certificates (`CN` = `NodeId`). See
[security/inter-node-mtls.md](security/inter-node-mtls.md).

Explicit settings path:

```csharp
builder.AddSquirixServer(
    options => options.NodeId = "node-a",
    settingsPath: "Squirix.settings.json",
    loadDiscoveredSettings: false);
```

## Tests and samples

`SquirixServer.StartAsync` uses discovered settings or an ephemeral free HTTPS port:

```csharp
var listenUrl = "https://localhost:5001"; // or Cluster.Url from Squirix.settings.json
await using var server = await SquirixServer.StartAsync(cancellationToken);
await using var client = await SquirixClient.ConnectAsync(listenUrl, cancellationToken);
```

For options controlled entirely in code, prefer `AddSquirixServer` on `WebApplicationBuilder` over `SquirixServer.StartAsync`.
