# squirix

[![CI](https://github.com/squirix/squirix/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/squirix/squirix/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![NuGet](https://img.shields.io/badge/NuGet-0.1.0--preview.10-004880?logo=nuget&logoColor=white)](https://www.nuget.org/profiles/squirix)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Slack](https://img.shields.io/badge/Slack-join-4A154B?logo=slack&logoColor=white)](https://squirix.slack.com)

Experimental distributed cache for **.NET 10**. Apps use the `Squirix` client SDK over HTTPS gRPC; server nodes own
cache state, routing, durability, and ops endpoints.
Replica groups favor consistency over availability (CP in CAP, PC/EC in PACELC); see [consistency](docs/consistency.md#cap-and-pacelc).

[Website](https://squirix.io/) · [Getting started](https://squirix.io/docs/getting-started/) · [Blog](https://squirix.io/blog/)

**0.1.0-preview.10** — early preview, not production-ready. [Release notes](docs/release-notes/v0.1.0.md)

[![Powered by NDepend](docs/assets/powered-by-ndepend.png)](https://www.ndepend.com/)

## NuGet packages

All packages are published under the [squirix NuGet profile](https://www.nuget.org/profiles/squirix):

| Package                                                                      | Role                                                     |
|------------------------------------------------------------------------------|----------------------------------------------------------|
| [`squirix`](https://www.nuget.org/packages/squirix/)                         | Client SDK — `SquirixClient`, `ICache<T>`, serialization |
| [`squirix.server`](https://www.nuget.org/packages/squirix.server/)           | Server runtime — routing, durability, gRPC host          |
| [`squirix.server.tool`](https://www.nuget.org/packages/squirix.server.tool/) | Standalone `squirix-server` global tool                  |

## Quick start

Use the .NET 10 SDK. In a terminal, trust the development HTTPS certificate and start the cache server:

```bash
dotnet dev-certs https --trust
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.10
squirix-server run --urls https://localhost:5501
```

Leave the server running at `https://localhost:5501`. In a second terminal, create a console app and install the client SDK:

```bash
dotnet new console --framework net10.0 -o SquirixDemo
dotnet add SquirixDemo/SquirixDemo.csproj package squirix --version 0.1.0-preview.10
```

Replace `SquirixDemo/Program.cs` with:

```csharp
using System;
using System.Threading;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

await using var client = await SquirixClient.ConnectAsync(
    new Uri("https://localhost:5501"), cancellationToken);
var cache = await client.GetCacheAsync<string>("demo", cancellationToken);

await cache.SetAsync("greeting", "hello", null, cancellationToken);
Console.WriteLine("Wrote: hello");

var result = await cache.GetValueAsync("greeting", cancellationToken);
if (!result.Found)
{
    Console.Error.WriteLine("Cache miss.");
    return 1;
}

Console.WriteLine($"Read: {result.Value}");
return 0;
```

Run the app:

```bash
dotnet run --project SquirixDemo
```

Expected output:

```text
Wrote: hello
Read: hello
```

The server stores entries in memory by default. To enable journal and snapshot persistence, stop it and restart with:

```bash
squirix-server run --urls https://localhost:5501 --persist --data-dir data
```

For Docker, JWT authentication, and ASP.NET Core embedding, see [Getting started](docs/getting-started.md).

## Microsoft distributed caching

The preview.11 adapter provides `IDistributedCache` and integrates with Microsoft HybridCache as its secondary cache. See the [provider guide](docs/distributed-cache.md) and the [ASP.NET Core sample](samples/aspnet-hybrid-cache/README.md).

## Examples

- [Share a cache between two console processes](https://squirix.io/#example).
- [Share a cache between ASP.NET Core instances](https://squirix.io/docs/develop/aspnet-core-shared-cache/)
  ([runnable project](https://github.com/squirix/squirix/tree/develop/samples/aspnet-shared-cache)).

The ASP.NET Core guide and runnable project are in `develop` and use the published `0.1.0-preview.10` SDK.

## Documentation

- [Getting started](docs/getting-started.md) · [Client & server](docs/client-server.md) · [Configuration](docs/configuration.md)
- [Persistence](docs/persistence.md) · [Clustering](docs/clustering.md) · [Observability](docs/observability.md)
- [API](docs/api.md) · [Architecture](docs/architecture.md) · [Operations](docs/operations.md)
- [Operational runbook](docs/operational-runbook.md) · [Security (internode mTLS)](docs/security/internode-mtls.md)
- [Related projects](docs/related-projects.md)

## Contributing

[Issues](https://github.com/squirix/squirix/issues) · [contributing.md](contributing.md) · [admin@squirix.io](mailto:admin@squirix.io)

## SAST Tools

[PVS-Studio](https://pvs-studio.com/pvs-studio/?utm_source=website&utm_medium=github&utm_campaign=open_source) -
static code analyzer for Enterprise (C, C++, C#, Go, and Java) and Web (JS and TS) development.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
