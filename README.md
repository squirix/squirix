# squirix

[![CI](https://github.com/squirix/squirix/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/squirix/squirix/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![NuGet](https://img.shields.io/badge/NuGet-0.1.0--preview.4-004880?logo=nuget&logoColor=white)](https://www.nuget.org/packages/squirix/)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/download/dotnet/10.0)

Experimental distributed cache for **.NET 10**. Apps use the `Squirix` client SDK over HTTPS gRPC; server nodes own
cache state, routing, durability, and ops endpoints.

**0.1.0-preview.4** — early preview, not production-ready. [Release notes](docs/release-notes/v0.1.0.md)

## Quick start

```powershell
dotnet add package squirix --version 0.1.0-preview.4
dotnet tool install -g squirix.server.tool --version 0.1.0-preview.4
squirix-server run
```

- **NuGet package** for the CLI: `squirix.server.tool`
- **Command after install**: `squirix-server` (`run`, `init`, `doctor`, `validate-config`, …)

Durable mode (WAL + snapshots):

```powershell
squirix-server run --persist --data-dir ./data
```

Client:

```csharp
using Squirix;

await using var client = await SquirixClient.ConnectAsync("https://localhost:5001", cancellationToken);
var cache = await client.GetCacheAsync<string>("demo", cancellationToken);
await cache.SetAsync("greeting", "hello", cancellationToken: cancellationToken);
```

Docker, JWT, and ASP.NET Core embedding: [getting started](docs/getting-started.md).

## Documentation

- [Getting started](docs/getting-started.md) · [Client & server](docs/client-server.md) · [Configuration](docs/configuration.md)
- [Persistence](docs/persistence.md) · [Clustering](docs/clustering.md) · [Observability](docs/observability.md)
- [API](docs/api.md) · [Architecture](docs/architecture.md) · [Operations](docs/operations.md)

## Contributing

[Issues](https://github.com/squirix/squirix/issues) · [contributing.md](contributing.md) · [admin@squirix.io](mailto:admin@squirix.io)

## License

Apache-2.0 — see [LICENSE](./LICENSE).
