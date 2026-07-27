# squirix

[![CI](https://github.com/squirix/squirix/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/squirix/squirix/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![NuGet](https://img.shields.io/badge/NuGet-0.1.0--preview.7-004880?logo=nuget&logoColor=white)](https://www.nuget.org/profiles/squirix)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Slack](https://img.shields.io/badge/Slack-join-4A154B?logo=slack&logoColor=white)](https://squirix.slack.com)

Experimental distributed cache for **.NET 10**. Apps use the `Squirix` client SDK over HTTPS gRPC; server nodes own
cache state, routing, durability, and ops endpoints.

**0.1.0-preview.7** — early preview, not production-ready. [Release notes](docs/release-notes/v0.1.0.md)

## NuGet packages

All packages are published under the [squirix NuGet profile](https://www.nuget.org/profiles/squirix):

| Package                                                                      | Role                                                     |
|------------------------------------------------------------------------------|----------------------------------------------------------|
| [`squirix`](https://www.nuget.org/packages/squirix/)                         | Client SDK — `SquirixClient`, `ICache<T>`, serialization |
| [`squirix.server`](https://www.nuget.org/packages/squirix.server/)           | Server runtime — routing, durability, gRPC host          |
| [`squirix.server.tool`](https://www.nuget.org/packages/squirix.server.tool/) | Standalone `squirix-server` global tool                  |

## Quick start

```bash
dotnet add package squirix --version 0.1.0-preview.7
dotnet tool install -g squirix.server.tool --version 0.1.0-preview.7
squirix-server run
```

- **NuGet package** for the CLI: `squirix.server.tool`
- **Command after install**: `squirix-server` (`run`, `init`, `doctor`, `validate-config`, …)

Durable mode (journal + snapshots):

```bash
squirix-server run --persist --data-dir ./data
```

Client:

```csharp
using System;
using System.Threading;
using Squirix.Client;

await using var client = await SquirixClient.ConnectAsync(new Uri("https://localhost:5001"), CancellationToken.None);
var cache = await client.GetCacheAsync<string>("demo", CancellationToken.None);
await cache.SetAsync("greeting", "hello", cancellationToken: CancellationToken.None);
```

Docker, JWT, and ASP.NET Core embedding: [getting started](docs/getting-started.md).

## Documentation

- [Getting started](docs/getting-started.md) · [Client & server](docs/client-server.md) · [Configuration](docs/configuration.md)
- [Persistence](docs/persistence.md) · [Clustering](docs/clustering.md) · [Observability](docs/observability.md)
- [API](docs/api.md) · [Architecture](docs/architecture.md) · [Operations](docs/operations.md)
- [Operational runbook](docs/operational-runbook.md) · [Security (inter-node mTLS)](docs/security/inter-node-mtls.md)

## Contributing

[Issues](https://github.com/squirix/squirix/issues) · [contributing.md](contributing.md) · [admin@squirix.io](mailto:admin@squirix.io)

## License

Apache-2.0 — see [LICENSE](./LICENSE).
