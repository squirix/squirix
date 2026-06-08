# squirix

[![CI](https://github.com/squirix/squirix/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/squirix/squirix/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![NuGet](https://img.shields.io/badge/NuGet-0.1.0--preview.4-004880?logo=nuget&logoColor=white)](https://www.nuget.org/packages/squirix/)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/download/dotnet/10.0)

**squirix is an experimental distributed cache for modern .NET.**

Applications connect with the `Squirix` client SDK over HTTPS gRPC. Server nodes own cache state, key routing,
durability, and operational endpoints.

## Status

| | |
| --- | --- |
| **Release line** | **0.1.0** (first public preview) |
| **NuGet version** | `0.1.0-preview.4` |
| **Maturity** | Early preview — **not production-ready** |
| **Target framework** | .NET 10 only |
| **License** | Apache-2.0 |

We are looking for API, architecture, and operational feedback — not production adoption yet.
[Release notes](docs/release-notes/v0.1.0.md)

## Key capabilities

- Typed client/server packages with a narrow `ICache<T>` API
- HTTPS gRPC transport and HTTP/2 REST cache endpoints
- Per-node journal durability with snapshots and compaction
- Health, admin, Prometheus metrics, and OpenTelemetry tracing
- Static consistent-hash routing with bootstrap client failover

## Install

```powershell
dotnet add package squirix --version 0.1.0-preview.4
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.4
```

## Quick start

Start a local server (default gRPC endpoint `https://localhost:5001`):

```powershell
squirix-server run --data-dir ./data
```

Connect from your app:

```csharp
using Squirix;

await using var client = await SquirixClient.ConnectAsync("https://localhost:5001", cancellationToken);
var cache = await client.GetCacheAsync<string>("demo", cancellationToken);
await cache.SetAsync("greeting", "hello", cancellationToken: cancellationToken);
```

Full setup options (Docker, ASP.NET Core embedding, TLS): [getting started](docs/getting-started.md).

## Documentation

| Topic | Guide |
| --- | --- |
| First run | [getting-started.md](docs/getting-started.md) |
| Client and server model | [client-server.md](docs/client-server.md) |
| Configuration | [configuration.md](docs/configuration.md) |
| Persistence and recovery | [persistence.md](docs/persistence.md) |
| Cluster routing | [clustering.md](docs/clustering.md) |
| Health, metrics, tracing | [observability.md](docs/observability.md) |
| Client and REST API | [api.md](docs/api.md) |
| Build, limits, roadmap | [operations.md](docs/operations.md) |

Additional references: [architecture](docs/architecture.md), [operational runbook](docs/operational-runbook.md),
[containerization](docs/containerization.md), [serialization](docs/serialization.md).

## Contributing

- [Open an issue](https://github.com/squirix/squirix/issues) for bugs or API ideas
- [contributing.md](contributing.md) for pull request guidelines
- Email: [admin@squirix.io](mailto:admin@squirix.io)

## License

Apache-2.0 — see [LICENSE](LICENSE).
