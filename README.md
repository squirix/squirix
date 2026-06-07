# squirix

[![CI](https://github.com/squirix/squirix/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/squirix/squirix/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![NuGet](https://img.shields.io/badge/NuGet-0.1.0--preview.4-004880?logo=nuget&logoColor=white)](https://www.nuget.org/packages/squirix/)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/download/dotnet/10.0)

**squirix is an experimental distributed cache for modern .NET.**

Applications use the `Squirix` client SDK (`squirix` on NuGet) to connect to a remote server node over gRPC. The server
owns cache state, routing, durability, and operational endpoints.

## Status

| | |
| --- | --- |
| **Release line** | **0.1.0** (first public preview) |
| **NuGet version** | `0.1.0-preview.4` |
| **Maturity** | Early preview — **not production-ready** |
| **Target framework** | .NET 10 only |
| **License** | Apache-2.0 |

This release validates the client/server architecture, typed API, and durability foundation. We are looking for API,
architecture, and operational feedback — not production adoption yet.

[Release notes for 0.1.0](docs/release-notes/v0.1.0.md)

## Who is it for?

- **.NET backend engineers** exploring a typed, client/server cache SDK for .NET 10
- **Distributed systems engineers** evaluating ownership routing, journal-backed durability, and operational surfaces
- **OSS contributors** interested in cache engines, gRPC services, and durability tooling
- Teams comparing early-stage alternatives in the problem space of distributed in-memory caches — without claims of
  parity with mature products such as Redis, NCache, or Hazelcast

## Quick start

### 1. Run a development server

**Docker** (fastest if you have Docker Desktop):

```powershell
docker build -f Dockerfile.dev -t squirix-server .
docker run --rm `
  -p 5000:5000 `
  -p 5001:5001 `
  -e SQUIRIX_HTTP1_PORT=5001 `
  -e SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL=true `
  -e SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL=true `
  squirix-server run --urls http://0.0.0.0:5000
```

Port **5000** is gRPC/HTTP/2 (map it for client apps). Port **5001** is the HTTP/1 sidecar for `curl`, health, and admin.

**NuGet tool** (after install):

```powershell
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.4
squirix-server run --dev --data-dir ./data
```

**Release Docker image** (pinned NuGet tool version; requires `squirix.server.tool` on nuget.org):

```powershell
docker build -f Dockerfile.release -t squirix-server:0.1.0-preview.4 .
docker run --rm `
  -p 5000:5000 `
  -p 5001:5001 `
  -e SQUIRIX_HTTP1_PORT=5001 `
  -e SQUIRIX_HTTP1_ALLOW_INSECURE_EXTERNAL=true `
  -e SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL=true `
  squirix-server:0.1.0-preview.4 run --urls http://0.0.0.0:5000
```

**From this repository** (requires a clone):

```powershell
dotnet run --project src/squirix.server.host/Squirix.Server.Host.csproj -- run --dev --data-dir ./data
```

The host prints a gRPC endpoint and a ready-to-use client snippet. Set `SQUIRIX_HTTP1_PORT` when you need a
browser-friendly HTTP/1 sidecar for health or admin checks.

Install the global tool from NuGet (`squirix.server.tool`; command `squirix-server`) or use the dev/release Dockerfiles
described in [containerization](docs/containerization.md).

More: [containerization](docs/containerization.md), [server mode](docs/server-mode.md).

### 2. Add the client SDK

```powershell
dotnet add package squirix --version 0.1.0-preview.4
```

For cleartext HTTP/2 (h2c) during local development:

```powershell
$env:DOTNET_SYSTEM_NET_HTTP_SOCKETSHTTPHANDLER_HTTP2UNENCRYPTEDSUPPORT = "1"
```

### 3. Connect and use a typed cache

Use the gRPC endpoint from the host output. With `squirix-server run --dev` (or `dotnet run ... --dev`), that is
`http://localhost:5001`. With the Docker example above, connect to `http://localhost:5000` (mapped gRPC port).

```csharp
using System.Threading;
using Squirix;

var cancellationToken = CancellationToken.None;

await using var client = await SquirixClient.ConnectAsync(
    "http://localhost:5001", // or http://localhost:5000 when using the Docker gRPC mapping
    cancellationToken);

var cache = await client.GetCacheAsync<string>("demo", cancellationToken);
await cache.SetAsync("greeting", "hello", cancellationToken: cancellationToken);

var lookup = await cache.GetValueAsync("greeting", cancellationToken);
Console.WriteLine(lookup.Found ? lookup.Value : "<missing>");
```

## Features in 0.1.0

- **Strict client/server architecture** — `Squirix` client SDK and server runtime with wire-contract boundaries
- **Strongly typed cache API** — `ICache<T>`, explicit read results, expiration on writes
- **gRPC transport** — shared `SquirixCache.proto` contract between client and server
- **HTTP/2 REST endpoints** — subset of cache operations plus health, readiness, and admin routes
- **WAL-based durability** — per-node journal segments with recovery on startup
- **Snapshots and compaction** — periodic snapshot triggers and journal tail compaction
- **Health and admin endpoints** — liveness/readiness probes and owner/ring inspection
- **Prometheus metrics** — text scrape at `/metrics` (configurable)
- **OpenTelemetry tracing** — server-side journal operation spans
- **Static cluster routing** — consistent-hash single-owner placement with bootstrap client failover
- **Standalone host** — `squirix-server` CLI and optional ASP.NET Core embedding

## Why squirix?

squirix is designed for teams that want a **modern .NET-native distributed cache engine** with a clear separation between
application clients and server runtime:

- **Typed application surface** instead of untyped string payloads at the SDK boundary
- **Explicit client/server packages** so applications do not accidentally embed server durability code
- **Operational visibility** through health, admin, metrics, and tracing hooks on the server
- **Durability-first server design** with journal, snapshots, and recovery as first-class server concerns
- **Focused 0.1 scope** — a narrow API that can evolve based on real feedback

squirix is **experimental** and **early-stage**. It is intended for evaluation, prototyping, and contributor
experiments — not as a drop-in replacement for production cache infrastructure today.

## Packages

NuGet ids use lowercase **`squirix.*`**. C# namespaces and exported types remain **`Squirix` / `Squirix.Server`**.

| NuGet package | Role | nuget.org |
| --- | --- | --- |
| [`squirix`](https://www.nuget.org/packages/squirix/) | Client SDK — `SquirixClient`, `ICache<T>`, serialization, connectivity | `0.1.0-preview.4` |
| [`squirix.server`](https://www.nuget.org/packages/squirix.server/) | Server runtime — routing, durability, REST/gRPC host (library) | `0.1.0-preview.4` |
| [`squirix.server.tool`](https://www.nuget.org/packages/squirix.server.tool/) | Standalone `squirix-server` executable as a .NET global tool | `0.1.0-preview.4` |

```text
application -> Squirix client SDK -> squirix server node(s)
```

Server library consumers:

```powershell
dotnet add package squirix.server --version 0.1.0-preview.4
```

Or reference `Squirix.Server.csproj` from a clone during early preview evaluation.

## Current limitations

- **Preview stability** — API, wire format, and on-disk layouts may change during 0.x
- **Performance** — characteristics are not final; do not benchmark against mature cache products yet
- **No replication or automatic failover** — durability is per node
- **Static topology** — peers are configured explicitly; dynamic membership is future work
- **Single-key operations** — cross-key or multi-node atomicity is out of scope for v0.1
- **Narrow client API** — basic KV + expiration; no batch, scan, watch, counters, or tag invalidation yet
- **0.x compatibility** — no promise of upgrade or on-disk persistence compatibility until 1.0.0
- **.NET 10 only** — older TFMs are intentionally out of scope for the preview line

See [consistency](docs/consistency.md) and [operational runbook](docs/operational-runbook.md) before any
production-like evaluation.

## Roadmap (directional)

These are **not commitments** — they reflect likely next areas based on current gaps:

- Harden durability, recovery, and compaction semantics across preview releases
- Expand operational tooling and observability defaults
- Evaluate additional client operations (batch, scan, invalidation) based on feedback
- Explore dynamic cluster membership and replication models after the 0.1 foundation stabilizes
- Performance tuning and benchmark baselines once API and durability contracts settle

## Contributing and feedback

Early feedback is especially valuable:

- [Open an issue](https://github.com/squirix/squirix/issues) for bugs, API ideas, or durability questions
- [contributing.md](contributing.md) for pull request guidelines
- Email: [admin@squirix.io](mailto:admin@squirix.io)
- Slack: [Squirix workspace](https://squirix.slack.com)

## Documentation

- [Architecture](docs/architecture.md)
- [Server mode](docs/server-mode.md)
- [Configuration](docs/configuration.md)
- [Bootstrap client failover](docs/bootstrap-client-failover.md)
- [Operational runbook](docs/operational-runbook.md)
- [Diagnostics](docs/diagnostics.md)
- [Serialization](docs/serialization.md)
- [Containerization](docs/containerization.md)
- [Naming conventions](docs/naming-conventions.md)

## Build and test

Prerequisite: .NET SDK as pinned in [`global.json`](global.json) (minimum **10.0.203**).

```powershell
dotnet restore squirix.slnx
dotnet build squirix.slnx --configuration Release
$env:DOTNET_SYSTEM_NET_HTTP_SOCKETSHTTPHANDLER_HTTP2UNENCRYPTEDSUPPORT = "1"
dotnet test squirix.slnx --configuration Release --no-build
```

## HTTP routes (summary)

Health: `/health`, `/health/live`, `/health/ready`, `/health/ready/details`

Admin: `/admin/whoami`, `/admin/owner/{key}`, `/admin/ring`

REST cache: `PUT/GET/HEAD/DELETE /api/v1/cache/{key}`

Metrics: `GET /metrics` (Prometheus text; enabled by default)

Details: [configuration](docs/configuration.md), [server mode](docs/server-mode.md).

## License

Apache-2.0 — see [LICENSE](LICENSE).
