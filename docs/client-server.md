# Client and server model

squirix separates application clients from server runtime. Applications never embed durability or routing logic.

```text
application -> Squirix client SDK -> squirix server node(s)
```

## Who is it for?

- **.NET backend engineers** exploring a typed, client/server cache SDK for .NET 10
- **Distributed systems engineers** evaluating ownership routing, journal-backed durability, and operational surfaces
- **OSS contributors** interested in cache engines, gRPC services, and durability tooling
- Teams comparing early-stage alternatives — without claims of parity with mature products such as Redis, NCache, or
  Hazelcast

## Why squirix?

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
| [`squirix.server`](https://www.nuget.org/packages/squirix.server/) | Server runtime — routing, durability, gRPC host (library) | `0.1.0-preview.4` |
| [`squirix.server.tool`](https://www.nuget.org/packages/squirix.server.tool/) | Standalone `squirix-server` executable as a .NET global tool | `0.1.0-preview.4` |

Install:

```powershell
dotnet add package squirix --version 0.1.0-preview.4
dotnet add package squirix.server --version 0.1.0-preview.4
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.4
```

During early preview evaluation you may reference `Squirix.Server.csproj` from a clone instead of the NuGet package.

## Responsibilities

| Layer | Owns |
| --- | --- |
| **`Squirix` (client)** | `SquirixClient`, `ICache<T>`, serializers, bootstrap connectivity and transport failover |
| **`Squirix.Server` (runtime)** | Key placement, journal/snapshot/recovery, gRPC hosting, health/metrics |
| **`squirix-server` (host)** | Standalone process lifecycle and CLI |

`Squirix.Server` does not reference the `Squirix` client assembly. Wire compatibility is through gRPC
contracts and shared proto source.

## Features in 0.1.0

- Strict client/server architecture with wire-contract boundaries
- Strongly typed `ICache<T>` with explicit read results and expiration on writes
- HTTPS gRPC transport (shared `SquirixCache.proto` contract)
- Health, readiness, and metrics routes on the same primary TLS listener (HTTP/1.1 and HTTP/2)
- Opt-in WAL-based durability with recovery on startup (`UsePersistence()` / `--persist`)
- Snapshots and journal compaction
- Prometheus metrics and OpenTelemetry tracing
- Static consistent-hash single-owner routing with bootstrap client failover
- Standalone `squirix-server` CLI and optional ASP.NET Core embedding

## Further reading

- Package boundaries and wire contract: [architecture.md](architecture.md)
- Run and embed a server: [getting-started.md](getting-started.md), [server-mode.md](server-mode.md)
- Client API surface: [api.md](api.md)
