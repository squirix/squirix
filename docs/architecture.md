# Architecture

For a shorter product overview, see [client-server.md](client-server.md). This document covers package boundaries and
wire-contract details.

squirix is a client/server distributed cache engine. The primary production topology is:

```text
application -> Squirix client SDK -> Squirix.Server cluster
```

The repository uses a **client/server library split** plus a standalone host tool on NuGet:

| Package | Purpose |
| --- | --- |
| `squirix` | Client SDK |
| `squirix.server` | Server runtime library (embed in ASP.NET Core) |
| `squirix.server.tool` | Standalone `squirix-server` global tool |

The architectural boundary is two runtime roles — application client vs server node — not two packages total.

The standalone `Squirix.Server.Host` executable is published as the `squirix.server.tool` NuGet global tool (`squirix-server`)
and as a GitHub Release archive.

Package dependency rule:

```text
Squirix.Server does not reference the Squirix client assembly.
```

Wire compatibility is through gRPC/REST contracts and shared proto source, not a project reference from server to
client.

The v0.1 gRPC wire contract is the shared source file at
`src/shared/Squirix/Transport/Grpc/Protos/SquirixCache.proto`, not a separate NuGet package. `Squirix` generates
internal client transport types from that file with `GrpcServices="Client"`.
`Squirix.Server` generates internal server and cluster client transport types from the same file with
`GrpcServices="Server;Client"`. Share-sourced transport under `src/shared/Squirix/Transport/Grpc/Mappers/` is limited to
cluster routing signals such as stale-owner markers (`GrpcStaleOwnerMarkers.cs`). Extended-operation wire mappers are
outside this repository. Generated transport CLR types use internal transport namespaces, remain assembly-local
implementation details, and must not become exported product API.

`Squirix.Server` owns data placement, partition ownership, static cluster topology, owner routing, server-side
KV/expiration mutation execution, journal/snapshot/recovery, durability lifecycle, backpressure, memory pressure,
health/security/metrics endpoints, and REST/gRPC hosting. The separate `Squirix.Server.Host` executable owns the
standalone process lifecycle.

`Squirix` owns cache value/result types (`ICache<T>`, entry/result types, `ISquirixSerializer`). The exported v0.1 client
entry surface lives in `Squirix.Client` (`SquirixClient`, `ISquirixClient`, `SquirixClientOptions`), typed facade,
serializer boundary, and server-backed client connection configuration with bootstrap failover. Applications connect to
`Squirix.Server` over gRPC/REST; the client package does not host cache state or run the durability stack in the
application process.

v0.1 `ICache<T>` is limited to basic async key/value and expiration operations (`AddAsync`, `TryAddAsync`, `SetAsync`,
`UpdateAsync`, `GetValueAsync`, `GetEntryAsync`, `GetExpirationAsync`, `GetOrAddAsync`, `RemoveAsync`, `TouchAsync`,
`RemoveExpirationAsync`). Prefer `GetValueAsync` for reads with explicit presence; there is no `GetValueOrDefaultAsync`
on `ICache<T>`. `ContainsAsync` is not part of the v0.1 public client surface or the gRPC wire surface because existence
can become stale immediately in a distributed cache; use `GetValueAsync` / `GetEntryAsync` or REST `HEAD` instead.
Writes accept a value plus optional `CacheEntryOptions`; `CacheEntry<T>` is a read model returned by lookup APIs, not a
mutation parameter. When write options omit expiration (`options` is null, or neither `Expiration` nor `ExpiresAt` is
set), the entry is stored without TTL and does not expire by time. Compare-and-set, counters, batch, scan, watch, and tag
invalidation are not part of the v0.1 exported client surface.

The v0.1 gRPC service surface in `SquirixCache.proto` exposes ten unary cache RPCs. Mutations with expiration use
`SetEntry` / `TryAddEntry` with `CacheEntryWire`; the client SDK maps them to public `SetAsync` / `TryAddAsync`.
`GetValue` and `GetEntry` are separate read paths; `GetExpiration` and `GetOrAdd` are additional wire RPCs aligned with
`ICache<T>` without requiring the client to compose multiple calls. See [api.md](api.md#wire-contract). Regressions are
caught by `SquirixGrpcEndpointSurface.golden.txt` in server unit tests.

The exported client entry point is asynchronous and remote:

- `SquirixClient.ConnectAsync(string endpoint, ...)` connects to one `Squirix.Server` endpoint.
- `SquirixClient.ConnectAsync(options => options.Endpoints.Add(...), ...)` connects with one or more bootstrap endpoints
  (HA standby URLs, not shards). See [bootstrap client failover](bootstrap-client-failover.md).

Cluster topology, partition ownership, and server-side durability belong to `Squirix.Server`. Tests that validate exported
client behavior should start a server host and connect through `SquirixClient.ConnectAsync(...)`.

Within `Squirix.Server`, **Cluster** and **Node** are separate namespace roots:

| Namespace | Responsibility |
| --- | --- |
| `Squirix.Server.Cluster` | Multi-node rules and peer communication: hash-ring ownership (`INodeLocator`), topology (`TopologyOptions`, peers), gRPC transport (`ServerClientPool`), remote routing (`ClusteredCache`), and call reliability (`ServerCallPolicy`). |
| `Squirix.Server.Node` | Single-process orchestration: ASP.NET Core hosting, cache pipeline decorators, background services, observability, backpressure, and memory pressure. `Node` does not own a `Cluster/` subtree. |

`Squirix.Server.Cluster` is the home for all cluster-domain types; `Squirix.Server.Node` wires them into the host via DI
(`RuntimeServiceRegistration`, cache pipeline registration) but does not define cluster semantics.

`Squirix` must not reference `Squirix.Server`. Product code must not use `InternalsVisibleTo("Squirix.Server")` or
access-check bypasses to join the packages.

The current package boundary keeps server runtime, hosting, durability, endpoint adapters, cluster ownership,
validation, memory pressure, and observability in `Squirix.Server`. `Squirix` remains the lightweight client package and
must not regain server hosting or durable runtime responsibilities.

Server dependencies such as ASP.NET Core hosting, Kestrel, gRPC server adapters, JWT authentication, journal services,
snapshots, recovery, and server-owned metrics belong in `Squirix.Server`. They must not be added to `Squirix`.

Production clients use `ConnectAsync(...)` against external server endpoints, the shared `SquirixCache.proto` contract,
and server-only dependencies kept out of `Squirix.csproj`.
