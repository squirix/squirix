# Architecture

squirix is a client/server distributed cache engine. The primary production topology is:

```text
application -> Squirix client SDK -> Squirix.Server cluster
```

The repository uses a two-package layout:

| Package          | Purpose                                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `Squirix`        | Client SDK, exported cache API, typed client facade, serializer boundary, server-backed connection/routing/retry behavior |
| `Squirix.Server` | Distributed cache server engine, data placement, partition ownership, runtime, durability, hosting, REST/gRPC host        |

The standalone `Squirix.Server.Host` executable is a deployment project that references the `Squirix.Server` runtime
library. It is published as a release archive and as the `Squirix.Server.Tool` .NET global tool package.

Package dependency rule:

```text
Squirix.Server does not reference the Squirix client assembly.
```

Wire compatibility is through gRPC/REST contracts and shared proto source, not a project reference from server to
client.

The v0.1 gRPC wire contract is the shared source file at `src/shared/transport/grpc/Protos/SquirixCache.proto`, not a
separate NuGet package. `Squirix` generates internal client transport types from that file with `GrpcServices="Client"`.
`Squirix.Server` generates internal server and cluster client transport types from the same file with
`GrpcServices="Server;Client"`. Share-sourced transport under `src/shared/transport/grpc/Mappers/` is limited to
cluster routing signals such as stale-owner markers (`GrpcStaleOwnerMarkers.cs`). Extended-operation wire mappers are
outside this repository. Generated transport CLR types use internal transport namespaces, remain assembly-local
implementation details, and must not become exported product API.

`Squirix.Server` owns data placement, partition ownership, static cluster topology, owner routing, server-side
KV/expiration mutation execution, journal/snapshot/recovery, durability lifecycle, backpressure, memory pressure,
health/admin/security/metrics endpoints, and REST/gRPC hosting. The separate `Squirix.Server.Host` executable owns the
standalone process lifecycle.

`Squirix` owns the exported v0.1 client surface (`SquirixClient`, `ISquirixClient`, `SquirixOptions`, `ICache<T>`,
entry/result types, `ISquirixSerializer`), typed facade, serializer boundary, and server-backed client connection
configuration with bootstrap failover. It does not expose exported local, embedded, or in-process client modes.

v0.1 `ICache<T>` is limited to basic async key/value and expiration operations (`AddAsync`, `TryAddAsync`, `SetAsync`,
`UpdateAsync`, `GetValueAsync`, `GetEntryAsync`, `GetExpirationAsync`, `GetOrAddAsync`, `RemoveAsync`, `TouchAsync`,
`RemoveExpirationAsync`). Prefer `GetValueAsync` for reads with explicit presence; there is no `GetValueOrDefaultAsync`
on `ICache<T>`. `ContainsAsync` is not part of the v0.1 public client surface because existence can become stale
immediately in a distributed cache. Writes accept a value plus optional `CacheEntryOptions`; `CacheEntry<T>` is a read
model returned by lookup APIs, not a mutation parameter. Compare-and-set, counters, batch, scan, watch, and tag
invalidation are not part of the v0.1 exported client surface.

The exported client factory is asynchronous and server-backed:

- `SquirixClient.ConnectAsync(string endpoint, ...)` connects to one external `Squirix.Server` endpoint.
- `SquirixClient.ConnectAsync(options => options.Endpoints.Add(...), ...)` connects with one or more bootstrap endpoints
  (HA standby URLs, not shards). See [bootstrap client failover](bootstrap-client-failover.md).

`UseLocal()`, `UseEmbedded()`, and `UseInMemory()` are not exported client mode names. `UseCluster(...)` is also not an
exported client mode name because cluster topology belongs to `Squirix.Server`.

There is no embedded/local exported client path and no embedded test-client helper boundary. Tests that exercise
exported client behavior should start a server host and connect through `SquirixClient.ConnectAsync(...)`.

`Squirix` must not reference `Squirix.Server`. Product code must not use `InternalsVisibleTo("Squirix.Server")` or
access-check bypasses to join the packages.

The current package boundary keeps server runtime, hosting, durability, endpoint adapters, cluster ownership,
validation, memory pressure, and observability in `Squirix.Server`. `Squirix` remains the lightweight client package and
must not regain server hosting or durable runtime responsibilities.

Server dependencies such as ASP.NET Core hosting, Kestrel, gRPC server adapters, JWT authentication, journal services,
snapshots, recovery, and server-owned metrics belong in `Squirix.Server`. They must not be added to `Squirix`.

Production clients use `ConnectAsync(...)` against external server endpoints, the shared `SquirixCache.proto` contract,
and server-only dependencies kept out of `Squirix.csproj`.
