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
| `squirix.server` | Server runtime library for .NET applications |
| `squirix.server.tool` | Standalone `squirix-server` global tool |

The architectural boundary is two runtime roles — application client vs server node — not two packages total.

The standalone `Squirix.Server.Host` executable is published as the `squirix.server.tool` NuGet global tool (`squirix-server`)
and as a GitHub Release archive.

Package dependency rule:

```text
Squirix.Server does not reference the Squirix client assembly.
```

The SDK and server compile the same internal cache message definitions and binary codec from shared C# sources.
The replication message definitions are private to the server. HTTP health and metrics are served separately.
The [wire generator](../tools/internal/generate-wire.py) emits the checked-in C# message types from the positional schemas.
Each operation defines the order of its fields. Flags encode booleans and optional-field presence without per-field tags.

Peers exchange versioned binary messages on the internal mTLS listener.
Owner forwarding, log replication, elections and state transfer use this protocol.
The client preface is SQB3 and the peer ALPN is `squirix-peer/2`. Peer frames use a reserved length marker for large messages.
The SDK negotiates extended lengths for large values. The public ingress checks the declared value length before allocating the body buffer.
Peers send SHA-256 ring fingerprints and W3C trace identifiers as bytes. The ring fingerprint includes the value limit, so cluster members must agree on it.

The shared transport code defines routing signals such as stale-owner markers.
Wire message types are internal to their assemblies.

`Squirix.Server` owns data placement, partition ownership, static cluster topology, owner routing, server-side
KV/expiration mutation execution, journal/snapshot/recovery, durability lifecycle, backpressure, memory pressure,
the management HTTP listener and the binary client and peer listeners. The separate `Squirix.Server.Host` executable owns the
standalone process lifecycle.

`Squirix` owns cache value/result types (`ICache<T>`, entry/result types, `ISquirixSerializer`). The exported v0.1 client
entry surface lives in `Squirix.Client` (`SquirixClient`, `ISquirixClient`, `SquirixClientOptions`), typed facade,
serializer boundary, and server-backed client connection configuration with bootstrap failover. Applications connect to
`Squirix.Server` over binary TLS for cache calls and HTTPS for health and metrics; the client package does not host cache state or run the
durability stack in the application process.

v0.1 `ICache<T>` is limited to basic async key/value and expiration operations (`AddAsync`, `TryAddAsync`, `SetAsync`,
`UpdateAsync`, `GetValueAsync`, `GetEntryAsync`, `GetExpirationAsync`, `GetOrAddAsync`, `RemoveAsync`, `TouchAsync`,
`RemoveExpirationAsync`). Prefer `GetValueAsync` for reads with explicit presence; there is no `GetValueOrDefaultAsync`
on `ICache<T>`. `ContainsAsync` is not part of the v0.1 public client surface or the binary wire surface because existence
can become stale immediately in a distributed cache; use `GetValueAsync` / `GetEntryAsync` instead.
Writes accept a value plus optional `CacheEntryOptions`; `CacheEntry<T>` is a read model returned by lookup APIs, not a
mutation parameter. When write options omit expiration (`options` is null, or neither `Expiration` nor `ExpiresAt` is
set), the entry is stored without TTL and does not expire by time; overwriting an expiring key that way clears its TTL.
Compare-and-set, counters, batch, scan, watch, and tag invalidation are not part of the v0.1 exported client surface.

The binary protocol defines 13 operations, including three for `IDistributedCache`. Mutations with expiration use
`SetEntry` / `TryAddEntry` with `CacheEntryWire`; the client SDK maps them to public `SetAsync` / `TryAddAsync`.
`GetValue` and `GetEntry` are separate read paths; `GetExpiration` and `GetOrAdd` are additional wire RPCs aligned with
`ICache<T>` without requiring the client to compose multiple calls. See [api.md](api.md#wire-contract). Regressions are
covered by SDK and server transport tests.

The exported client entry point is asynchronous and remote:

- `SquirixClient.ConnectAsync(Uri endpoint, ...)` connects to one `Squirix.Server` endpoint.
- `SquirixClient.ConnectAsync(options => options.Endpoints.Add(new Uri(...)), ...)` connects with one or more bootstrap
  endpoints (HA standby URLs, not shards). See [bootstrap client failover](bootstrap-client-failover.md).

Cluster topology, partition ownership, and server-side durability belong to `Squirix.Server`. Tests that validate exported
client behavior should start a server host and connect through `SquirixClient.ConnectAsync(...)`.

Within `Squirix.Server`, **Cluster** and **Node** are separate namespace roots:

| Namespace | Responsibility |
| --- | --- |
| `Squirix.Server.Cluster` | Multi-node rules and peer communication: hash-ring ownership (`INodeLocator`), topology (`TopologyOptions`, peers), binary peer transport (`ServerClientPool`), and call reliability (`ServerCallPolicy`). The cache dispatcher resolves key ownership first (`OwnerRouter`) and forwards non-owner client calls through this transport (`OwnerRpcForwarder`). |
| `Squirix.Server.Node` | Single-process orchestration: node lifecycle, cache pipeline decorators, background services, observability, backpressure, and memory pressure. `Node` does not own a `Cluster/` subtree. |

`Squirix.Server.Cluster` is the home for all cluster-domain types; `Squirix.Server.Node` wires them into the host via DI
(`RuntimeServiceRegistration`, cache pipeline registration) but does not define cluster semantics.

`Squirix` must not reference `Squirix.Server`. Product code must not use `InternalsVisibleTo("Squirix.Server")` or
access-check bypasses to join the packages.

The current package boundary keeps server runtime, hosting, durability, endpoint adapters, cluster ownership,
validation, memory pressure, and observability in `Squirix.Server`. `Squirix` remains the lightweight client package and
must not regain server hosting or durable runtime responsibilities.

Server dependencies such as socket hosting, JWT authentication, journal services,
snapshots, recovery, and server-owned metrics belong in `Squirix.Server`. They must not be added to `Squirix`.

Production clients use `ConnectAsync(...)` against external server endpoints through the shared binary cache contract.

## Replica sets

Consensus, quorum reads, and the executable protocol model are specified in
[replication-consensus.md](architecture/replication-consensus.md).
