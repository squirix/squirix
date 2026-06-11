# API reference (v0.1 preview)

squirix exposes a typed gRPC client SDK. Cache operations use gRPC only; HTTP endpoints are limited to health and metrics.

## Client SDK

Entry point:

```csharp
await using var client = await SquirixClient.ConnectAsync("https://localhost:5001", cancellationToken);
var cache = await client.GetCacheAsync<T>("cache-name", cancellationToken);
```

`ICache<T>` methods (v0.1 exported surface):

| Method | Purpose |
| --- | --- |
| `AddAsync` / `TryAddAsync` | Insert if absent |
| `SetAsync` | Upsert with optional expiration |
| `UpdateAsync` | Update existing entry |
| `GetValueAsync` / `GetEntryAsync` | Read with explicit presence |
| `GetExpirationAsync` | Read expiration metadata |
| `GetOrAddAsync` | Read or insert factory value |
| `RemoveAsync` | Delete key |
| `TouchAsync` / `RemoveExpirationAsync` | Expiration management |

Prefer `GetValueAsync` for reads with explicit presence.

Writes accept `(key, value, options?, cancellationToken)`. Expiration uses `CacheEntryOptions`, not `CacheEntry<T>`
write overloads.

`UpdateAsync`, `GetOrAddAsync`, and `GetExpirationAsync` are implemented on the client by composing the RPCs below (no
dedicated gRPC methods).

Out of scope for v0.1: batch, scan, watch, counters, tag invalidation, compare-and-set.

Configuration (`SquirixOptions`): endpoints, JWT bearer token provider, custom serializer.
See [configuration.md](configuration.md) and [serialization.md](serialization.md).

## Wire contract

gRPC contract: `src/shared/transport/grpc/Protos/SquirixCache.proto` (shared source, not a separate NuGet package).

`SquirixCacheService` exposes nine unary RPCs on the v0.1 server surface (cluster/control-plane services are not mapped
here):

| gRPC RPC | `ICache<T>` mapping | Notes |
| --- | --- | --- |
| `TrySetValue` | `TryAddAsync` | Typed `CacheValue` payload |
| `SetValue` | `SetAsync` | Typed `CacheValue` payload |
| `GetValue` | `GetValueAsync` | Returns `found` + value |
| `Get` | `GetEntryAsync` | Returns full `Entry`; missing key → gRPC `NotFound` |
| `Remove` | `RemoveAsync` | |
| `Touch` | `TouchAsync` | Relative expiration (`Duration`) |
| `RemoveExpiration` | `RemoveExpirationAsync` | |
| `TrySet` | — | `Entry` / `Struct` payload; cluster and legacy struct paths |
| `Set` | — | `Entry` / `Struct` payload; cluster and legacy struct paths |

There is no `Contains` RPC. Prefer `GetValue` or REST `HEAD` for presence checks.

The approved RPC list is locked by a golden snapshot test:
`tests/squirix.server/squirix.server.unit-tests/ApiSnapshots/SquirixGrpcEndpointSurface.golden.txt`.

Transport requires HTTPS endpoints. Cleartext `http://` URLs are rejected at configuration time.

Authentication uses JWT bearer tokens when enabled via `SquirixOptions.BearerTokenProvider`.

## Cache names

Validation rules for logical cache names: [cache-name-validation.md](cache-name-validation.md).
