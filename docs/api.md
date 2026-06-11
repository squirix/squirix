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

Prefer `GetValueAsync` for reads with explicit presence. `ContainsAsync` is not part of the v0.1 surface.

Writes accept `(key, value, options?, cancellationToken)`. Expiration uses `CacheEntryOptions`, not `CacheEntry<T>`
write overloads.

Out of scope for v0.1: batch, scan, watch, counters, tag invalidation, compare-and-set.

Configuration (`SquirixOptions`): endpoints, bearer token provider, custom serializer.
See [configuration.md](configuration.md) and [serialization.md](serialization.md).

## Wire contract

gRPC contract: `src/shared/transport/grpc/Protos/SquirixCache.proto` (shared source, not a separate NuGet package).

Transport requires HTTPS endpoints. Cleartext `http://` URLs are rejected at configuration time.

Authentication uses JWT bearer tokens when enabled via `SquirixOptions.BearerTokenProvider`.

## Cache names

Validation rules for logical cache names: [cache-name-validation.md](cache-name-validation.md).
