# API reference (v0.1 preview)

squirix exposes a typed gRPC client SDK. Cache operations use gRPC only; HTTP endpoints are limited to health and metrics.

## Client SDK

Entry point (`Squirix.Client`):

```csharp
using System;
using System.Threading;
using Squirix.Client;

await using var client = await SquirixClient.ConnectAsync(new Uri("https://localhost:5001"), CancellationToken.None);
var cache = await client.GetCacheAsync<T>("cache-name", CancellationToken.None);
```

`ICache<T>` methods (v0.1 exported surface):

| Method                                   | Purpose                           |
| :--------------------------------------- | :-------------------------------- |
| `AddAsync` / `TryAddAsync`               | Insert if absent                  |
| `SetAsync`                               | Upsert with optional expiration   |
| `UpdateAsync`                            | Update existing entry             |
| `GetValueAsync` / `GetEntryAsync`        | Read with explicit presence       |
| `GetExpirationAsync`                     | Read expiration metadata          |
| `GetOrAddAsync`                          | Read or insert factory value      |
| `RemoveAsync`                            | Delete key                        |
| `TouchAsync` / `RemoveExpirationAsync`   | Expiration management             |

Prefer `GetValueAsync` for reads with explicit presence.

Writes accept `(key, value, options?, cancellationToken)`. Expiration uses `CacheEntryOptions`, not `CacheEntry<T>`
write overloads.

When `options` is omitted or null, or neither `CacheEntryOptions.Expiration` nor `CacheEntryOptions.ExpiresAt` is set,
the entry is stored **without expiration** and **does not expire by TTL**. Pass an explicit relative or absolute expiration
when you need TTL eviction.

Out of scope for v0.1: batch, scan, watch, counters, tag invalidation, compare-and-set.

Configuration (`SquirixClientOptions`): endpoints, JWT bearer token provider, custom serializer.
See [configuration.md](configuration.md) and [serialization.md](serialization.md).

## Wire contract

gRPC contract: `src/shared/Squirix/Transport/Grpc/Protos/SquirixCache.proto` (shared source, not a separate NuGet package).

`SquirixCacheService` exposes **10** unary RPCs on the v0.1 server surface:

| Wire RPC (`SquirixCache.proto`)   | gRPC client / `ICache<T>`   | Notes                                                                                                                 |
| :-------------------------------- | :-------------------------- | :-------------------------------------------------------------------------------------------------------------------- |
| `SetEntry`                        | `SetAsync`                  | Upsert via `CacheEntryWire` (`SetEntryAsync` on generated client)                                                     |
| `TryAddEntry`                     | `TryAddAsync` / `AddAsync`  | Insert-if-absent via `CacheEntryWire` (`TryAddEntryAsync` on generated client); `AddAsync` throws when the key exists |
| `GetValue`                        | `GetValueAsync`             | Value-only read; returns `found` + value                                                                              |
| `GetEntry`                        | `GetEntryAsync`             | Full entry read; returns `found` + entry (missing key → `found=false`, not an error)                                  |
| `GetExpiration`                   | `GetExpirationAsync`        | Expiration metadata only; handler reads via runtime `GetEntry`                                                        |
| `GetOrAdd`                        | `GetOrAddAsync`             | Single RPC with `CacheEntryWire`; client runs factory locally, server get-or-insert atomically                        |
| `Update`                          | `UpdateAsync`               | Update value if key exists via `CacheEntryWire` (value field only)                                                    |
| `Remove`                          | `RemoveAsync`               |                                                                                                                       |
| `Touch`                           | `TouchAsync`                | Relative expiration (`Duration`)                                                                                      |
| `RemoveExpiration`                | `RemoveExpirationAsync`     |                                                                                                                       |

Mutations that accept expiration use `SetEntry` / `TryAddEntry` with `CacheEntryWire`. There are no flat `Set` / `TryAdd`
value-only mutation RPCs on the wire surface.

The server runtime pipeline (`ICacheApi`) is entry-based only (eight methods). gRPC handlers translate wire requests into
that runtime surface; `GetExpiration` and `GetOrAdd` are wire convenience RPCs whose handlers may compose runtime calls
internally — the client SDK calls one RPC per exported method and does not stitch multiple RPCs together.

Wire RPC names omit the `Async` suffix; grpc-dotnet appends it on generated client methods (for example `SetEntry` →
`SetEntryAsync`). Public `ICache<T>` names stay `SetAsync` / `TryAddAsync`.

There is no `Contains` RPC. Prefer `GetValueAsync` or `GetEntryAsync` for presence checks (`found=false` is not an
error). HTTP on the primary listener is limited to health and metrics — there is no REST cache `HEAD` route in v0.1.

Mutating gRPC RPCs require a non-empty `operation_id` of exactly **32 lowercase hex characters** (UUID without
hyphens, for example `0123456789abcdef0123456789abcdef`). The `Squirix` client SDK generates a fresh id per mutating
call via `RpcOperationIdentity.New()`; custom gRPC clients must supply a conforming value. Over-length or malformed ids
are rejected with gRPC `InvalidArgument`. The server deduplicates retries with the same `operation_id` and rejects
reuse with a different payload (`FailedPrecondition`).

### Entry limits (v0.1)

| Limit | Value |
| :---- | :---- |
| Encoded entry payload | 4 MiB |
| Tags per entry | 32 |
| Tag key UTF-8 size | 256 bytes |
| Tag value UTF-8 size | 1024 bytes |

Violations return stable public codes: `INVALID_ENTRY_TAGS` as gRPC `InvalidArgument`, and `PAYLOAD_TOO_LARGE` as
gRPC `ResourceExhausted`. Other live public codes include `INVALID_CACHE_KEY`, `TOO_MANY_REQUESTS`,
`MEMORY_PRESSURE`, `JOURNAL_DISK_QUOTA`, and the `OPERATION_ID_*` family. Cache **names** still fail as
`ArgumentException` → gRPC `InvalidArgument` without a structured `INVALID_CACHE_NAME` code.

In a multi-node cluster, the entry node forwards the **client** `operation_id` to the key owner over inter-node gRPC
instead of minting a new id. Idempotency records are per-node in memory (durable nodes also persist outcomes through the
journal/snapshot path); when a retry lands on a different entry node (bootstrap endpoint switch or transport failover),
the owner node replays the cached outcome for the same `operation_id` and fingerprint so the mutation is not applied
twice. Operators can tune in-memory store caps with `SQUIRIX_IDEMPOTENCY_*` — see
[configuration.md — Environment variables](configuration.md#environment-variables).

The approved RPC list is locked by a golden snapshot test:
`tests/squirix.server/squirix.server.unit-tests/ApiSnapshots/SquirixGrpcEndpointSurface.golden.txt`.

Transport requires HTTPS endpoints. Cleartext `http://` URLs are rejected at configuration time.

Authentication uses JWT bearer tokens when enabled via `SquirixClientOptions.BearerTokenProvider`.

## Cache names

Validation rules for logical cache names: [cache-name-validation.md](cache-name-validation.md).
