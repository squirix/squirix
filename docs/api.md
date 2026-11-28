# API reference (v0.1 preview)

The Squirix client SDK uses a binary protocol over TCP with optional TLS for cache operations. HTTP endpoints serve health and metrics.

Preview.11 also provides an [`IDistributedCache` adapter](distributed-cache.md) for ASP.NET Core and HybridCache.

## Client SDK

Entry point (`Squirix.Client`):

```csharp
using System;
using System.Threading;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

await using var client = await SquirixClient.ConnectAsync(new Uri("https://localhost:5001"), cancellationToken);
var cache = await client.GetCacheAsync<string>("cache-name", cancellationToken);
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

For application-specific value types, register `SquirixClientOptions.JsonSerializerContexts` before connecting.
See the runnable example in [Serialization](serialization.md#application-value-types).

### Lookup and conditional results

Check `Found` before interpreting a lookup value. A live entry can contain `null`; `Value == null` alone does not mean
the key is missing. A missing value-type entry can likewise have a default value such as `0`.

`GetExpirationAsync` distinguishes three states:

| Entry state | `Found` | `HasExpiration` | `Expiration` |
| --- | --- | --- | --- |
| Missing or expired | `false` | `false` | `null` |
| Live, without TTL | `true` | `false` | `null` |
| Live, with TTL | `true` | `true` | Remaining `TimeSpan` |

`UpdateAsync`, `TouchAsync`, and `RemoveAsync` return `false` for a missing or expired key; they do not create it.
`TryAddAsync` returns `false` for an existing live entry, including one whose value is `null`.
`GetOrAddAsync` also preserves an existing `null`; its result has `Found = true` and `Value = null`.

### GetOrAdd factory execution

`GetOrAddAsync` runs the value factory on the client before sending its get-or-insert request. The server then returns
the existing value or inserts the proposed value atomically. The factory can therefore run even when the key already
exists; do not assume that it runs only on a cache miss or use it for side effects that must happen exactly once.

Concurrent callers sharing the same client-side cache instance and key share in-flight factory work. Separate client
sessions or processes can run their factories independently. For cache-aside, call `GetValueAsync` first and load from the source
on a miss; that sequence alone does not prevent concurrent misses from loading the same data.

### Expiration

`AddAsync`, `TryAddAsync`, `SetAsync`, and `GetOrAddAsync` accept `CacheEntryOptions`. For the first three, the
arguments are `(key, value, options?, cancellationToken)`; `GetOrAddAsync` takes a factory instead of the value.
`UpdateAsync` changes only the value and preserves the current expiration. `CacheEntry<T>` is a read model, not a
write-argument overload.

Set at most one of `Expiration` and `ExpiresAt`. Specifying both throws `ArgumentException` before sending the write.
Relative expiration is a lifetime applied when the server processes the write; reads do not renew it automatically.
Use `TouchAsync` to set a new deadline explicitly. This is not automatic sliding expiration.

When `options` is omitted or null, or neither `CacheEntryOptions.Expiration` nor `CacheEntryOptions.ExpiresAt` is set,
the entry is stored **without expiration** and **does not expire by TTL**. Pass an explicit relative or absolute expiration
when you need TTL eviction.

An absolute expiration (`CacheEntryOptions.ExpiresAt`, `TouchAsync(key, DateTimeOffset)`) is on the **client clock**. The client
sends the time left before it, so the entry lives that long even when the client and server clocks disagree. A value that is not
in the future on the client clock is rejected with `ArgumentOutOfRangeException`. `GetEntryAsync` reports `ExpiresUtc` on the
client clock too: the server returns the time the entry has left, and the client adds it to its own time.

`SetAsync` overwrites the whole entry, including its expiration: a `SetAsync` without expiration on a key that already has
a TTL **clears that TTL**, and the key no longer expires. To change only the value and keep the current expiration, use
`UpdateAsync`.

In v0.1, `ICache<T>` omits batch, scan, watch, counters, tag invalidation, and compare-and-set. For tag
invalidation through `RemoveByTagAsync`, see [HybridCache across processes](distributed-cache.md#hybridcache-across-processes).

Configuration (`SquirixClientOptions`): endpoints, JWT bearer token provider, custom serializer.
See [configuration.md](configuration.md) and [serialization.md](serialization.md).

### Errors returned to applications

Missing keys are normal lookup results: check `Found` on the returned value or entry result. `TryAddAsync` returns
`false` when the key exists; `AddAsync` throws `CacheConflictException` for the same conflict.

The SDK maps specific server responses to public exceptions:

| Server response | SDK exception |
| --- | --- |
| Required operation id is missing | `OperationIdRequiredException` |
| Operation id is reused with a different fingerprint | `OperationIdReuseMismatchException` |
| `COMMIT_OUTCOME_UNKNOWN` | `CommitOutcomeUnknownException` |

Other transport failures surface as `Squirix.Transport.SquirixTransportException`. Inspect `StatusCode` and `Message`. For example, missing or
invalid credentials yield `Unauthenticated`, and payload or capacity refusals use `ResourceExhausted`.
`Squirix.Server` exception types are not the SDK's remote exception contract; applications do not need to reference the
server package to handle these failures. See [retry and failover behavior](bootstrap-client-failover.md#per-operation-failover-v01-exported-client).

## Wire contract

The SDK and server share a versioned binary cache codec. See [Architecture](architecture.md) for the client and peer protocol boundaries.

The binary protocol exposes **13** cache operations:

| Wire RPC | SDK API | Notes |
| :-------------------------------- | :-------------------------- | :-------------------------------------------------------------------------------------------------------------------- |
| `SetEntry` | `SetAsync` | Upsert via `CacheEntryWire` |
| `TryAddEntry` | `TryAddAsync` / `AddAsync` | Insert-if-absent via `CacheEntryWire`; `AddAsync` throws when the key exists |
| `GetValue` | `GetValueAsync` | Value-only read; returns `found` + value |
| `GetEntry` | `GetEntryAsync` | Full entry read; returns `found` + entry (missing key → `found=false`, not an error) and the time left (`remaining`) |
| `GetExpiration` | `GetExpirationAsync` | Expiration metadata only; handler reads via runtime `GetEntry` |
| `GetOrAdd` | `GetOrAddAsync` | Single RPC with `CacheEntryWire`; client runs factory locally, server get-or-insert atomically; no value on add |
| `Update` | `UpdateAsync` | Update value if key exists via `CacheEntryWire` (value field only) |
| `Remove` | `RemoveAsync` | Removes an entry and reports whether it was present. |
| `Touch` | `TouchAsync` | Relative expiration in .NET ticks |
| `RemoveExpiration` | `ICache<T>.RemoveExpirationAsync` | Clears an existing expiration. |
| `SetDistributed` | `IDistributedCache.SetAsync` | Stores opaque bytes and their expiration policy. |
| `GetDistributed` | `IDistributedCache.GetAsync` | Reads the current value and renews sliding expiration when applicable. |
| `RefreshDistributed` | `IDistributedCache.RefreshAsync` | Renews sliding expiration without returning the value. |

Mutations that accept expiration use `SetEntry` / `TryAddEntry` with `CacheEntryWire`. There are no flat `Set` / `TryAdd`
value-only mutation RPCs on the wire surface.

The server runtime pipeline (`ICacheApi`) is entry-based only (eight methods). Cache handlers translate wire requests into
that runtime surface; `GetExpiration` and `GetOrAdd` are wire convenience RPCs whose handlers may compose runtime calls
internally — the client SDK calls one RPC per exported method and does not stitch multiple RPCs together.

Wire operation names use the `BinaryOperation` enum. Public `ICache<T>` methods use their asynchronous API names.

There is no `Contains` RPC. Prefer `GetValueAsync` or `GetEntryAsync` for presence checks (`found=false` is not an
error). The management listener serves health and metrics. Cache operations use the binary protocol.

### Value encoding

Cache values carry an explicit kind so an empty binary value and a present null stay distinct from a cache miss.

| Kind | Used for |
| :--- | :------- |
| `null_value` | `null` |
| `string_value` | `string` |
| `int64_value` | `int` and `long` |
| `double_value` | `double` |
| `bool_value` | `bool` |
| `json_utf8` | JSON values such as objects, arrays, `decimal`, and numbers of a JSON element |
| `binary_value` | opaque bytes, including an empty byte array (preview.11) |

`json_utf8` holds exactly one JSON value as UTF-8: no byte order mark, valid UTF-8, no comments, no trailing commas, nesting
of at most 64 levels. The server checks it and refuses anything else with `InvalidArgument` before any durable effect, then
stores, journals, replicates and returns the very same bytes: it does not parse, reformat or canonicalize the JSON, so every
digit of a number survives.

The SDK sends scalar `JsonElement` values as native kinds where possible. Numbers, objects, and arrays use their raw
JSON text. `ICache<object>` reads native integers as `long` and JSON values as `JsonElement`. When the server returns
`binary_value`, both `ICache<byte[]>` and `ICache<object>` read it as `byte[]`.

A `Remove` response carries only `removed`. A `GetOrAdd` response carries the value only when
the key already held one (`added=false`, `found=true`); when it added, the caller already has the value.

On a replicated deployment, a mutation carries an `operation_id` of 32 lowercase hexadecimal characters. The SDK
generates it once per call and uses it for retries. A raw binary client must supply it on replicated mutations and
distributed adapter access. The replica group rejects a malformed identifier or reuse of the same identifier with a
different mutation in the same cache and group.

On a node without replica groups, each request is executed against the current state. If an attempted mutation may
have run and the reply cannot be confirmed, the SDK throws `CommitOutcomeUnknownException`.

The adapter's `GetDistributed` reads the current value on every execution and atomically renews the observed entry
when necessary. A retry may return newer bytes or a miss. Replicated renewals carry a separate server identity. The
replica group retains only the boolean result of a committed removal.

### Entry limits (v0.1)

| Limit | Value |
| :---- | :---- |
| Serialized value without replication | 512 MiB |
| Serialized value with replication | 32 MiB |
| Tags per entry | 32 |
| Tag key UTF-8 size | 256 bytes |
| Tag value UTF-8 size | 1024 bytes |

The ingress server checks the serialized value length before admitting the write. Key, expiry, tags and protocol metadata have separate bounds.
The typed SDK measures the value in its serialized representation, including any serializer expansion. The distributed cache adapter sends bytes directly.
The v0.1 SDK has no public API for assigning entry tags; the tag limits also apply to raw binary entries.

Violations return stable public codes: `INVALID_ENTRY_TAGS` as `InvalidArgument`, and `PAYLOAD_TOO_LARGE` as
`ResourceExhausted`. Other live public codes include `INVALID_CACHE_KEY`, `TOO_MANY_REQUESTS`,
`MEMORY_PRESSURE`, `JOURNAL_DISK_QUOTA`, and the `OPERATION_ID_*` family. Cache **names** still fail as
`ArgumentException` → `InvalidArgument` without a structured `INVALID_CACHE_NAME` code.

The entry node forwards a request to the key owner. With replica groups, the group can replay a committed outcome
across leader changes and restarts. The window is at least 30 seconds by the answering node's clock, or at least
20 seconds after the leader decided it when clocks differ by at most 10 seconds. The SDK's 15-second deadline
fits inside that window. Conditional requests that produced no commit are evaluated against current state on retry.

The public binary operation names and wire IDs are recorded in a golden snapshot:
`tests/squirix.server/squirix.server.unit-tests/ApiSnapshots/SquirixBinaryOperations.golden.txt`.

The SDK accepts `https://` endpoints and uses TLS for the binary transport.

When JWT authentication is enabled on the server, set `SquirixClientOptions.BearerTokenProvider` to supply a token for each new physical connection.
Subsequent RPCs use the bound identity without bearer metadata; see [cache connection authentication](security/connection-authentication.md).
Setting the client provider does not enable authentication on the server. See [Configuration](configuration.md).

## Cache keys

Keys are strings of at most 1024 UTF-16 code units (`string.Length`), not 1024 UTF-8 bytes. Null, empty, whitespace-only
keys and keys containing control characters are rejected with `ArgumentException` by the SDK. Keys are not restricted
to the ASCII character set required for cache names. Raw binary requests with invalid keys are rejected with
`InvalidArgument` and the `INVALID_CACHE_KEY` code.

## Cache names

`T` in `ICache<T>` selects client serialization and deserialization; it is not part of the server-side cache identity.
`GetCacheAsync<Document>("shared", ...)` and `GetCacheAsync<object>("shared", ...)` address the same entries. The server
does not create a separate namespace for each CLR type or enforce a schema for the cache. Coordinate payload schemas
between applications, or use distinct cache names for incompatible types.

Validation rules for logical cache names: [cache-name-validation.md](cache-name-validation.md).
