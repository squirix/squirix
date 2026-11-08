# API reference (v0.1 preview)

squirix exposes a typed gRPC client SDK. Cache operations use gRPC only; HTTP endpoints are limited to health and metrics.

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

Out of scope for v0.1: batch, scan, watch, counters, tag invalidation, compare-and-set.

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

Other gRPC failures remain `Grpc.Core.RpcException`; inspect `StatusCode` and server details. For example, missing or
invalid credentials yield `Unauthenticated`, and payload or capacity refusals use `ResourceExhausted`.
`Squirix.Server` exception types are not the SDK's remote exception contract; applications do not need to reference the
server package to handle these failures. See [retry and failover behavior](bootstrap-client-failover.md#per-operation-failover-v01-exported-client).

## Wire contract

gRPC contract: `src/shared/Squirix/Transport/Grpc/Protos/SquirixCache.proto` (shared source, not a separate NuGet package).

`SquirixCacheService` exposes **10** unary RPCs on the v0.1 server surface:

| Wire RPC (`SquirixCache.proto`)   | gRPC client / `ICache<T>`   | Notes                                                                                                                 |
| :-------------------------------- | :-------------------------- | :-------------------------------------------------------------------------------------------------------------------- |
| `SetEntry`                        | `SetAsync`                  | Upsert via `CacheEntryWire` (`SetEntryAsync` on generated client)                                                     |
| `TryAddEntry`                     | `TryAddAsync` / `AddAsync`  | Insert-if-absent via `CacheEntryWire` (`TryAddEntryAsync` on generated client); `AddAsync` throws when the key exists |
| `GetValue`                        | `GetValueAsync`             | Value-only read; returns `found` + value                                                                              |
| `GetEntry`                        | `GetEntryAsync`             | Full entry read; returns `found` + entry (missing key → `found=false`, not an error) and the time left (`remaining`)  |
| `GetExpiration`                   | `GetExpirationAsync`        | Expiration metadata only; handler reads via runtime `GetEntry`                                                        |
| `GetOrAdd`                        | `GetOrAddAsync`             | Single RPC with `CacheEntryWire`; client runs factory locally, server get-or-insert atomically; no value on add       |
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

### Value encoding

Every service lives in the proto package `squirix.cache.v2`, so a client and a server that disagree on the value encoding
meet as `Unimplemented` instead of silently corrupting values. A `CacheValue` is one of:

| Kind | Used for |
| :--- | :------- |
| `null_value` | `null` |
| `string_value` | `string` |
| `int64_value` | `int` and `long` |
| `double_value` | `double` |
| `bool_value` | `bool` |
| `json_utf8` | everything else: objects, arrays, `decimal`, numbers of a JSON element |

`json_utf8` holds exactly one JSON value as UTF-8: no byte order mark, valid UTF-8, no comments, no trailing commas, nesting
of at most 64 levels. The server checks it and refuses anything else with `InvalidArgument` before any durable effect, then
stores, journals, replicates and returns the very same bytes: it does not parse, reformat or canonicalize the JSON, so every
digit of a number survives. A `Remove` response carries only `removed`. A `GetOrAdd` response carries the value only when
the key already held one (`added=false`, `found=true`); when it added, the caller already has the value.

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

The 4 MiB limit applies to the encoded entry, including value encoding, expiration, version, and tag metadata, not just
to the original application value. JSON encoding can increase size; for example, byte arrays become base64 text with the
default serializer. Leave room for entry metadata rather than treating a 4 MiB application payload as admissible.
The v0.1 SDK has no public API for assigning entry tags; the tag limits also apply to raw gRPC entries.

Violations return stable public codes: `INVALID_ENTRY_TAGS` as gRPC `InvalidArgument`, and `PAYLOAD_TOO_LARGE` as
gRPC `ResourceExhausted`. Other live public codes include `INVALID_CACHE_KEY`, `TOO_MANY_REQUESTS`,
`MEMORY_PRESSURE`, `JOURNAL_DISK_QUOTA`, and the `OPERATION_ID_*` family. Cache **names** still fail as
`ArgumentException` → gRPC `InvalidArgument` without a structured `INVALID_CACHE_NAME` code.

In a multi-node cluster, the entry node forwards the client request to the key owner (the replica group leader when automatic failover is on) over internode gRPC unchanged, with
the **client** `operation_id` and without running the cache pipeline itself. Only the owner records the outcome:
idempotency records are per-node in memory (durable nodes also persist outcomes through the journal/snapshot path), and
the entry node keeps none. When a retry lands on a different entry node (bootstrap endpoint switch or transport
failover), the owner node replays the cached outcome for the same `operation_id` and fingerprint so the mutation is not
applied twice. In a replicated cache an outcome is replayable for at least 30 seconds by the clock of the node that answers the retry; with
node clocks within 10 seconds of each other, it is replayable for at least 20 seconds after the leader decided it, across a leader change or
a restart too. The `squirix` client retries within its 15 s operation deadline, so it is covered. A raw gRPC client that retries later than
that may get a new execution of the mutation. On an RF=1 node an outcome is replayable for at least 30 seconds, and up to 15 minutes while
the byte budget of the store has room.
The owner response and errors reach the client unchanged; an unreachable owner surfaces as `Unavailable`.
Operators can tune the in-memory store caps of an RF=1 node with `SQUIRIX_IDEMPOTENCY_*` — see
[configuration.md — Idempotency store capacity](configuration.md#idempotency-store-capacity). A replicated host dedupes every write in
the replica group of its key and keeps no node-wide record of it. There, a conditional write that took no effect (`TryAddEntry` or
`GetOrAdd` over a present key, `Update` of an absent key) is evaluated again on retry instead of replayed, and the read of `GetOrAdd`
reflects the current value.

The approved RPC list is locked by a golden snapshot test:
`tests/squirix.server/squirix.server.unit-tests/ApiSnapshots/SquirixGrpcEndpointSurface.golden.txt`.

Transport requires HTTPS endpoints. Cleartext `http://` URLs are rejected at configuration time.

When JWT authentication is enabled on the server, set `SquirixClientOptions.BearerTokenProvider` to supply client tokens.
Setting the client provider does not enable authentication on the server. See [Configuration](configuration.md).

## Cache keys

Keys are strings of at most 1024 UTF-16 code units (`string.Length`), not 1024 UTF-8 bytes. Null, empty, whitespace-only
keys and keys containing control characters are rejected with `ArgumentException` by the SDK. Keys are not restricted
to the ASCII character set required for cache names. Raw gRPC requests with invalid keys are rejected with
`InvalidArgument` and the `INVALID_CACHE_KEY` code.

## Cache names

`T` in `ICache<T>` selects client serialization and deserialization; it is not part of the server-side cache identity.
`GetCacheAsync<Document>("shared", ...)` and `GetCacheAsync<object>("shared", ...)` address the same entries. The server
does not create a separate namespace for each CLR type or enforce a schema for the cache. Coordinate payload schemas
between applications, or use distinct cache names for incompatible types.

Validation rules for logical cache names: [cache-name-validation.md](cache-name-validation.md).
