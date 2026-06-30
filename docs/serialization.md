# Serialization and Serializer Customization

squirix uses `ISquirixSerializer` to materialize arbitrary POCO cache values before binary wire encoding, and for
server durability paths that still accept JSON-shaped inputs. The default implementation is `SystemTextJsonSerializer`
(`System.Text.Json` with relaxed web defaults).

Client and server packages keep **separate** serializer hosts:

- **Client** (`Squirix.Serialization.Provider`): immutable default used by transport helpers; each
  `SquirixClient.ConnectAsync` session gets its own serializer from `SquirixClientOptions.Serializer`.
- **Server:** uses a built-in JSON encoder for durability and adapters. `AddSquirixServerAsync` / `SquirixServer.StartAsync`
  do not expose a serializer hook on `SquirixServerOptions`.

## gRPC wire encoding

- **Scalar values** on `GetValue` / `Remove` / `GetOrAdd` responses use protobuf `CacheValue` oneof fields
  (`string_value`, `int64_value`, `double_value`, `bool_value`, `null_value`) when the CLR type is a primitive.
- **Structured values** use `CacheValue.payload` / `CacheEntryWire.payload` as a **binary value blob** (same tagged value
  kinds as journal/snapshot payloads — see `docs/snapshot-format.md` value kinds section).
- `SetAsync` / entry mutations always encode the value into `CacheEntryWire.payload` as binary, including scalars.

- **Structured POCOs** encode and decode through `JsonTypeInfo` metadata walkers (`BinaryJsonTreeMetadataCodec`): property
  getters/setters read and write the binary tree directly. No UTF-8 JSON text is sent on the wire.
- Scalar and leaf values inside the tree use direct binary kinds; `DateTimeOffset` and enums use `ValueKind.Int64`.
- Positional records without `CreateObject` are materialized with `RuntimeHelpers.GetUninitializedObject` plus metadata
  `Set` delegates (init properties).

`ISquirixSerializer` is still used to resolve `JsonTypeInfo` for arbitrary POCO types on the wire path; it does **not**
serialize payloads to UTF-8 JSON for transport.

## Remote client serializer

Pass a custom serializer when connecting:

```csharp
using System;
using System.Threading;
using Squirix;
using Squirix.Client;
using Squirix.Serialization;

await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add("https://localhost:5001");
        options.Serializer = new MyCustomSerializer();
    },
    CancellationToken.None);

ICache<MyDocument> cache = await client.GetCacheAsync<MyDocument>("docs", CancellationToken.None);

await cache.SetAsync(
    "doc:1",
    new MyDocument { Id = 1 },
    new CacheEntryOptions { Expiration = TimeSpan.FromHours(1) },
    CancellationToken.None);
```

Leave `Serializer` null to use the default `SystemTextJsonSerializer` for that session only. The choice does **not**
mutate process-wide client state and does **not** change the server journal/snapshot encoder.

## Compatibility and version tolerance

Serializer swapping is safe only when encoders agree on payload shape:

- Server journal/snapshot paths store binary cache-entry blobs; structured POCOs encode through the metadata walker.
- Treat serializer choice as part of your persisted payload contract; test mixed history before rollout.
- If a new serializer cannot read existing payloads, plan a storage migration instead of a drop-in swap.
- gRPC wire payloads are binary-only; JSON UTF-8 in `bytes payload` is not supported.

## Thread safety

- Default `SystemTextJsonSerializer` is stateless and thread-safe.
- Custom implementations must be safe for concurrent use or provide their own synchronization.
- Each `SquirixClient` session holds one serializer instance; different clients may use different implementations in the
  same process.

## Server nodes

- Standalone and embedded server hosts encode arbitrary POCOs through the metadata walker before binary persistence
  and wire encoding.
- **Clients** choose the serializer per `SquirixClient.ConnectAsync` session (`SquirixClientOptions.Serializer`).
- Server and client serializers must agree on POCO shape for a given cache; mismatched encoders against existing on-disk
  data require migration.

## Diagnostics

- Journal and snapshots store encoded payloads verbatim; encoder changes affect on-disk format.
- Serializer metrics appear in the Prometheus scrape output when the `/metrics` endpoint is enabled
  (`squirix_serializer_*`).

## Further reading

- `Squirix.Serialization.ISquirixSerializer`
- `Squirix.Serialization.SystemTextJsonSerializer`
- `docs/snapshot-format.md` (binary value kinds)
