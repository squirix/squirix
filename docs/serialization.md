# Serialization and Serializer Customization

squirix uses `ISquirixSerializer` for cache payloads on the wire and in server durability paths. The default
implementation is `SystemTextJsonSerializer` (`System.Text.Json` with relaxed web defaults).

Client and server packages keep **separate** serializer hosts:

- **Client** (`Squirix.Serialization.SerializationProvider`): immutable default used by transport helpers; each
  `SquirixClient.ConnectAsync` session gets its own serializer from `SquirixOptions.Serializer`.
- **Server:** uses a built-in JSON encoder for durability and adapters. `AddSquirixServer` / `SquirixServer.StartAsync`
  do not expose a serializer hook on `SquirixServerOptions`.

## Remote client serializer

Pass a custom serializer when connecting:

```csharp
using System;
using System.Threading;
using Squirix;
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

- Server journal/snapshot paths require JSON-compatible UTF-8 for stored values when using the default stack.
- Treat serializer choice as part of your persisted payload contract; test mixed history before rollout.
- If a new serializer cannot read existing payloads, plan a storage migration instead of a drop-in swap.

## Thread safety

- Default `SystemTextJsonSerializer` is stateless and thread-safe.
- Custom implementations must be safe for concurrent use or provide their own synchronization.
- Each `SquirixClient` session holds one serializer instance; different clients may use different implementations in the
  same process.

## Server nodes

- Standalone and embedded server hosts use the default JSON encoder for journal, snapshots, and gRPC/REST payloads.
- **Clients** choose the serializer per `SquirixClient.ConnectAsync` session (`SquirixOptions.Serializer`).
- Server and client serializers must agree on payload shape for a given cache; mismatched encoders against existing
  on-disk data require migration.

## Diagnostics

- Journal and snapshots store encoded payloads verbatim; encoder changes affect on-disk format.
- Serializer metrics appear in the Prometheus scrape output when the `/metrics` endpoint is enabled
  (`squirix_serializer_*`).

## Further reading

- `Squirix.Serialization.ISquirixSerializer`
- `Squirix.Serialization.SystemTextJsonSerializer`
