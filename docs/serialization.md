# Serialization and Serializer Customization

squirix uses `ISquirixSerializer` for cache payloads on the wire and in server durability paths. The default
implementation is `SystemTextJsonSerializer`, backed by source-generated `System.Text.Json` metadata. JSON property
names, converters, and other serialization settings come from the context that supplies that metadata; Squirix does not
impose ASP.NET Core web defaults on application contexts. Both types live in the
`Squirix` namespace (`ISquirixSerializer` is public; `SystemTextJsonSerializer` is the internal default).

Client and server packages keep **separate** serializer hosts:

- **Client:** each `SquirixClient.ConnectAsync` session gets its own serializer from `SquirixClientOptions.Serializer`
  (or the built-in default when unset). The serializer instance is session-local; application JSON metadata registration is additive and process-wide.
- **Server:** keeps structured values as the validated UTF-8 JSON bytes the client sent and uses a built-in JSON encoder for
  the rare values it has to serialize itself. `AddSquirixServerAsync` / `SquirixServer.StartAsync`
  do not expose a serializer hook on `SquirixServerOptions`.

## Application value types

The default serializer requires source-generated JSON metadata for application-specific types. Add a
`JsonSerializerContext` covering each type to `SquirixClientOptions.JsonSerializerContexts` before connecting.
Without registered metadata, these types throw `InvalidOperationException`; the serializer does not fall back to reflection.
Native protobuf values such as strings do not need an application JSON context. The built-in metadata also covers
`decimal` and `JsonElement`. Other collection and binary types can need registration too: for example, add
`[JsonSerializable(typeof(byte[]))]` to an application context before using `ICache<byte[]>`. A byte array travels as a
JSON base64 string through the serializer, not as a native protobuf byte-array kind.

With the local HTTPS server running, create a .NET 10 console app and add the client:

```bash
dotnet new console --framework net10.0 -o TypedCacheDemo
dotnet add TypedCacheDemo/TypedCacheDemo.csproj package squirix --version 0.1.0-preview.10
```

Replace `TypedCacheDemo/Program.cs` with:

```csharp
using System;
using System.Text.Json.Serialization;
using System.Threading;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

await using var client = await SquirixClient.ConnectAsync(
    static options =>
    {
        options.Endpoints.Add(new Uri("https://localhost:5001"));
        options.JsonSerializerContexts.Add(DocumentJsonContext.Default);
    },
    cancellationToken);

var cache = await client.GetCacheAsync<MyDocument>("documents", cancellationToken);
await cache.SetAsync("doc:1", new MyDocument { Id = 1 }, null, cancellationToken);

var result = await cache.GetValueAsync("doc:1", cancellationToken);
Console.WriteLine(result.Found ? $"Read document: {result.Value!.Id}" : "<missing>");

public sealed class MyDocument
{
    public int Id { get; init; }
}

[JsonSerializable(typeof(MyDocument))]
internal partial class DocumentJsonContext : JsonSerializerContext
{
}
```

Run `dotnet run --project TypedCacheDemo`. Expected output: `Read document: 1`.
For server startup and certificate trust, see [Getting started](getting-started.md).

Context registration is additive and process-wide; a context registered when one client connects can be consulted by
other default client serializers in that process. This does not replace another session's custom serializer.

## Remote client serializer

The following fragment assumes you provide `MyCustomSerializer`, implementing `ISquirixSerializer`, and a
`MyDocument` value type. A custom serializer supplies its own metadata and must produce JSON-compatible UTF-8.
Pass it when connecting:

```csharp
using System;
using System.Threading;
using Squirix;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
var cancellationToken = timeout.Token;

await using var client = await SquirixClient.ConnectAsync(
    static options =>
    {
        options.Endpoints.Add(new Uri("https://localhost:5001"));
        options.Serializer = new MyCustomSerializer();
    },
    cancellationToken);

ICache<MyDocument> cache = await client.GetCacheAsync<MyDocument>("docs", cancellationToken);

await cache.SetAsync(
    "doc:1",
    new MyDocument { Id = 1 },
    new CacheEntryOptions { Expiration = TimeSpan.FromHours(1) },
    cancellationToken);
```

Leave `Serializer` null to use the default `SystemTextJsonSerializer` for that session only. The choice does **not**
mutate process-wide client state and does **not** change the server journal/snapshot encoder.

## What travels on the wire

`null`, `string`, `bool`, `double` and the integers `int` and `long` travel as native protobuf kinds and never touch the
serializer. A `JsonElement` travels as its scalar kind, or for a number, an object or an array as its own raw text. Every
other value, including `decimal`, is turned into UTF-8 bytes by `ISquirixSerializer.SerializeToUtf8Bytes` and sent as one
JSON value; the server refuses bytes that are not exactly one valid JSON value (see [api.md](api.md#value-encoding)).
A custom serializer therefore has to produce JSON. The bytes are stored, journaled and replicated as received, so
number text and property order are never rewritten.

A cache of `object` reads back `null`, `string`, `bool`, `long` and `double` as such (an `int` reads as `long`), and
every JSON value, including every number sent as a `JsonElement`, as a `JsonElement`.

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

- Standalone and embedded server hosts store structured values as received and use the default JSON encoder for the
  values they serialize themselves.
  Health/metrics HTTP responses use the server's JSON serializer context separately.
- **Clients** choose the serializer per `SquirixClient.ConnectAsync` session (`SquirixClientOptions.Serializer`).
- Clients that share entries must agree on their payload schema. The server does not deserialize application DTOs and
  does not need their source-generated contexts. A change that makes clients unable to read stored payloads needs an
  application data migration or a new cache namespace.

## Diagnostics

- Journal and snapshots store structured values verbatim as UTF-8 JSON. Changing a client encoder can change the stored
  payload schema; it does not by itself change the journal or snapshot framing format.
- Client sessions record serializer metrics (`squirix_serializer_*`) on the client-side `Squirix` .NET meter; export
  them from the client process with OpenTelemetry or a `MeterListener`. Server nodes do not record serializer metrics.

## Further reading

- `Squirix.ISquirixSerializer`
- Internal default: `Squirix.SystemTextJsonSerializer`
