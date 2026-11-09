# Microsoft distributed caching with Squirix

`squirix.extensions.caching` is the official Squirix provider for `IDistributedCache`. ASP.NET Core and Microsoft HybridCache can use it to store byte payloads on a Squirix server. The adapter, client, and server use the same preview version. **Preview software. Evaluate it before production use.**

## Install and register

When preview.11 is available on your package source, install:

```shell
dotnet add package squirix.extensions.caching --version 0.1.0-preview.11
dotnet add package Microsoft.Extensions.Caching.Hybrid --version 10.11.0
```

Use the HybridCache package only if the application needs HybridCache.

```csharp
using Microsoft.Extensions.DependencyInjection;

builder.Services.AddSquirixCache(options =>
{
    options.CacheName = "application-cache";
    options.ClientOptions.Endpoints.Add(new Uri("https://cache.example.com:7443"));
});
builder.Services.AddHybridCache();
```

Use HTTPS endpoints for the same logical Squirix service. Set `ClientOptions.BearerTokenProvider` when the server requires a token. `AddSquirixCache` replaces existing unkeyed `IDistributedCache` registrations. It registers one singleton that connects on first use and shuts down with the service provider.

## Expiration and errors

- `Get` returns null for a missing entry and an empty byte array for a stored empty value.
- `Set` replaces the bytes and expiration options together. If both absolute options are supplied, the earlier deadline wins. Empty options store the entry without expiration.
- `Get` and `Refresh` renew sliding entries on the server, up to any absolute deadline. `Refresh` leaves missing and absolute-only entries alone.
- Backend failures and cancellation reach the application. A cancelled wait may leave a mutation committed.

## HybridCache across processes

HybridCache serializes its own payload and maintains a separate L1 in each application process. A remote update or removal can leave another process's local copy readable until its `LocalCacheExpiration` elapses. Choose that duration for the application's staleness tolerance. HybridCache's stampede protection applies within an application instance.

Keys must fit within 1,024 UTF-16 code units. Server values have a 4 MiB encoded-entry limit, including Squirix metadata. HybridCache adds its own envelope and tag metadata, and replicated sliding entries also have a combined write and replay-response limit. Large values and high-rate sliding reads can reach server admission limits.

Run the [two-process ASP.NET Core example](../samples/aspnet-hybrid-cache/README.md) to see a fresh application reuse an existing L2 value.
