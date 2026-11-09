# Squirix distributed cache provider

Use `squirix.extensions.caching` to store `IDistributedCache` values on a Squirix server. Use the same preview release of the adapter, client and server.

**Early preview. Evaluate it before production use.**

```csharp
using Microsoft.Extensions.DependencyInjection;

builder.Services.AddSquirixCache(options =>
{
    options.ClientOptions.Endpoints.Add(new Uri("https://cache.example.com:7443"));
});
```

To use Squirix as the secondary cache for Microsoft HybridCache, register the provider as above and call `builder.Services.AddHybridCache()` from the separate `Microsoft.Extensions.Caching.Hybrid` package. HybridCache manages its local cache in the application. Squirix stores the secondary values on the server.

```csharp
using Microsoft.Extensions.Caching.Hybrid;

builder.Services.AddHybridCache();

var app = builder.Build();
app.MapGet("/cached-message", async (HybridCache cache, CancellationToken token) =>
    await cache.GetOrCreateAsync(
        "demo:message",
        static _ => ValueTask.FromResult("Hello from HybridCache"),
        cancellationToken: token));
app.Run();
```

Registration replaces other unkeyed `IDistributedCache` providers. Repeated calls compose configuration in order and keep one singleton. The provider connects lazily, reuses its client, and is disposed with the service provider. Configure a bearer token delegate through `ClientOptions.BearerTokenProvider` when required.

`Get` returns null for a missing entry. An empty stored value returns an empty array. `Set` replaces the value and its expiration policy together. When both absolute options are set, the earlier deadline wins. `Get` and `Refresh` renew sliding entries up to any absolute cap. `Remove` deletes the entry and its policy.

The provider stores the bytes supplied by the caller or HybridCache. Native Squirix serializers and JSON contexts are unused. Avoid sharing the configured cache namespace with a native application that writes its own values.

The asynchronous methods accept caller cancellation. Cancellation can end the wait for a remote mutation after the server has already accepted it. The synchronous methods block until their corresponding operation completes. An asynchronous bearer token provider should honor cancellation. Disposal cancels pending work and waits up to 30 seconds for the owned client to shut down.
