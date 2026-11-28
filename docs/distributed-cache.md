# Microsoft distributed caching with Squirix

`squirix.extensions.caching` is the official Squirix provider for `IDistributedCache`.
ASP.NET Core and Microsoft HybridCache can use it to store byte payloads on a Squirix server. The adapter, client, and server use the same preview version. **Preview software.
Evaluate it before production use.**

## Switch from Redis or Garnet to Squirix

If your HybridCache app uses Redis or Garnet through `AddStackExchangeRedisCache`, install the Squirix provider:

```shell
dotnet add package squirix.extensions.caching --prerelease
```

Replace the `AddStackExchangeRedisCache(...)` registration with `AddSquirixCache`. Keep the existing `AddHybridCache()` registration.

```csharp
using Microsoft.Extensions.DependencyInjection;

builder.Services.AddSquirixCache(options =>
    options.ClientOptions.Endpoints.Add(new Uri("https://cache.example.com:7443")));
builder.Services.AddHybridCache();
```

Use the endpoint printed by the server: `tcp://` for plain TCP or `https://` for TLS. Existing `GetOrCreateAsync` calls stay the same.

Use a dedicated Squirix deployment per application to keep keys isolated.
Switch all instances of the app together. Redis or Garnet and Squirix do not share cached entries or tag invalidations.

Use endpoints for the same logical Squirix service. Set `ClientOptions.BearerTokenProvider` when the server requires a token.
`AddSquirixCache` replaces existing unkeyed `IDistributedCache` registrations. It registers one singleton that connects on first use and shuts down with the service provider.

## Expiration and errors

- `Get` returns null for a missing entry and an empty byte array for a stored empty value.
- `Set` replaces the bytes and expiration options together. The server owner resolves relative TTL against its clock.
  If both absolute options are supplied, the earlier deadline wins. Empty options store the entry without expiration.
- `Get` and `Refresh` renew sliding entries on the server, up to any absolute deadline. `Refresh` leaves missing and absolute-only entries alone.
- Each network attempt of `Get` reads the current entry and renews that same entry atomically. A retry may return newer bytes or a miss.
- Backend failures and cancellation reach the application. A cancelled wait may leave a mutation committed.

## Upgrading this preview

Upgrade the adapter, client and all server nodes together. Stop the old nodes and use empty data directories for this preview format.
Older data directories and replication snapshots are rejected; mixed-version rolling upgrades are unsupported.
Keep the previous data directories and binaries together if a rollback is needed.

## HybridCache across processes

HybridCache serializes its own payload and maintains a separate L1 in each application process.
A remote update or removal can leave another process's local copy readable until its `LocalCacheExpiration` elapses.
Choose that duration for the application's staleness tolerance. HybridCache's stampede protection applies within an application instance.
HybridCache can also cache tag invalidation timestamps independently of L1 entries.
After `RemoveByTagAsync`, an existing process that already cached the tag timestamp can continue serving the previous value until the shared entry expires.

Keys must fit within 1,024 UTF-16 code units. A distributed cache value can contain up to 512 MiB without replication or 32 MiB with replication.
HybridCache adds its own envelope and tag metadata. Replicated sliding renewal records include the updated entry and replication metadata.
Get responses and removed values are not retained in the mutation-outcome budget. Renewal still writes and replicates the entry;
large values and high request rates can reach server admission limits.

Run the [two-process HybridCache example](../samples/hybrid-cache/README.md) to see a fresh application reuse an existing L2 value.
