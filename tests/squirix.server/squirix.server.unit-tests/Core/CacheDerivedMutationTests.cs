using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Unit tests for derived cache mutations on the server local cache surface.</summary>
[Immutable]
public sealed class CacheDerivedMutationTests : ServerUnitTestBase
{
    /// <summary>Ensures ClientCache UpdateAsync preserves expiration through the adapter.</summary>
    [Fact]
    public async Task ClientUpdateAsyncPreservesExpiry()
    {
        var timeProvider = new FakeTimeProvider();
        await using var physical = new PhysicalCache<string>(timeProvider);
        var clientCache = new ClientCache<string>(physical, physical);
        var expires = timeProvider.GetUtcNow().UtcDateTime.AddMinutes(10);
        await clientCache.SetEntryAsync(UnitMutationOpIds.Default, "orders", "k", new NodeCacheEntry<string> { Value = "old", ExpiresUtc = expires }, DefaultCancellationToken);

        var updated = await clientCache.UpdateAsync(UnitMutationOpIds.Default, "orders", "k", "new", DefaultCancellationToken);

        Assert.True(updated);
        var entry = await clientCache.GetEntryAsync("orders", "k", DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("new", entry.Value);
        Assert.Equal(expires, entry.ExpiresUtc);
    }

    /// <summary>Ensures UpdateAsync changes the value while preserving expiration.</summary>
    [Fact]
    public async Task UpdateKeepsExpiryOnPhysicalCacheAsync()
    {
        var timeProvider = new FakeTimeProvider();
        await using var cache = new PhysicalCache<string>(timeProvider);
        var expires = timeProvider.GetUtcNow().UtcDateTime.AddMinutes(5);
        await cache.SetAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "old", ExpiresUtc = expires }, DefaultCancellationToken);

        var updated = await cache.UpdateAsync(CacheKey.Default("k"), "new", DefaultCancellationToken);

        Assert.True(updated);
        var entry = await cache.GetEntryAsync(CacheKey.Default("k"), DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("new", entry.Value);
        Assert.Equal(expires, entry.ExpiresUtc);
    }

    /// <summary>Ensures UpdateAsync returns false for missing keys.</summary>
    [Fact]
    public async Task UpdateAsyncReturnsFalseForMissingKey()
    {
        await using var cache = new PhysicalCache<string>();
        var updated = await cache.UpdateAsync(CacheKey.Default("missing"), "new", DefaultCancellationToken);
        Assert.False(updated);
    }
}
