using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Unit tests for derived cache mutations on the server local cache surface.</summary>
[Immutable]
public sealed class CacheDerivedMutationTests : ServerUnitTestBase
{
    /// <summary>Ensures ClientCache SetEntryAsync without expiration clears a previous TTL and tags instead of carrying them over.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientSetWithoutExpiryClearsTtl(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var physical = new PhysicalCache<string>(timeProvider);
        var clientCache = new ClientCache<string>(physical, physical);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
        var expiring = new NodeCacheEntry<string>("old", expiration: TimeSpan.FromMinutes(10), tags: tags);
        await clientCache.SetEntryAsync(UnitMutationOpIds.Default, "orders", "k", expiring, cancellationToken);

        await clientCache.SetEntryAsync(UnitMutationOpIds.Default, "orders", "k", new NodeCacheEntry<string> { Value = "new" }, cancellationToken);
        timeProvider.Advance(TimeSpan.FromMinutes(11));

        var entry = await clientCache.GetEntryAsync("orders", "k", cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("new");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
        _ = await Assert.That(entry.Tags).IsNull();
    }

    /// <summary>Ensures ClientCache UpdateAsync preserves expiration through the adapter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientUpdateAsyncPreservesExpiry(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var physical = new PhysicalCache<string>(timeProvider);
        var clientCache = new ClientCache<string>(physical, physical);
        var expires = timeProvider.GetUtcNow().UtcDateTime.AddMinutes(10);
        await clientCache.SetEntryAsync(UnitMutationOpIds.Default, "orders", "k", new NodeCacheEntry<string> { Value = "old", ExpiresUtc = expires }, cancellationToken);

        var updated = await clientCache.UpdateAsync(UnitMutationOpIds.Default, "orders", "k", "new", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
        var entry = await clientCache.GetEntryAsync("orders", "k", cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.Value).IsEqualTo("new");
        _ = await Assert.That(entry.ExpiresUtc).IsEqualTo(expires);
    }

    /// <summary>Ensures UpdateAsync returns false for missing keys.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateAsyncReturnsFalseForMissingKey(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var updated = await cache.UpdateAsync(CacheKey.Default("missing"), "new", cancellationToken);
        _ = await Assert.That(updated).IsFalse();
    }

    /// <summary>Ensures UpdateAsync changes the value while preserving expiration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateKeepsExpiryOnPhysicalCacheAsync(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);
        var expires = timeProvider.GetUtcNow().UtcDateTime.AddMinutes(5);
        await cache.SetAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "old", ExpiresUtc = expires }, cancellationToken);

        var updated = await cache.UpdateAsync(CacheKey.Default("k"), "new", cancellationToken);

        _ = await Assert.That(updated).IsTrue();
        var entry = await cache.GetEntryAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.Value).IsEqualTo("new");
        _ = await Assert.That(entry.ExpiresUtc).IsEqualTo(expires);
    }
}
