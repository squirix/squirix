using System;
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

/// <summary>
/// Under <see cref="CacheExpiryAuthority.CommittedRecords" /> the cache never decides expiry on its own clock: an entry past its deadline
/// stays present for every operation until a committed record removes it.
/// </summary>
[Immutable]
public sealed class CommittedExpiryCacheTests : ServerUnitTestBase
{
    private static readonly CacheKey Key = CacheKey.Default("k");

    /// <summary>A conditional add finds the expired entry present and leaves it in place.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddKeepsExpiredEntry(CancellationToken cancellationToken)
    {
        var (cache, _) = await CreateExpiredAsync(cancellationToken);

        _ = await Assert.That(await cache.TryAddAsync(Key, new NodeCacheEntry<string>("new"), cancellationToken)).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync(Key, cancellationToken)).Value).IsEqualTo("old");
    }

    /// <summary>Enumeration for snapshots yields the expired entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnumerationKeepsExpiredEntry(CancellationToken cancellationToken)
    {
        var (cache, deadline) = await CreateExpiredAsync(cancellationToken);

        var entries = new List<(CacheKey Key, NodeCacheEntry<string> Entry)>();
        await foreach (var item in cache.EnumerateLiveAsync(cancellationToken))
            entries.Add(item);

        var (key, entry) = await Assert.That(entries).HasSingleItem();
        _ = await Assert.That(key).IsEqualTo(Key);
        _ = await Assert.That(entry.ExpiresUtc).IsEqualTo(deadline);
    }

    /// <summary>A read of the expired entry returns it and does not drop it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredEntryStaysReadable(CancellationToken cancellationToken)
    {
        var (cache, deadline) = await CreateExpiredAsync(cancellationToken);

        var entry = await cache.GetEntryAsync(Key, cancellationToken);
        var value = await cache.GetValueAsync(Key, cancellationToken);

        _ = await Assert.That(entry?.ExpiresUtc).IsEqualTo(deadline);
        _ = await Assert.That(value.Found).IsTrue();
        ILocalCacheStats stats = cache;
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);
    }

    /// <summary>Removing the expired entry reports it removed, with its value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveReportsExpiredEntry(CancellationToken cancellationToken)
    {
        var (cache, _) = await CreateExpiredAsync(cancellationToken);

        var removed = await cache.RemoveAsync(Key, cancellationToken);

        _ = await Assert.That(removed.Removed).IsTrue();
        _ = await Assert.That(removed.Value).IsEqualTo("old");
        _ = await Assert.That(await cache.GetEntryAsync(Key, cancellationToken)).IsNull();
    }

    private static async Task<(PhysicalCache<string> Cache, DateTime Deadline)> CreateExpiredAsync(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
        var deadline = clock.GetUtcNow().UtcDateTime.AddSeconds(1);
        await cache.SetAsync(Key, new NodeCacheEntry<string>("old", 1, deadline), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(2));
        return (cache, deadline);
    }
}
