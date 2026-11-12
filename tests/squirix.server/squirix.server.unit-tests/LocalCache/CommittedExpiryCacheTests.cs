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
        var (cache, _) = CreateExpired(cancellationToken);

        _ = await Assert.That(cache.TryAdd(Key, new NodeCacheEntry<string>("new"), cancellationToken)).IsFalse();
        _ = await Assert.That(cache.GetValue(Key, cancellationToken).Value).IsEqualTo("old");
    }

    /// <summary>Enumeration for snapshots yields the expired entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnumerationKeepsExpiredEntry(CancellationToken cancellationToken)
    {
        var (cache, deadline) = CreateExpired(cancellationToken);

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
        var (cache, deadline) = CreateExpired(cancellationToken);

        var entry = cache.GetEntry(Key, cancellationToken);
        var value = cache.GetValue(Key, cancellationToken);
        ILocalCacheReadOperations<string> reads = cache;
        var withDeadline = reads.GetValueWithDeadline(Key, cancellationToken);

        _ = await Assert.That(entry?.ExpiresUtc).IsEqualTo(deadline);
        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(withDeadline).IsEqualTo(new NodeCacheValueRead<string>(true, "old", deadline));
        ILocalCacheStats stats = cache;
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);
    }

    /// <summary>Removing the expired entry reports it removed, with its value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveReportsExpiredEntry(CancellationToken cancellationToken)
    {
        var (cache, _) = CreateExpired(cancellationToken);

        var removed = cache.Remove(Key, cancellationToken);

        _ = await Assert.That(removed.Removed).IsTrue();
        _ = await Assert.That(removed.Value).IsEqualTo("old");
        _ = await Assert.That(cache.GetEntry(Key, cancellationToken)).IsNull();
    }

    private static (PhysicalCache<string> Cache, DateTime Deadline) CreateExpired(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(clock, new CacheEntrySizeEstimator<string>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var deadline = clock.GetUtcNow().UtcDateTime.AddSeconds(1);
        cache.Set(Key, new NodeCacheEntry<string>("old", 1, deadline), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(2));
        return (cache, deadline);
    }
}
