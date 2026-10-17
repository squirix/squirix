using System;
using System.Globalization;
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

/// <summary>Under <see cref="CacheExpiryAuthority.LocalClock" /> the cache removes the entries past their deadline that nobody reads, and nothing else.</summary>
[Immutable]
public sealed class ExpiredEntryRemovalTests : ServerUnitTestBase
{
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Expired entries leave memory with their bytes, and an entry without a deadline stays.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemovesExpiredEntriesNobodyReads(CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<object?>();
        var (cache, clock) = Create(CacheExpiryAuthority.LocalClock, estimator);
        foreach (var key in new[] { "a", "b", "c" })
            await SetAsync(cache, key, TimeSpan.FromSeconds(5), cancellationToken);

        await cache.SetAsync(CacheKey.Default("forever"), new NodeCacheEntry<object?>("v"), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(5));

        var removed = cache.ExpiredKeys.RemoveExpired();

        ILocalCacheStats stats = cache;
        var liveBytes = estimator.EstimateBytes(CacheKey.Default("forever"), new NodeCacheEntry<object?>("v"), false);
        _ = await Assert.That(removed).IsEqualTo(3);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(liveBytes);
    }

    /// <summary>An entry whose deadline moved later, by an overwrite or a touch, stays until its new deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KeepsEntriesWhoseDeadlineMovedLater(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.LocalClock);
        await SetAsync(cache, "overwritten", TimeSpan.FromSeconds(5), cancellationToken);
        await SetAsync(cache, "touched", TimeSpan.FromSeconds(5), cancellationToken);
        await SetAsync(cache, "overwritten", TimeSpan.FromSeconds(60), cancellationToken);
        _ = await cache.TouchAsync(CacheKey.Default("touched"), TimeSpan.FromSeconds(60), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));

        var early = cache.ExpiredKeys.RemoveExpired();
        clock.Advance(TimeSpan.FromSeconds(60));
        var late = cache.ExpiredKeys.RemoveExpired();

        ILocalCacheStats stats = cache;
        _ = await Assert.That((early, late, stats.EntryCount)).IsEqualTo((0, 2, 0));
    }

    /// <summary>An entry whose deadline moved earlier, or that got a deadline after having none, is removed at the new deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemovesEntriesWhoseDeadlineMovedEarlier(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.LocalClock);
        await SetAsync(cache, "shortened", TimeSpan.FromHours(1), cancellationToken);
        await SetAsync(cache, "shortened", TimeSpan.FromSeconds(5), cancellationToken);
        await cache.SetAsync(CacheKey.Default("bounded"), new NodeCacheEntry<object?>("v"), cancellationToken);
        _ = await cache.TouchAsync(CacheKey.Default("bounded"), TimeSpan.FromSeconds(5), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));

        var removed = cache.ExpiredKeys.RemoveExpired();

        ILocalCacheStats stats = cache;
        _ = await Assert.That((removed, stats.EntryCount)).IsEqualTo((2, 0));
    }

    /// <summary>An entry that lost its deadline stays, and so does one that was removed and stored again without one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KeepsEntriesWithoutDeadline(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.LocalClock);
        await SetAsync(cache, "persisted", TimeSpan.FromSeconds(5), cancellationToken);
        await SetAsync(cache, "replaced", TimeSpan.FromSeconds(5), cancellationToken);
        _ = await cache.RemoveExpirationAsync(CacheKey.Default("persisted"), cancellationToken);
        _ = await cache.RemoveAsync(CacheKey.Default("replaced"), cancellationToken);
        await cache.SetAsync(CacheKey.Default("replaced"), new NodeCacheEntry<object?>("v"), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));

        var removed = cache.ExpiredKeys.RemoveExpired();

        ILocalCacheStats stats = cache;
        _ = await Assert.That((removed, stats.EntryCount)).IsEqualTo((0, 2));
    }

    /// <summary>More expired entries than one batch are all removed by one call.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainsPastOneBatch(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.LocalClock);
        for (var i = 0; i < 2049; i++)
            await SetAsync(cache, "k" + i.ToString(CultureInfo.InvariantCulture), TimeSpan.FromSeconds(5), cancellationToken);

        clock.Advance(TimeSpan.FromSeconds(5));

        var removed = cache.ExpiredKeys.RemoveExpired();

        ILocalCacheStats stats = cache;
        _ = await Assert.That((removed, stats.EntryCount, stats.EstimatedBytes)).IsEqualTo((2049, 0, 0L));
    }

    /// <summary>Under committed records only a committed record removes an entry, so the call removes nothing and the keys stay listed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemovesNothingUnderCommittedRecords(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        await SetAsync(cache, "a", TimeSpan.FromSeconds(5), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));

        var removed = cache.ExpiredKeys.RemoveExpired();

        ILocalCacheStats stats = cache;
        var listed = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true);
        _ = await Assert.That((removed, stats.EntryCount, listed.Count)).IsEqualTo((0, 1, 1));
    }

    /// <summary>Under committed records an entry whose deadline moved later is listed at its new deadline, not at the old one, and only once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ListsExtendedEntryAtNewDeadline(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        await SetAsync(cache, "a", TimeSpan.FromSeconds(5), cancellationToken);
        await SetAsync(cache, "a", TimeSpan.FromSeconds(60), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));
        var early = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true).Count;
        clock.Advance(TimeSpan.FromSeconds(60));

        var late = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true);

        _ = await Assert.That((early, late.Count)).IsEqualTo((0, 1));
    }

    /// <summary>Under committed records an entry whose deadline toggled or moved earlier then later is listed once per pass, at its current deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ListsToggledDeadlineOnce(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        await SetAsync(cache, "toggled", TimeSpan.FromSeconds(10), cancellationToken);
        await cache.SetAsync(CacheKey.Default("toggled"), new NodeCacheEntry<object?>("v"), cancellationToken);
        await SetAsync(cache, "toggled", TimeSpan.FromSeconds(60), cancellationToken);
        await SetAsync(cache, "moved", TimeSpan.FromSeconds(10), cancellationToken);
        await SetAsync(cache, "moved", TimeSpan.FromSeconds(5), cancellationToken);
        await SetAsync(cache, "moved", TimeSpan.FromSeconds(60), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(70));

        var first = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true);
        var second = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true);

        _ = await Assert.That((first.Count, second.Count)).IsEqualTo((2, 2));
    }

    /// <summary>After many deadline changes force the index to be rebuilt, both authorities find exactly the entries due at their current deadlines.</summary>
    /// <param name="localClock">Whether the local clock decides expiry, otherwise committed records do.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task FindsDueEntriesAfterIndexRebuild(bool localClock, CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(localClock ? CacheExpiryAuthority.LocalClock : CacheExpiryAuthority.CommittedRecords);
        await SetAsync(cache, "due", TimeSpan.FromSeconds(30), cancellationToken);
        await SetAsync(cache, "later", TimeSpan.FromHours(1), cancellationToken);
        for (var i = 1; i <= 3000; i++)
            await SetAsync(cache, "churn", TimeSpan.FromSeconds(3000 - i + 1), cancellationToken);

        await SetAsync(cache, "churn", TimeSpan.FromSeconds(10), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(40));

        if (localClock)
        {
            ILocalCacheStats stats = cache;
            _ = await Assert.That((cache.ExpiredKeys.RemoveExpired(), stats.EntryCount)).IsEqualTo((2, 1));
            return;
        }

        var listed = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true);
        var names = new string[listed.Count];
        for (var i = 0; i < names.Length; i++)
            names[i] = listed[i].Key;

        Array.Sort(names, StringComparer.Ordinal);
        _ = await Assert.That(string.Join(',', names)).IsEqualTo("churn,due");
    }

    /// <summary>A key the caller skipped whose deadline then moves later is not listed before the new deadline, and is listed once after it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SkippedKeyWithLaterDeadlineListedOnce(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        await SetAsync(cache, "a", TimeSpan.FromSeconds(5), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));
        var skipped = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => false).Count;
        await SetAsync(cache, "a", TimeSpan.FromSeconds(60), cancellationToken);
        var before = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true).Count;
        clock.Advance(TimeSpan.FromSeconds(60));

        var after = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true).Count;

        _ = await Assert.That((skipped, before, after)).IsEqualTo((0, 0, 1));
    }

    private static (PhysicalCache<object?> Cache, FakeTimeProvider Clock) Create(CacheExpiryAuthority expiry, CacheEntrySizeEstimator<object?>? estimator = null)
    {
        var clock = new FakeTimeProvider(Start);
        return (new PhysicalCache<object?>(clock, estimator ?? new CacheEntrySizeEstimator<object?>(), expiry: expiry), clock);
    }

    private static ValueTask SetAsync(PhysicalCache<object?> cache, string key, TimeSpan ttl, CancellationToken cancellationToken) =>
        cache.SetAsync(CacheKey.Default(key), new NodeCacheEntry<object?>("v", 1, null, ttl), cancellationToken);
}
