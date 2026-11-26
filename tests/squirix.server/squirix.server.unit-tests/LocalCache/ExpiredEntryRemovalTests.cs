using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Under <see cref="CacheExpiryAuthority.LocalClock" /> the cache removes the entries past their deadline that nobody reads, and nothing else.</summary>
[Immutable]
public sealed class ExpiredEntryRemovalTests : ServerUnitTestBase
{
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>A failed expiry check leaves the slot, index and memory accounting intact for a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemovalCanRetryAfterClockFailure(CancellationToken cancellationToken)
    {
        var clock = new FaultingClock();
        var cache = new PhysicalCache<string>(clock, new CacheEntrySizeEstimator<string>());
        var key = CacheKey.Default("retry");
        cache.Set(key, new NodeCacheEntry<string>("original", 1, Start.AddMinutes(1).UtcDateTime), cancellationToken);
        ILocalCacheStats stats = cache;
        var bytes = stats.EstimatedBytes;
        clock.Fail = true;
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(cache, static c => _ = c.Remove(CacheKey.Default("retry"), CancellationToken.None));
        clock.Fail = false;

        _ = await Assert.That((stats.EntryCount, stats.EstimatedBytes)).IsEqualTo((1, bytes));
        _ = await Assert.That(cache.GetValue(key, cancellationToken).Value).IsEqualTo("original");
        _ = await Assert.That(cache.Remove(key, cancellationToken)).IsTrue();
        _ = await Assert.That((stats.EntryCount, stats.EstimatedBytes)).IsEqualTo((0, 0L));
    }

    /// <summary>Expired entries leave memory with their bytes, and an entry without a deadline stays.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemovesExpiredEntriesNobodyReads(CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<object?>();
        var (cache, clock) = Create(CacheExpiryAuthority.LocalClock, estimator);
        foreach (var key in new[] { "a", "b", "c" })
            Set(cache, key, TimeSpan.FromSeconds(5), cancellationToken);

        cache.Set(CacheKey.Default("forever"), new NodeCacheEntry<object?>("v"), cancellationToken);
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
        Set(cache, "overwritten", TimeSpan.FromSeconds(5), cancellationToken);
        Set(cache, "touched", TimeSpan.FromSeconds(5), cancellationToken);
        Set(cache, "overwritten", TimeSpan.FromSeconds(60), cancellationToken);
        _ = cache.Touch(CacheKey.Default("touched"), TimeSpan.FromSeconds(60), cancellationToken);
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
        Set(cache, "shortened", TimeSpan.FromHours(1), cancellationToken);
        Set(cache, "shortened", TimeSpan.FromSeconds(5), cancellationToken);
        cache.Set(CacheKey.Default("bounded"), new NodeCacheEntry<object?>("v"), cancellationToken);
        _ = cache.Touch(CacheKey.Default("bounded"), TimeSpan.FromSeconds(5), cancellationToken);
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
        Set(cache, "persisted", TimeSpan.FromSeconds(5), cancellationToken);
        Set(cache, "replaced", TimeSpan.FromSeconds(5), cancellationToken);
        _ = cache.RemoveExpiration(CacheKey.Default("persisted"), cancellationToken);
        _ = cache.Remove(CacheKey.Default("replaced"), cancellationToken);
        cache.Set(CacheKey.Default("replaced"), new NodeCacheEntry<object?>("v"), cancellationToken);
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
            Set(cache, "k" + i.ToString(CultureInfo.InvariantCulture), TimeSpan.FromSeconds(5), cancellationToken);

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
        Set(cache, "a", TimeSpan.FromSeconds(5), cancellationToken);
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
        Set(cache, "a", TimeSpan.FromSeconds(5), cancellationToken);
        Set(cache, "a", TimeSpan.FromSeconds(60), cancellationToken);
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
        Set(cache, "toggled", TimeSpan.FromSeconds(10), cancellationToken);
        cache.Set(CacheKey.Default("toggled"), new NodeCacheEntry<object?>("v"), cancellationToken);
        Set(cache, "toggled", TimeSpan.FromSeconds(60), cancellationToken);
        Set(cache, "moved", TimeSpan.FromSeconds(10), cancellationToken);
        Set(cache, "moved", TimeSpan.FromSeconds(5), cancellationToken);
        Set(cache, "moved", TimeSpan.FromSeconds(60), cancellationToken);
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
        Set(cache, "due", TimeSpan.FromSeconds(30), cancellationToken);
        Set(cache, "later", TimeSpan.FromHours(1), cancellationToken);
        cache.Set(CacheKey.Default("permanent"), new NodeCacheEntry<object?>("v"), cancellationToken);
        for (var i = 1; i <= 3000; i++)
            Set(cache, "churn", TimeSpan.FromSeconds(3000 - i + 1), cancellationToken);

        Set(cache, "churn", TimeSpan.FromSeconds(10), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(40));

        if (localClock)
        {
            ILocalCacheStats stats = cache;
            _ = await Assert.That((cache.ExpiredKeys.RemoveExpired(), stats.EntryCount)).IsEqualTo((2, 2));
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
        Set(cache, "a", TimeSpan.FromSeconds(5), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));
        var skipped = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => false).Count;
        Set(cache, "a", TimeSpan.FromSeconds(60), cancellationToken);
        var before = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true).Count;
        clock.Advance(TimeSpan.FromSeconds(60));

        var after = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true).Count;

        _ = await Assert.That((skipped, before, after)).IsEqualTo((0, 0, 1));
    }

    /// <summary>A deadline held for an old occupant cannot expire or list its replacement twice.</summary>
    /// <param name="sameKey">Whether the replacement reuses the same key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OldDeadlineCannotClaimReusedSlot(bool sameKey, CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        var replacement = sameKey ? "old" : "new";
        var ttl = TimeSpan.FromSeconds(10);
        Set(cache, "old", ttl, cancellationToken);
        _ = cache.Remove(CacheKey.Default("old"), cancellationToken);
        Set(cache, replacement, ttl, cancellationToken);
        clock.Advance(ttl);
        var due = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true);
        _ = await Assert.That(due.Count).IsEqualTo(1);
        _ = await Assert.That(due[0]).IsEqualTo(CacheKey.Default(replacement));

        var (local, localClock) = Create(CacheExpiryAuthority.LocalClock);
        Set(local, "old", ttl, cancellationToken);
        _ = local.Remove(CacheKey.Default("old"), cancellationToken);
        Set(local, replacement, ttl + ttl, cancellationToken);
        localClock.Advance(ttl);
        _ = await Assert.That(local.ExpiredKeys.RemoveExpired()).IsEqualTo(0);
        _ = await Assert.That(local.GetValue(CacheKey.Default(replacement), cancellationToken).Found).IsTrue();
        localClock.Advance(ttl);
        _ = await Assert.That(local.ExpiredKeys.RemoveExpired()).IsEqualTo(1);
    }

    /// <summary>A replacement that acquires a TTL must not inherit the removed occupant's deferred schedule.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReusedSlotSchedulesItsOwnDeadline(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.LocalClock);
        var oldKey = CacheKey.Default("old");
        Set(cache, oldKey.Key, TimeSpan.FromSeconds(1), cancellationToken);
        _ = await Assert.That(cache.Touch(oldKey, TimeSpan.FromHours(1), cancellationToken)).IsTrue();
        _ = await Assert.That(cache.Remove(oldKey, cancellationToken)).IsTrue();

        var replacement = CacheKey.Default("new");
        cache.Set(replacement, new NodeCacheEntry<object?>("value"), cancellationToken);
        _ = await Assert.That(cache.Touch(replacement, TimeSpan.FromSeconds(5), cancellationToken)).IsTrue();
        clock.Advance(TimeSpan.FromSeconds(1));
        _ = await Assert.That(cache.ExpiredKeys.RemoveExpired()).IsEqualTo(0);
        clock.Advance(TimeSpan.FromSeconds(4));
        _ = await Assert.That(cache.ExpiredKeys.RemoveExpired()).IsEqualTo(1);
    }

    private static (PhysicalCache<object?> Cache, FakeTimeProvider Clock) Create(CacheExpiryAuthority expiry, CacheEntrySizeEstimator<object?>? estimator = null)
    {
        var clock = new FakeTimeProvider(Start);
        return (new PhysicalCache<object?>(clock, estimator ?? new CacheEntrySizeEstimator<object?>(), expiry: expiry), clock);
    }

    private static void Set(PhysicalCache<object?> cache, string key, TimeSpan ttl, CancellationToken cancellationToken) =>
        cache.Set(CacheKey.Default(key), new NodeCacheEntry<object?>("v", 1, null, ttl), cancellationToken);

    private sealed class FaultingClock : TimeProvider
    {
        internal bool Fail { get; set; }

        public override DateTimeOffset GetUtcNow() => Fail ? throw new InvalidOperationException("Clock failed.") : Start;
    }
}
