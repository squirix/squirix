using System;
using System.Collections.Generic;
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

/// <summary>
/// Under <see cref="CacheExpiryAuthority.CommittedRecords" /> the cache lists the keys past their deadline from a deadline index, earliest
/// first, without walking the stored entries; a key stays listed until a committed record removes it or its deadline moves (#1031).
/// </summary>
[Immutable]
public sealed class ExpiredKeyListingTests : ServerUnitTestBase
{
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Only the keys past their deadline are listed, earliest deadline first, at most the number asked for.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ListsDueKeysEarliestFirst(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        Set(cache, "late", TimeSpan.FromSeconds(3), cancellationToken);
        Set(cache, "early", TimeSpan.FromSeconds(1), cancellationToken);
        Set(cache, "live", TimeSpan.FromHours(1), cancellationToken);
        cache.Set(CacheKey.Default("forever"), new NodeCacheEntry<object?>("v"), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(5));

        var all = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true);
        var first = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 1, static _ => true);

        _ = await Assert.That(string.Join(',', Keys(all))).IsEqualTo("early,late");
        _ = await Assert.That(string.Join(',', Keys(first))).IsEqualTo("early");
    }

    /// <summary>
    /// A listed key stays due, and a key the filter skips stays due too, behind the keys due before it was skipped: only a committed record
    /// removes an expired entry.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ListingLeavesKeysDue(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        Set(cache, "a", TimeSpan.FromSeconds(1), cancellationToken);
        Set(cache, "b", TimeSpan.FromSeconds(2), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(5));
        var now = clock.GetUtcNow().UtcDateTime;

        var skipped = cache.ExpiredKeys.ListExpired(now, 10, static key => !string.Equals(key.Key, "a", StringComparison.Ordinal));
        var again = cache.ExpiredKeys.ListExpired(now, 10, static _ => true);

        _ = await Assert.That(string.Join(',', Keys(skipped))).IsEqualTo("b");
        _ = await Assert.That(string.Join(',', Keys(again))).IsEqualTo("b,a");
    }

    /// <summary>
    /// Keys the caller keeps skipping, such as those of a group whose leader is away, cannot hold back a key it can expire: a call examines
    /// at most four due keys per key asked for, and a skipped key goes behind the keys due before that call.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SkippedKeysDoNotStarveOthers(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        for (var i = 1; i <= 10; i++)
            Set(cache, "skipped" + i.ToString(CultureInfo.InvariantCulture), TimeSpan.FromSeconds(i), cancellationToken);

        Set(cache, "wanted", TimeSpan.FromSeconds(11), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(20));

        var calls = 0;
        IReadOnlyList<CacheKey> listed;
        do
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            listed = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 1, static key => string.Equals(key.Key, "wanted", StringComparison.Ordinal));
            calls++;
        }
        while (listed.Count == 0 && calls < 10);

        _ = await Assert.That(string.Join(',', Keys(listed))).IsEqualTo("wanted");
        _ = await Assert.That(calls).IsEqualTo(3);
    }

    /// <summary>A key whose deadline moved, lost its deadline, or left the cache is not listed under its old deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChangedEntriesAreNotListed(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.CommittedRecords);
        foreach (var key in new[] { "touched", "persisted", "removed", "rewritten", "kept" })
            Set(cache, key, TimeSpan.FromSeconds(1), cancellationToken);

        _ = cache.Touch(CacheKey.Default("touched"), TimeSpan.FromHours(1), cancellationToken);
        _ = cache.RemoveExpiration(CacheKey.Default("persisted"), cancellationToken);
        _ = cache.Remove(CacheKey.Default("removed"), cancellationToken);
        Set(cache, "rewritten", TimeSpan.FromHours(1), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(5));

        var listed = cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true);

        _ = await Assert.That(string.Join(',', Keys(listed))).IsEqualTo("kept");
    }

    /// <summary>A cache whose local clock decides expiry keeps no expired entry, so it lists none.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LocalClockCacheListsNone(CancellationToken cancellationToken)
    {
        var (cache, clock) = Create(CacheExpiryAuthority.LocalClock);
        Set(cache, "a", TimeSpan.FromSeconds(1), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(5));

        _ = await Assert.That(cache.ExpiredKeys.ListExpired(clock.GetUtcNow().UtcDateTime, 10, static _ => true).Count).IsEqualTo(0);
    }

    private static (PhysicalCache<object?> Cache, FakeTimeProvider Clock) Create(CacheExpiryAuthority expiry)
    {
        var clock = new FakeTimeProvider(Start);
        return (new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: expiry), clock);
    }

    private static void Set(PhysicalCache<object?> cache, string key, TimeSpan ttl, CancellationToken cancellationToken) =>
        cache.Set(CacheKey.Default(key), new NodeCacheEntry<object?>("v", 1, null, ttl), cancellationToken);

    private static string[] Keys(IReadOnlyList<CacheKey> keys)
    {
        var names = new string[keys.Count];
        for (var i = 0; i < names.Length; i++)
            names[i] = keys[i].Key;

        return names;
    }
}
