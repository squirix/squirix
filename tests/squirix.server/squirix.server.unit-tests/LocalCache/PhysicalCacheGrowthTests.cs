using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Checks lookup, removal, eviction and snapshots while buckets are partially transferred.</summary>
[Immutable]
public sealed class PhysicalCacheGrowthTests : ServerUnitTestBase
{
    /// <summary>Both generations retain unique, recoverable entries during long chains and long runs of empty buckets.</summary>
    /// <param name="bucket">The target bucket in a 64-bucket index.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(63)]
    public async Task SnapshotPreservesBothGenerations(int bucket, CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var keys = CollidingKeys(bucket);
        for (var i = 0; i < 49; i++)
            cache.Set(keys[i], new NodeCacheEntry<string>(keys[i].Key, i + 1), cancellationToken);

        _ = await Assert.That(cache.TryAdd(keys[0], new NodeCacheEntry<string>("duplicate"), cancellationToken)).IsFalse();
        _ = await Assert.That(cache.Remove(keys[4], cancellationToken)).IsTrue();
        _ = await Assert.That(cache.Remove(keys[48], cancellationToken)).IsTrue();
        _ = await Assert.That(cache.TryAdd(keys[4], new NodeCacheEntry<string>("reinserted", 99), cancellationToken)).IsTrue();
        var restored = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var seen = new HashSet<CacheKey>();
        await foreach (var (key, entry) in cache.EnumerateLiveAsync(cancellationToken))
        {
            _ = await Assert.That(seen.Add(key)).IsTrue();
            await restored.InsertRecoveryAsync(key, entry, cancellationToken);
        }

        _ = await Assert.That(seen.Count).IsEqualTo(48);
        for (var i = 0; i < 48; i++)
        {
            var entry = restored.GetEntry(keys[i], cancellationToken);
            _ = await Assert.That(entry?.Value).IsEqualTo(i == 4 ? "reinserted" : keys[i].Key);
            _ = await Assert.That(entry?.Version).IsEqualTo(i == 4 ? 99L : i + 1L);
        }

        _ = await Assert.That(restored.GetValue(keys[48], cancellationToken).Found).IsFalse();
        ILocalCacheStats before = cache;
        ILocalCacheStats after = restored;
        _ = await Assert.That(before.EstimatedBytes).IsEqualTo(after.EstimatedBytes);
    }

    /// <summary>Eviction unlinks the original node even when its bucket is still in the previous generation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrowthKeepsFifoVictimOrder(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>(), new EvictionOptions { Capacity = 49, Policy = EvictionPolicyType.Fifo });
        var keys = CollidingKeys(63);
        foreach (var key in keys)
            cache.Set(key, new NodeCacheEntry<string>(key.Key), cancellationToken);

        ILocalCacheStats stats = cache;
        _ = await Assert.That(stats.EntryCount).IsEqualTo(49);
        for (var i = 0; i < keys.Length; i++)
            _ = await Assert.That(cache.GetValue(keys[i], cancellationToken).Found).IsEqualTo(i >= keys.Length - 49);
    }

    /// <summary>Renewing one colliding key preserves it while the deadline index removes due nodes in either table.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RenewalAndExpiryDuringGrowth(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var cache = new PhysicalCache<string>(clock, new CacheEntrySizeEstimator<string>());
        var keys = CollidingKeys(63);
        for (var i = 0; i < 49; i++)
        {
            var ttl = i is 0 or 1 or 48 ? TimeSpan.FromSeconds(5) : TimeSpan.FromDays(1);
            cache.Set(keys[i], new NodeCacheEntry<string>(keys[i].Key, 1, null, ttl), cancellationToken);
        }

        _ = await Assert.That(cache.Touch(keys[1], TimeSpan.FromMinutes(1), cancellationToken)).IsTrue();
        clock.Advance(TimeSpan.FromSeconds(5));
        _ = await Assert.That(cache.ExpiredKeys.RemoveExpired()).IsEqualTo(2);
        _ = await Assert.That(cache.GetValue(keys[0], cancellationToken).Found).IsFalse();
        _ = await Assert.That(cache.GetValue(keys[48], cancellationToken).Found).IsFalse();
        _ = await Assert.That(cache.GetValue(keys[1], cancellationToken).Value).IsEqualTo(keys[1].Key);
    }

    /// <summary>The insertion that crosses a growth threshold allocates only the first pages it needs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GrowthAllocatesBucketsOnDemand(CancellationToken cancellationToken)
    {
        const int threshold = 49_152;
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var value = new NodeCacheEntry<string>("value");
        for (var i = 0; i < threshold; i++)
            cache.Set(CacheKey.Default(i.ToString(CultureInfo.InvariantCulture)), value, cancellationToken);

        var key = CacheKey.Default("trigger");
        var allocated = GC.GetAllocatedBytesForCurrentThread();
        cache.Set(key, value, cancellationToken);
        allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;

        // An array of 131,072 references alone would consume at least 512 KiB.
        _ = await Assert.That(allocated).IsLessThan(256 * 1024L);
        _ = await Assert.That(cache.GetValue(key, cancellationToken).Value).IsEqualTo("value");
    }

    private static CacheKey[] CollidingKeys(int bucket)
    {
        var keys = new CacheKey[52];
        var count = 0;
        var candidate = 0;
        while (count < keys.Length)
        {
            var key = CacheKey.Default((candidate++).ToString(CultureInfo.InvariantCulture));
            if (unchecked(key.GetHashCode() * -0x61C88647) >>> 26 == bucket)
                keys[count++] = key;
        }

        return keys;
    }
}
