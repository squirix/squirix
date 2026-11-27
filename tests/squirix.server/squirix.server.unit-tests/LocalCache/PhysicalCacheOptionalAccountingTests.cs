using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Verifies the physical cache can omit accounting and that the enabled total follows concurrent mutations.</summary>
[Immutable]
public sealed class PhysicalCacheOptionalAccountingTests : ServerUnitTestBase
{
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Disabled accounting preserves relative expiry, retained replica entries, and recovery mutations.</summary>
    /// <param name="committedExpiry">Whether only committed removals decide expiry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ExpiryAndRecoveryWithoutAccounting(bool committedExpiry, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var expiry = committedExpiry ? CacheExpiryAuthority.CommittedRecords : CacheExpiryAuthority.LocalClock;
        var cache = new PhysicalCache<string>(clock, null, expiry: expiry);
        ILocalCacheStats stats = cache;
        var key = new CacheKey("ns", "key");
        var ttl = TimeSpan.FromMinutes(2);
        var entry = new NodeCacheEntry<string>("first", 1, Start.UtcDateTime.AddMinutes(5), ttl);

        _ = await Assert.That(cache.TryAdd(key, entry, cancellationToken)).IsTrue();
        var added = cache.GetEntry(key, cancellationToken);
        _ = await Assert.That(added?.Version).IsEqualTo(1L);
        _ = await Assert.That(added?.ExpiresUtc).IsEqualTo(Start.UtcDateTime + ttl);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(0L);

        _ = await Assert.That(cache.RemoveExpiration(key, cancellationToken)).IsTrue();
        _ = await Assert.That(cache.Update(key, "updated", cancellationToken)).IsTrue();
        _ = await Assert.That(cache.GetEntry(key, cancellationToken)?.Value).IsEqualTo("updated");
        _ = await Assert.That(cache.Touch(key, ttl, cancellationToken)).IsTrue();
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(0L);

        clock.Advance(ttl);
        _ = await Assert.That(cache.GetValue(key, cancellationToken).Found).IsEqualTo(committedExpiry);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(committedExpiry ? 1 : 0);

        await cache.InsertRecoveryAsync(key, new NodeCacheEntry<string>("recovered", 7), cancellationToken, 128);
        var recovered = cache.GetEntry(key, cancellationToken);
        _ = await Assert.That(recovered?.Version).IsEqualTo(7L);
        _ = await Assert.That(recovered?.Value).IsEqualTo("recovered");
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(0L);
        _ = await Assert.That(await cache.RemoveRecoveryAsync(key, cancellationToken)).IsTrue();
        _ = await Assert.That(stats.EntryCount).IsEqualTo(0);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(0L);
    }

    /// <summary>Counts the entries left by concurrent writers, renewal, recovery, and count-based eviction in both modes.</summary>
    /// <param name="enabled">Whether the estimates are tracked.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConcurrentMutationsRespectAccounting(bool enabled, CancellationToken cancellationToken)
    {
        const int capacity = 24;
        var clock = new FakeTimeProvider(Start);
        var estimator = new CacheEntrySizeEstimator<string>();
        var eviction = new EvictionOptions { Capacity = capacity, Policy = EvictionPolicyType.Fifo };
        var cache = new PhysicalCache<string>(clock, enabled ? estimator : null, eviction);
        ILocalCacheStats stats = cache;
        var keys = new CacheKey[48];
        for (var i = 0; i < keys.Length; i++)
            keys[i] = new CacheKey("ns", $"key-{i}");

        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = new Task[6];
        for (var i = 0; i < workers.Length; i++)
            workers[i] = MutateAsync(start.Task, cache, keys, i, cancellationToken);

        start.SetResult();
        await Task.WhenAll(workers);

        var chargedEstimator = CacheEntrySizeEstimator<string>.ForEviction(estimator, eviction);
        long expectedBytes = 0;
        var entryCount = 0;
        await foreach (var (key, entry) in cache.EnumerateLiveAsync(cancellationToken))
        {
            expectedBytes += chargedEstimator.EstimateBytes(key, entry, false);
            entryCount++;
        }

        _ = await Assert.That(entryCount).IsBetween(1, capacity);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(entryCount);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(enabled ? expectedBytes : 0L);

        foreach (var key in keys)
            _ = cache.Remove(key, cancellationToken);

        _ = await Assert.That(stats.EntryCount).IsEqualTo(0);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(0L);
    }

    /// <summary>Concurrent inserts cannot spend the same remaining byte budget.</summary>
    /// <param name="tryAdd">Whether to exercise conditional inserts instead of sets.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConcurrentInsertsRespectQuota(bool tryAdd, CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<string>();
        var entry = new NodeCacheEntry<string>("value");
        var entryBytes = estimator.EstimateBytes(new CacheKey("ns", "key-0"), entry, false);
        var cache = new PhysicalCache<string>(new FakeTimeProvider(Start), estimator, criticalMemoryThresholdBytes: entryBytes * 3);
        ILocalCacheStats stats = cache;
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writes = new Task<bool>[8];
        for (var i = 0; i < writes.Length; i++)
            writes[i] = TryInsertAsync(start.Task, cache, new CacheKey("ns", $"key-{i}"), entry, tryAdd, cancellationToken);

        start.SetResult();
        var outcomes = await Task.WhenAll(writes);
        var accepted = Array.FindAll(outcomes, static outcome => outcome).Length;

        _ = await Assert.That(accepted).IsEqualTo(2);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(2);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(entryBytes * 2);
    }

    /// <summary>Deadline and payload growth respect the exact boundary; recovery and shrinking writes can proceed under critical pressure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CriticalAllowsRecoveryAndShrink(CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<string>();
        var key = new CacheKey("ns", "key");
        var initial = new NodeCacheEntry<string>("small");
        var initialBytes = estimator.EstimateBytes(key, initial, false);
        var threshold = initialBytes + CacheEntryDeadlineSize.IndexBytes;
        var cache = new PhysicalCache<string>(new FakeTimeProvider(Start), estimator, criticalMemoryThresholdBytes: threshold);
        ILocalCacheStats stats = cache;
        cache.Set(key, initial, cancellationToken);

        var touch = NodeExceptionAssert.For<ResourceExhaustedException>().Throws(
            (Cache: cache, Key: key, Token: cancellationToken),
            static s => _ = s.Cache.Touch(s.Key, TimeSpan.FromMinutes(1), s.Token));
        _ = await Assert.That(touch.WasCritical).IsFalse();
        _ = await Assert.That(cache.GetEntry(key, cancellationToken)?.ExpiresUtc).IsNull();

        var longerValue = new string('v', int.CreateChecked(threshold));
        var update = NodeExceptionAssert.For<ResourceExhaustedException>().Throws(
            (Cache: cache, Key: key, Token: cancellationToken, Value: longerValue),
            static s => _ = s.Cache.Update(s.Key, s.Value, s.Token));
        _ = await Assert.That(update.WasCritical).IsFalse();
        _ = await Assert.That(cache.GetValue(key, cancellationToken).Value).IsEqualTo("small");
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(initialBytes);

        var recovered = new NodeCacheEntry<string>(longerValue);
        await cache.InsertRecoveryAsync(key, recovered, cancellationToken);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(estimator.EstimateBytes(key, recovered, false));

        var insert = NodeExceptionAssert.For<ResourceExhaustedException>().Throws(
            (Cache: cache, Key: new CacheKey("ns", "other"), Entry: initial, Token: cancellationToken),
            static s => _ = s.Cache.TryAdd(s.Key, s.Entry, s.Token));
        _ = await Assert.That(insert.WasCritical).IsTrue();
        _ = await Assert.That(insert.IsTryAdd).IsTrue();
        _ = await Assert.That(cache.TryAdd(key, initial, cancellationToken)).IsFalse();
        _ = await Assert.That(cache.Update(key, "small", cancellationToken)).IsTrue();
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(initialBytes);
    }

    /// <summary>A replacement can reuse the bytes of an expired resident without waiting for a sweep.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredSlotFundsReplacement(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var estimator = new CacheEntrySizeEstimator<string>();
        var key = new CacheKey("ns", "key");
        var old = new NodeCacheEntry<string>(new string('o', 128), 1, Start.UtcDateTime.AddMinutes(1));
        var residentBytes = estimator.EstimateBytes(key, old, false);
        var cache = new PhysicalCache<string>(clock, estimator, criticalMemoryThresholdBytes: residentBytes + 1);
        ILocalCacheStats stats = cache;
        cache.Set(key, old, cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(2));

        var oversized = new NodeCacheEntry<string>(new string('v', 512));
        _ = NodeExceptionAssert.For<ResourceExhaustedException>().Throws(
            (Cache: cache, Key: key, Entry: oversized, Token: cancellationToken),
            static s => _ = s.Cache.TryAdd(s.Key, s.Entry, s.Token));
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(residentBytes);

        var replacement = new NodeCacheEntry<string>("smaller");
        _ = await Assert.That(cache.TryAdd(key, replacement, cancellationToken)).IsTrue();
        _ = await Assert.That(cache.GetValue(key, cancellationToken).Value).IsEqualTo("smaller");
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(estimator.EstimateBytes(key, replacement, false));
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);
    }

    private static async Task<bool> TryInsertAsync(
        Task start,
        PhysicalCache<string> cache,
        CacheKey key,
        NodeCacheEntry<string> entry,
        bool tryAdd,
        CancellationToken cancellationToken)
    {
        await start;
        try
        {
            if (tryAdd)
                return cache.TryAdd(key, entry, cancellationToken);

            cache.Set(key, entry, cancellationToken);
            return true;
        }
        catch (ResourceExhaustedException exception) when (exception.WasCritical == false && exception.IsTryAdd == tryAdd)
        {
            return false;
        }
    }

    private static async Task MutateAsync(Task start, PhysicalCache<string> cache, CacheKey[] keys, int worker, CancellationToken cancellationToken)
    {
        await start;
        for (var i = 0; i < 641; i++)
        {
            var key = keys[(i + worker) % keys.Length];
            var value = new string('v', 1 + ((i + worker) % 31));
            switch ((i + (3 * worker)) % 8)
            {
                case 0:
                    await cache.InsertRecoveryAsync(key, new NodeCacheEntry<string>(value, 3), cancellationToken, 64);
                    break;
                case 1:
                    _ = cache.TryAdd(key, new NodeCacheEntry<string>(value), cancellationToken);
                    break;
                case 2:
                    _ = cache.Update(key, value, cancellationToken);
                    break;
                case 3:
                    _ = cache.Touch(key, TimeSpan.FromMinutes(1), cancellationToken);
                    break;
                case 4:
                    _ = cache.RemoveExpiration(key, cancellationToken);
                    break;
                case 5:
                    _ = cache.Remove(key, cancellationToken);
                    break;
                default:
                    cache.Set(key, new NodeCacheEntry<string>(value, expiration: TimeSpan.FromMinutes(5)), cancellationToken);
                    break;
            }
        }
    }
}
