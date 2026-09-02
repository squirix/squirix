using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>
/// Multi-threaded load tests for the invariants of a single-lock <see cref="PhysicalCache{T}" />.
/// The former design (a lock-free store plus a separately locked eviction index) could diverge
/// under contention - a key present in one structure but not the other - which showed up as ghost
/// entries that eviction could never reclaim (issues #444 and #387) and as single-shot CAS
/// mutations quietly failing on a live key (issue #444). Merging both into one node per key under a
/// single lock makes those failure modes structurally impossible; these tests hammer the cache to
/// prove it under load.
/// </summary>
[Immutable]
[Trait(StressTrait.TraitName, StressTrait.TraitValue)]
public sealed class PhysicalCacheStressTests : ServerUnitTestBase
{
    /// <summary>
    /// TouchAsync on a key that stays live must always report success, even while other threads
    /// concurrently reset the same key's expiration. The former single-shot CAS (one
    /// <c language="csharp">TryUpdate</c> without a retry) could lose to a concurrent expiration
    /// reset on the same live key and silently return false - a no-op for an entry that needed
    /// touching.
    /// </summary>
    [Fact]
    public async Task ConcurrentTouchNeverFailsOnLiveKey()
    {
        const int keyCount = 8;
        const int workersPerRole = 4;
        const int iterations = 10_000;
        var time = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(time);
        var keys = CreateKeys(keyCount);
        foreach (var key in keys)
            await cache.SetAsync(key, new NodeCacheEntry<string>("v", expiration: TimeSpan.FromHours(1)), DefaultCancellationToken);

        var failures = new int[workersPerRole * 2];
        var jobs = new Task[workersPerRole * 2];
        for (var worker = 0; worker < workersPerRole; worker++)
        {
            jobs[worker] = Task.Factory.StartNew(
                RunTouchWorkerAsync,
                new PhysicalCacheStressState(cache, keys, worker, iterations, failures, worker),
                DefaultCancellationToken,
                TaskCreationOptions.None,
                TaskScheduler.Default).Unwrap();
            jobs[worker + workersPerRole] = Task.Factory.StartNew(
                RunResetExpirationWorkerAsync,
                new PhysicalCacheStressState(cache, keys, worker, iterations, failures, worker + workersPerRole),
                DefaultCancellationToken,
                TaskCreationOptions.None,
                TaskScheduler.Default).Unwrap();
        }

        await Task.WhenAll(jobs);

        var totalFailures = 0;
        foreach (var count in failures)
            totalFailures += count;

        Assert.Equal(0, totalFailures);
        Assert.Equal(keyCount, EntryCountOf(cache));
        foreach (var key in keys)
            Assert.NotNull(await cache.GetEntryAsync(key, DefaultCancellationToken));
    }

    /// <summary>
    /// Concurrent writers, removers, readers, and touchers over a bounded cache must never leave
    /// the store above its capacity. A ghost entry (present in the store but missing from the
    /// eviction order) is never evicted, so the store grows past the bound permanently.
    /// </summary>
    /// <param name="policy">The eviction policy to load-test.</param>
    [Theory]
    [InlineData(EvictionPolicyType.Lru)]
    [InlineData(EvictionPolicyType.Fifo)]
    [InlineData(EvictionPolicyType.Lfu)]
    internal async Task ConcurrentSetRemoveKeepsCapacityBounded(EvictionPolicyType policy)
    {
        const int capacity = 32;
        const int keyCount = 64;
        const int workers = 8;
        const int iterations = 25_000;
        var cache = new PhysicalCache<string>(null, new EvictionOptions { Capacity = capacity, Policy = policy });
        var keys = CreateKeys(keyCount);

        var jobs = new Task[workers];
        for (var worker = 0; worker < workers; worker++)
        {
            jobs[worker] = Task.Factory.StartNew(
                RunMixedLoadWorkerAsync,
                new PhysicalCacheStressState(cache, keys, worker, iterations, null, worker),
                DefaultCancellationToken,
                TaskCreationOptions.None,
                TaskScheduler.Default).Unwrap();
        }

        await Task.WhenAll(jobs);

        Assert.InRange(EntryCountOf(cache), 0, capacity);
    }

    private static CacheKey[] CreateKeys(int keyCount)
    {
        var keys = new CacheKey[keyCount];
        for (var i = 0; i < keyCount; i++)
            keys[i] = new CacheKey("ns", $"k-{i}");

        return keys;
    }

    private static int EntryCountOf(ILocalCacheStats stats) => stats.EntryCount;

    private static Task RunMixedLoadWorkerAsync(object? state)
    {
        var s = state as PhysicalCacheStressState ?? throw new InvalidOperationException();
        return RunMixedLoadWorkerCoreAsync(s);
    }

    private static async Task RunMixedLoadWorkerCoreAsync(PhysicalCacheStressState s)
    {
        for (var i = 0; i < s.Iterations; i++)
        {
            var key = s.Keys[(i + s.Offset) % s.Keys.Length];
            var phase = (i + s.Offset) % 10;
            if (phase < 6)
                await s.Cache.SetAsync(key, new NodeCacheEntry<string>($"v{s.Offset}"), DefaultCancellationToken);
            else if (phase < 8)
                _ = await s.Cache.RemoveAsync(key, DefaultCancellationToken);
            else if (phase < 9)
                _ = await s.Cache.GetValueAsync(key, DefaultCancellationToken);
            else
                _ = await s.Cache.TouchAsync(key, TimeSpan.FromMinutes(5), DefaultCancellationToken);
        }
    }

    private static Task RunResetExpirationWorkerAsync(object? state)
    {
        var s = state as PhysicalCacheStressState ?? throw new InvalidOperationException();
        return RunResetExpirationWorkerCoreAsync(s);
    }

    private static async Task RunResetExpirationWorkerCoreAsync(PhysicalCacheStressState s)
    {
        for (var i = 0; i < s.Iterations; i++)
        {
            var key = s.Keys[(i + (s.Offset * 3)) % s.Keys.Length];
            _ = await s.Cache.RemoveExpirationAsync(key, DefaultCancellationToken);
        }
    }

    private static Task RunTouchWorkerAsync(object? state)
    {
        var s = state as PhysicalCacheStressState ?? throw new InvalidOperationException();
        return RunTouchWorkerCoreAsync(s);
    }

    private static async Task RunTouchWorkerCoreAsync(PhysicalCacheStressState s)
    {
        for (var i = 0; i < s.Iterations; i++)
        {
            var key = s.Keys[(i + s.Offset) % s.Keys.Length];
            if (!await s.Cache.TouchAsync(key, TimeSpan.FromMinutes(5), DefaultCancellationToken))
                s.Failures![s.FailureSlot]++;
        }
    }

    private sealed class PhysicalCacheStressState
    {
        internal PhysicalCacheStressState(PhysicalCache<string> cache, CacheKey[] keys, int offset, int iterations, int[]? failures, int failureSlot)
        {
            Cache = cache;
            Keys = keys;
            Offset = offset;
            Iterations = iterations;
            Failures = failures;
            FailureSlot = failureSlot;
        }

        internal PhysicalCache<string> Cache { get; }

        internal int[]? Failures { get; }

        internal int FailureSlot { get; }

        internal int Iterations { get; }

        internal CacheKey[] Keys { get; }

        internal int Offset { get; }
    }
}
