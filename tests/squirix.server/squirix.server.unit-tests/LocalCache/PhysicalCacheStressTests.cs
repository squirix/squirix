using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>
/// Multi-threaded load tests for the invariants of a single-lock <see cref="PhysicalCache{T}" />.
/// The former design (a lock-free store plus a separately locked eviction index) could diverge
/// under contention - a key present in one structure but not the other - which showed up as ghost
/// entries that eviction could never reclaim and as single-shot CAS
/// mutations quietly failing on a live key. Merging both into one node per key under a
/// single lock makes those failure modes structurally impossible; these tests hammer the cache to
/// prove it under load.
/// </summary>
[Immutable]
[Property(StressTrait.TraitName, StressTrait.TraitValue)]
public sealed class PhysicalCacheStressTests : ServerUnitTestBase
{
    /// <summary>
    /// Concurrent writers, removers, readers, and touchers over a bounded cache must never leave
    /// the store above its capacity. A ghost entry (present in the store but missing from the
    /// eviction order) is never evicted, so the store grows past the bound permanently.
    /// </summary>
    /// <param name="policyInt">The eviction policy to load-test.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task ConcurrentSetRemoveKeepsCapacityBounded(int policyInt, CancellationToken cancellationToken)
    {
        const int capacity = 32;
        const int keyCount = 64;
        const int workers = 8;
        const int iterations = 25_000;
        var policy = Unsafe.As<int, EvictionPolicyType>(ref policyInt);
        var estimator = new CacheEntrySizeEstimator<string>();
        var cache = new PhysicalCache<string>(TimeProvider.System, estimator, new EvictionOptions { Capacity = capacity, Policy = policy });
        var keys = CreateKeys(keyCount);

        var jobs = new Task[workers];
        for (var worker = 0; worker < workers; worker++)
        {
            jobs[worker] = Task.Factory.StartNew(
                RunMixedLoadWorker,
                new PhysicalCacheStressState(cache, keys, worker, iterations, null, worker, cancellationToken),
                cancellationToken,
                TaskCreationOptions.None,
                TaskScheduler.Default);
        }

        var maxObserved = 0;
        while (!AllCompleted(jobs))
        {
            var current = EntryCountOf(cache);
            if (current > maxObserved)
                maxObserved = current;

            await Task.Delay(5, cancellationToken);
        }

        await Task.WhenAll(jobs);
        maxObserved = Math.Max(maxObserved, EntryCountOf(cache));

        _ = await Assert.That(maxObserved).IsBetween(0, capacity);
        _ = await Assert.That(EntryCountOf(cache)).IsBetween(0, capacity);
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(await SumOfLiveEntriesAsync(cache, estimator, cancellationToken));
        _ = await Assert.That(EntryCountOf(cache)).IsEqualTo(await CountLiveEntriesAsync(cache, cancellationToken));
    }

    /// <summary>
    /// TouchAsync on a key that stays live must always report success, even while other threads
    /// concurrently reset the same key's expiration. The former single-shot CAS (one
    /// <c language="csharp">TryUpdate</c> without a retry) could lose to a concurrent expiration
    /// reset on the same live key and silently return false - a no-op for an entry that needed
    /// touching.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentTouchNeverFailsOnLiveKey(CancellationToken cancellationToken)
    {
        const int keyCount = 8;
        const int workersPerRole = 4;
        const int iterations = 10_000;
        var time = new FakeTimeProvider();
        var estimator = new CacheEntrySizeEstimator<string>();
        var cache = new PhysicalCache<string>(time, estimator);
        var keys = CreateKeys(keyCount);
        for (var i = 0; i < keys.Length; i++)
            cache.Set(keys[i], new NodeCacheEntry<string>(new string('v', 1 + (i * 13)), expiration: TimeSpan.FromHours(1)), cancellationToken);

        var failures = new int[workersPerRole * 2];
        var jobs = new Task[workersPerRole * 2];
        for (var worker = 0; worker < workersPerRole; worker++)
        {
            jobs[worker] = Task.Factory.StartNew(
                RunTouchWorker,
                new PhysicalCacheStressState(cache, keys, worker, iterations, failures, worker, cancellationToken),
                cancellationToken,
                TaskCreationOptions.None,
                TaskScheduler.Default);
            jobs[worker + workersPerRole] = Task.Factory.StartNew(
                RunResetExpirationWorker,
                new PhysicalCacheStressState(cache, keys, worker, iterations, failures, worker + workersPerRole, cancellationToken),
                cancellationToken,
                TaskCreationOptions.None,
                TaskScheduler.Default);
        }

        await Task.WhenAll(jobs);

        var totalFailures = 0;
        foreach (var count in failures)
            totalFailures += count;

        _ = await Assert.That(totalFailures).IsEqualTo(0);
        _ = await Assert.That(EntryCountOf(cache)).IsEqualTo(keyCount);
        foreach (var key in keys)
            _ = await Assert.That(cache.GetEntry(key, cancellationToken)).IsNotNull();

        _ = await Assert.That(BytesOf(cache)).IsEqualTo(await SumOfLiveEntriesAsync(cache, estimator, cancellationToken));
    }

    private static bool AllCompleted(Task[] jobs)
    {
        foreach (var job in jobs)
        {
            if (!job.IsCompleted)
                return false;
        }

        return true;
    }

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static async Task<int> CountLiveEntriesAsync(PhysicalCache<string> cache, CancellationToken cancellationToken)
    {
        var count = 0;
        await foreach (var (_, _) in cache.EnumerateLiveAsync(cancellationToken))
            count++;

        return count;
    }

    private static CacheKey[] CreateKeys(int keyCount)
    {
        var keys = new CacheKey[keyCount];
        for (var i = 0; i < keyCount; i++)
            keys[i] = new CacheKey("ns", $"k-{i}");

        return keys;
    }

    private static int EntryCountOf(ILocalCacheStats stats) => stats.EntryCount;

    private static void RunMixedLoadWorker(object? state)
    {
        var s = state as PhysicalCacheStressState ?? ThrowHelper.Throw<PhysicalCacheStressState>(new InvalidOperationException());
        RunMixedLoadWorkerCore(s);
    }

    private static void RunMixedLoadWorkerCore(PhysicalCacheStressState s)
    {
        for (var i = 0; i < s.Iterations; i++)
        {
            var key = s.Keys[(i + s.Offset) % s.Keys.Length];
            var phase = (i + s.Offset) % 10;
            if (phase < 6)
                s.Cache.Set(key, new NodeCacheEntry<string>(new string('v', 1 + (s.Offset * 11) + (i % 7))), s.Ct);
            else if (phase < 8)
                _ = s.Cache.Remove(key, s.Ct);
            else if (phase < 9)
                _ = s.Cache.GetValue(key, s.Ct);
            else
                _ = s.Cache.Touch(key, TimeSpan.FromMinutes(5), s.Ct);
        }
    }

    private static void RunResetExpirationWorker(object? state)
    {
        var s = state as PhysicalCacheStressState ?? ThrowHelper.Throw<PhysicalCacheStressState>(new InvalidOperationException());
        RunResetExpirationWorkerCore(s);
    }

    private static void RunResetExpirationWorkerCore(PhysicalCacheStressState s)
    {
        for (var i = 0; i < s.Iterations; i++)
        {
            var key = s.Keys[(i + (s.Offset * 3)) % s.Keys.Length];
            _ = s.Cache.RemoveExpiration(key, s.Ct);
        }
    }

    private static void RunTouchWorker(object? state)
    {
        var s = state as PhysicalCacheStressState ?? ThrowHelper.Throw<PhysicalCacheStressState>(new InvalidOperationException());
        RunTouchWorkerCore(s);
    }

    private static void RunTouchWorkerCore(PhysicalCacheStressState s)
    {
        for (var i = 0; i < s.Iterations; i++)
        {
            var key = s.Keys[(i + s.Offset) % s.Keys.Length];
            if (!s.Cache.Touch(key, TimeSpan.FromMinutes(5), s.Ct))
                s.Failures![s.FailureSlot]++;
        }
    }

    private static async Task<long> SumOfLiveEntriesAsync(PhysicalCache<string> cache, CacheEntrySizeEstimator<string> estimator, CancellationToken cancellationToken)
    {
        long sum = 0;
        await foreach (var (key, entry) in cache.EnumerateLiveAsync(cancellationToken))
            sum += estimator.EstimateBytes(key, entry, false);

        return sum;
    }

    private sealed class PhysicalCacheStressState
    {
        internal PhysicalCacheStressState(
            PhysicalCache<string> cache,
            CacheKey[] keys,
            int offset,
            int iterations,
            int[]? failures,
            int failureSlot,
            CancellationToken cancellationToken)
        {
            Cache = cache;
            Keys = keys;
            Offset = offset;
            Iterations = iterations;
            Failures = failures;
            FailureSlot = failureSlot;
            Ct = cancellationToken;
        }

        internal PhysicalCache<string> Cache { get; }

        internal CancellationToken Ct { get; }

        internal int FailureSlot { get; }

        internal int[]? Failures { get; }

        internal int Iterations { get; }

        internal CacheKey[] Keys { get; }

        internal int Offset { get; }
    }
}
