using System;
using System.Collections.Frozen;
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

/// <summary>Reads of a <see cref="PhysicalCache{T}" /> without a capacity take no lock; these tests race them against the writers.</summary>
[Immutable]
public sealed class PhysicalCacheLockFreeReadTests : ServerUnitTestBase
{
    private const string TagName = "k";
    private const int Rounds = 400;
    private const int ReaderCount = 4;
    private const int WriterIterations = 20_000;
    private static readonly TimeSpan RaceTimeout = TimeSpan.FromSeconds(60);
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly CacheKey SharedKey = CacheKey.Default("shared");
    private static readonly FrozenDictionary<string, string> TagsA = CreateTags("A");
    private static readonly FrozenDictionary<string, string> TagsB = CreateTags("B");
    private static readonly DateTime DeadlineA = new(2100, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime DeadlineB = new(2200, 6, 15, 12, 30, 0, DateTimeKind.Utc);

    /// <summary>A reader always sees the value, version, deadline and tags of one write, never fields of two.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadersNeverSeeTornEntries(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        await cache.SetAsync(SharedKey, new NodeCacheEntry<string>("A", 1, DeadlineA, null, TagsA), cancellationToken);
        var torn = 0;
        using var done = new CancellationTokenSource();

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                for (var i = 0; i < WriterIterations; i++)
                {
                    var entry = (i & 1) == 0 ? new NodeCacheEntry<string>("B", 2, DeadlineB, null, TagsB) : new NodeCacheEntry<string>("A", 1, DeadlineA, null, TagsA);
                    await cache.SetAsync(SharedKey, entry, cancellationToken);
                }

                await done.CancelAsync();
            },
        };
        Func<Task> reader = async () =>
        {
            while (!done.IsCancellationRequested)
            {
                var entry = await cache.GetEntryAsync(SharedKey, cancellationToken);
                if (entry == null || !IsConsistent(entry))
                    _ = Interlocked.Increment(ref torn);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await RunTogetherAsync(bodies, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref torn)).IsEqualTo(0);
    }

    /// <summary>A read racing a removal finds the value or finds nothing, never a found entry without its value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadersRacingRemovalFindValueOrNothing(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var broken = 0;
        using var done = new CancellationTokenSource();

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                for (var i = 0; i < WriterIterations; i++)
                {
                    await cache.SetAsync(SharedKey, new NodeCacheEntry<string>("v"), cancellationToken);
                    _ = await cache.RemoveAsync(SharedKey, cancellationToken);
                }

                await done.CancelAsync();
            },
        };
        Func<Task> reader = async () =>
        {
            while (!done.IsCancellationRequested)
            {
                var result = await cache.GetValueAsync(SharedKey, cancellationToken);
                var entry = await cache.GetEntryAsync(SharedKey, cancellationToken);
                if ((result.Found && !IsValue(result.Value)) || (entry != null && !IsValue(entry.Value)))
                    _ = Interlocked.Increment(ref broken);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await RunTogetherAsync(bodies, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref broken)).IsEqualTo(0);
    }

    /// <summary>A read that removes an expired entry cannot remove the entry a concurrent add stored under the same key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredReadKeepsConcurrentlyAddedEntry(CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<string>();
        var fresh = new NodeCacheEntry<string>("fresh");
        var freshBytes = estimator.EstimateBytes(SharedKey, fresh, false);
        var failures = 0;

        for (var round = 0; round < Rounds; round++)
        {
            if (!await RunExpiredReadRoundAsync(estimator, fresh, freshBytes, cancellationToken))
                failures++;
        }

        _ = await Assert.That(failures).IsEqualTo(0);
    }

    /// <summary>After concurrent adds, overwrites and removals, the entry count and the byte total match the live entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CountersMatchLiveEntriesAfterLoad(CancellationToken cancellationToken)
    {
        const int keyCount = 64;
        const int writers = 6;
        const int iterations = 10_000;
        var estimator = new CacheEntrySizeEstimator<string>();
        var cache = new PhysicalCache<string>(TimeProvider.System, estimator);
        var keys = new CacheKey[keyCount];
        for (var i = 0; i < keyCount; i++)
            keys[i] = CacheKey.Default("k" + i.ToString(CultureInfo.InvariantCulture));

        var bodies = new List<Func<Task>>();
        for (var w = 0; w < writers; w++)
            bodies.Add(CreateLoadWorker(cache, keys, w + 1, iterations, cancellationToken));

        await RunTogetherAsync(bodies, cancellationToken);

        var live = 0;
        long bytes = 0;
        await foreach (var (key, entry) in cache.EnumerateLiveAsync(cancellationToken))
        {
            live++;
            bytes += estimator.EstimateBytes(key, entry, false);
        }

        ILocalCacheStats stats = cache;
        _ = await Assert.That(stats.EntryCount).IsEqualTo(live);
        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(bytes);
    }

    /// <summary>A value hit on a cache without a capacity allocates nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ValueHitAllocatesNothing(CancellationToken cancellationToken)
    {
        const int calls = 1000;
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        await cache.SetAsync(SharedKey, new NodeCacheEntry<string>("v", 1, null, TimeSpan.FromDays(1), TagsA), cancellationToken);
        for (var i = 0; i < calls; i++)
            _ = await cache.GetValueAsync(SharedKey, cancellationToken);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < calls; i++)
            _ = await cache.GetValueAsync(SharedKey, cancellationToken);

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        _ = await Assert.That(allocated).IsEqualTo(0L);
    }

    /// <summary>A read that starts after a removal completed never finds the removed entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadAfterRemovalNeverFindsRemovedEntry(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var removedThrough = -1;
        var stale = 0;
        using var done = new CancellationTokenSource();

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                for (var i = 0; i < WriterIterations; i++)
                {
                    await cache.SetAsync(SharedKey, new NodeCacheEntry<string>(i.ToString(CultureInfo.InvariantCulture)), cancellationToken);
                    _ = await cache.RemoveAsync(SharedKey, cancellationToken);
                    Volatile.Write(ref removedThrough, i);
                }

                await done.CancelAsync();
            },
        };
        Func<Task> reader = async () =>
        {
            while (!done.IsCancellationRequested)
            {
                var removed = Volatile.Read(ref removedThrough);
                var result = await cache.GetValueAsync(SharedKey, cancellationToken);
                if (result.Found && int.Parse(result.Value!, CultureInfo.InvariantCulture) <= removed)
                    _ = Interlocked.Increment(ref stale);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await RunTogetherAsync(bodies, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref stale)).IsEqualTo(0);
    }

    /// <summary>Keys that stay stored are always found while the store grows under concurrent adds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadersFindStoredKeysWhileStoreGrows(CancellationToken cancellationToken)
    {
        const int stableCount = 32;
        const int addedCount = 200_000;
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var stable = new CacheKey[stableCount];
        for (var i = 0; i < stableCount; i++)
        {
            stable[i] = CacheKey.Default("stable" + i.ToString(CultureInfo.InvariantCulture));
            await cache.SetAsync(stable[i], new NodeCacheEntry<string>("v"), cancellationToken);
        }

        var missed = 0;
        using var done = new CancellationTokenSource();
        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                for (var i = 0; i < addedCount; i++)
                    await cache.SetAsync(CacheKey.Default("added" + i.ToString(CultureInfo.InvariantCulture)), new NodeCacheEntry<string>("a"), cancellationToken);

                await done.CancelAsync();
            },
        };
        Func<Task> reader = async () =>
        {
            var i = 0;
            while (!done.IsCancellationRequested)
            {
                var result = await cache.GetValueAsync(stable[i++ % stableCount], cancellationToken);
                if (!result.Found || !IsValue(result.Value))
                    _ = Interlocked.Increment(ref missed);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await RunTogetherAsync(bodies, cancellationToken);

        ILocalCacheStats stats = cache;
        _ = await Assert.That(Volatile.Read(ref missed)).IsEqualTo(0);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(stableCount + addedCount);
    }

    /// <summary>A read returns the deadline exactly as stored, its kind included.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadKeepsDeadlineKind(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new DateTime(2100, 3, 4, 5, 6, 7, DateTimeKind.Local);
        var unspecified = new DateTime(2100, 3, 4, 5, 6, 7, DateTimeKind.Unspecified);
        var localKey = CacheKey.Default("local");
        var unspecifiedKey = CacheKey.Default("unspecified");
        await cache.SetAsync(localKey, new NodeCacheEntry<string>("v", 1, local), cancellationToken);
        await cache.SetAsync(unspecifiedKey, new NodeCacheEntry<string>("v", 1, unspecified), cancellationToken);

        var localRead = await cache.GetEntryAsync(localKey, cancellationToken);
        var unspecifiedRead = await cache.GetEntryAsync(unspecifiedKey, cancellationToken);

        _ = await Assert.That(localRead?.ExpiresUtc?.Ticks).IsEqualTo(local.Ticks);
        _ = await Assert.That(localRead?.ExpiresUtc?.Kind).IsEqualTo(DateTimeKind.Local);
        _ = await Assert.That(unspecifiedRead?.ExpiresUtc?.Ticks).IsEqualTo(unspecified.Ticks);
        _ = await Assert.That(unspecifiedRead?.ExpiresUtc?.Kind).IsEqualTo(DateTimeKind.Unspecified);
    }

    private static async Task<bool> RunExpiredReadRoundAsync(CacheEntrySizeEstimator<string> estimator, NodeCacheEntry<string> fresh, long freshBytes, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var cache = new PhysicalCache<string>(clock, estimator);
        await cache.SetAsync(SharedKey, new NodeCacheEntry<string>("stale", 1, null, TimeSpan.FromSeconds(5)), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                for (var i = 0; i < 8; i++)
                    _ = await cache.GetEntryAsync(SharedKey, cancellationToken);
            },
            async () => _ = await cache.TryAddAsync(SharedKey, fresh, cancellationToken),
        };
        await RunTogetherAsync(bodies, cancellationToken);

        ILocalCacheStats stats = cache;
        var current = await cache.GetEntryAsync(SharedKey, cancellationToken);
        return current != null && string.Equals(current.Value, "fresh", StringComparison.Ordinal) && stats.EntryCount == 1 && stats.EstimatedBytes == freshBytes;
    }

    private static Func<Task> CreateLoadWorker(PhysicalCache<string> cache, CacheKey[] keys, int seed, int iterations, CancellationToken cancellationToken) =>
        async () =>
        {
            var state = seed;
            for (var i = 0; i < iterations; i++)
            {
                var key = keys[Next(ref state, keys.Length)];
                var entry = new NodeCacheEntry<string>(new string('x', 1 + Next(ref state, 40)));
                switch (Next(ref state, 4))
                {
                    case 0:
                        await cache.SetAsync(key, entry, cancellationToken);
                        break;
                    case 1:
                        _ = await cache.TryAddAsync(key, entry, cancellationToken);
                        break;
                    case 2:
                        _ = await cache.UpdateAsync(key, "updated", cancellationToken);
                        break;
                    default:
                        _ = await cache.RemoveAsync(key, cancellationToken);
                        break;
                }

                _ = await cache.GetEntryAsync(key, cancellationToken);
            }
        };

    private static FrozenDictionary<string, string> CreateTags(string value) =>
        new Dictionary<string, string>(StringComparer.Ordinal) { [TagName] = value }.ToFrozenDictionary(StringComparer.Ordinal);

    private static bool IsConsistent(NodeCacheEntry<string> entry)
    {
        var isA = string.Equals(entry.Value, "A", StringComparison.Ordinal);
        return entry.Tags != null && string.Equals(entry.Tags[TagName], entry.Value, StringComparison.Ordinal) && entry.Version == (isA ? 1 : 2) &&
            entry.ExpiresUtc == (isA ? DeadlineA : DeadlineB);
    }

    private static bool IsValue(string? value) => string.Equals(value, "v", StringComparison.Ordinal);

    /// <summary>Advances a deterministic xorshift sequence.</summary>
    /// <param name="state">The sequence state; never zero.</param>
    /// <param name="bound">The exclusive upper bound.</param>
    /// <returns>The next number, at least zero and below <paramref name="bound" />.</returns>
    private static int Next(ref int state, int bound)
    {
        state ^= state << 13;
        state ^= state >>> 17;
        state ^= state << 5;
        return int.Abs(state % bound);
    }

    /// <summary>Starts every body behind one gate so they overlap, and fails instead of hanging when they do not finish.</summary>
    /// <param name="bodies">The bodies to run at once.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static async Task RunTogetherAsync(List<Func<Task>> bodies, CancellationToken cancellationToken)
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var jobs = new Task[bodies.Count];
        for (var i = 0; i < bodies.Count; i++)
        {
            jobs[i] = Task.Factory.StartNew(
                RunAfterGateAsync,
                new GatedBody(gate.Task, bodies[i]),
                cancellationToken,
                TaskCreationOptions.None,
                TaskScheduler.Default).Unwrap();
        }

        _ = gate.TrySetResult();
        await Task.WhenAll(jobs).WaitAsync(RaceTimeout, TimeProvider.System, cancellationToken);
    }

    private static async Task RunAfterGateAsync(object? state)
    {
        if (state is not GatedBody gated)
            throw new InvalidOperationException("Unexpected state.");

        await gated.Gate;
        await gated.Body();
    }

    [Immutable]
    private sealed class GatedBody
    {
        internal GatedBody(Task gate, Func<Task> body)
        {
            Gate = gate;
            Body = body;
        }

        internal Task Gate { get; }

        internal Func<Task> Body { get; }
    }
}
