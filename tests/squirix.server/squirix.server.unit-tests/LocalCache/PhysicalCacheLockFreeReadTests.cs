using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
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
    private const int ExpiredReadAttempts = 8;
    private const int MaxLoadValueLength = 40;
    private const int LoadOperationCount = 4;
    private const int StableKeyCount = 32;
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
        var done = false;

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                try
                {
                    for (var i = 0; i < WriterIterations; i++)
                    {
                        var entry = (i & 1) == 0 ? new NodeCacheEntry<string>("B", 2, DeadlineB, null, TagsB) : new NodeCacheEntry<string>("A", 1, DeadlineA, null, TagsA);
                        await cache.SetAsync(SharedKey, entry, cancellationToken);
                    }
                }
                finally
                {
                    Volatile.Write(ref done, true);
                }
            },
        };
        var reader = async () =>
        {
            while (!Volatile.Read(ref done))
            {
                var entry = await cache.GetEntryAsync(SharedKey, cancellationToken);
                if (entry == null || !IsConsistent(entry))
                    _ = Interlocked.Increment(ref torn);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await ThreadRace.RunAsync(bodies, RaceTimeout, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref torn)).IsEqualTo(0);
    }

    /// <summary>A read racing a removal finds the value or finds nothing, never a found entry without its value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadersRacingRemovalFindValueOrNothing(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var broken = 0;
        var done = false;

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                try
                {
                    for (var i = 0; i < WriterIterations; i++)
                    {
                        await cache.SetAsync(SharedKey, new NodeCacheEntry<string>("v"), cancellationToken);
                        _ = await cache.RemoveAsync(SharedKey, cancellationToken);
                    }
                }
                finally
                {
                    Volatile.Write(ref done, true);
                }
            },
        };
        var reader = async () =>
        {
            while (!Volatile.Read(ref done))
            {
                var result = await cache.GetValueAsync(SharedKey, cancellationToken);
                var entry = await cache.GetEntryAsync(SharedKey, cancellationToken);
                if ((result.Found && !IsValue(result.Value)) || (entry != null && !IsValue(entry.Value)))
                    _ = Interlocked.Increment(ref broken);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await ThreadRace.RunAsync(bodies, RaceTimeout, cancellationToken);

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

        await ThreadRace.RunAsync(bodies, RaceTimeout, cancellationToken);

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

    /// <summary>A read that starts after a removal completed never finds the removed entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadAfterRemovalNeverFindsRemovedEntry(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var removedThrough = -1;
        var stale = 0;
        var done = false;

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                try
                {
                    for (var i = 0; i < WriterIterations; i++)
                    {
                        await cache.SetAsync(SharedKey, new NodeCacheEntry<string>(i.ToString(CultureInfo.InvariantCulture)), cancellationToken);
                        _ = await cache.RemoveAsync(SharedKey, cancellationToken);
                        Volatile.Write(ref removedThrough, i);
                    }
                }
                finally
                {
                    Volatile.Write(ref done, true);
                }
            },
        };
        var reader = async () =>
        {
            while (!Volatile.Read(ref done))
            {
                var removed = Volatile.Read(ref removedThrough);
                var result = await cache.GetValueAsync(SharedKey, cancellationToken);
                if (result.Found && int.Parse(result.Value!, CultureInfo.InvariantCulture) <= removed)
                    _ = Interlocked.Increment(ref stale);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await ThreadRace.RunAsync(bodies, RaceTimeout, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref stale)).IsEqualTo(0);
    }

    /// <summary>Keys that stay stored are always found, and keys never stored never are, while the store grows under concurrent adds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadersFindStoredKeysWhileStoreGrows(CancellationToken cancellationToken)
    {
        const int addedCount = 200_000;
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var stable = await StoreStableKeysAsync(cache, cancellationToken);
        var absent = CreateKeys("absent", StableKeyCount);
        var wrong = 0;
        var done = false;
        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                try
                {
                    for (var i = 0; i < addedCount; i++)
                        await cache.SetAsync(CacheKey.Default("added" + i.ToString(CultureInfo.InvariantCulture)), new NodeCacheEntry<string>("a"), cancellationToken);
                }
                finally
                {
                    Volatile.Write(ref done, true);
                }
            },
        };
        var reader = async () =>
        {
            var i = 0;
            while (!Volatile.Read(ref done))
            {
                var slot = i++ % StableKeyCount;
                if (await ReadsWrongAsync(cache, stable[slot], absent[slot], cancellationToken))
                    _ = Interlocked.Increment(ref wrong);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await ThreadRace.RunAsync(bodies, RaceTimeout, cancellationToken);

        ILocalCacheStats stats = cache;
        _ = await Assert.That(Volatile.Read(ref wrong)).IsEqualTo(0);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(StableKeyCount + addedCount);
    }

    /// <summary>Keys that stay stored are always found, and keys never stored never are, while the store grows under concurrent adds and removals.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadersFindKeysDuringGrowthWithRemovals(CancellationToken cancellationToken)
    {
        const int steps = 100_000;
        var cache = new PhysicalCache<string>(TimeProvider.System, new CacheEntrySizeEstimator<string>());
        var stable = await StoreStableKeysAsync(cache, cancellationToken);
        var absent = CreateKeys("absent", StableKeyCount);
        var wrong = 0;
        var done = false;
        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                try
                {
                    // Two adds and one removal per step: the store keeps growing while nodes leave it.
                    for (var i = 0; i < steps; i++)
                    {
                        var suffix = i.ToString(CultureInfo.InvariantCulture);
                        var removedKey = CacheKey.Default("removed" + suffix);
                        await cache.SetAsync(removedKey, new NodeCacheEntry<string>("r"), cancellationToken);
                        await cache.SetAsync(CacheKey.Default("kept" + suffix), new NodeCacheEntry<string>("k"), cancellationToken);
                        _ = await cache.RemoveAsync(removedKey, cancellationToken);
                    }
                }
                finally
                {
                    Volatile.Write(ref done, true);
                }
            },
        };
        var reader = async () =>
        {
            var i = 0;
            while (!Volatile.Read(ref done))
            {
                var slot = i++ % StableKeyCount;
                if (await ReadsWrongAsync(cache, stable[slot], absent[slot], cancellationToken))
                    _ = Interlocked.Increment(ref wrong);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await ThreadRace.RunAsync(bodies, RaceTimeout, cancellationToken);

        ILocalCacheStats stats = cache;
        _ = await Assert.That(Volatile.Read(ref wrong)).IsEqualTo(0);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(StableKeyCount + steps);
    }

    /// <summary>A reader of a value-type value always sees the fields of one write, never a value torn between two.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadersNeverSeeTornStructValues(CancellationToken cancellationToken)
    {
        const int writes = 200_000;
        const int setEvery = 8;
        var cache = new PhysicalCache<Quad>(TimeProvider.System, new CacheEntrySizeEstimator<Quad>());
        await cache.SetAsync(SharedKey, new NodeCacheEntry<Quad>(new Quad(0, 0, 0, 0)), cancellationToken);
        var torn = 0;
        var done = false;

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                try
                {
                    // Most steps replace only the value, the cheapest write, so writes overlap reads often; the rest replace the whole entry.
                    for (long i = 1; i <= writes; i++)
                    {
                        var quad = new Quad(i, i, i, i);
                        if (i % setEvery == 0)
                            await cache.SetAsync(SharedKey, new NodeCacheEntry<Quad>(quad), cancellationToken);
                        else
                            _ = await cache.UpdateAsync(SharedKey, quad, cancellationToken);
                    }
                }
                finally
                {
                    Volatile.Write(ref done, true);
                }
            },
        };
        var reader = async () =>
        {
            while (!Volatile.Read(ref done))
            {
                // A value read allocates nothing, so reads come fast enough to overlap the writes often.
                var result = await cache.GetValueAsync(SharedKey, cancellationToken);
                if (!result.Found || !result.Value.IsWhole)
                    _ = Interlocked.Increment(ref torn);
            }
        };
        for (var r = 0; r < ReaderCount; r++)
            bodies.Add(reader);

        await ThreadRace.RunAsync(bodies, RaceTimeout, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref torn)).IsEqualTo(0);
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

    private static async Task<bool> RunExpiredReadRoundAsync(
        CacheEntrySizeEstimator<string> estimator,
        NodeCacheEntry<string> fresh,
        long freshBytes,
        CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var cache = new PhysicalCache<string>(clock, estimator);
        await cache.SetAsync(SharedKey, new NodeCacheEntry<string>("stale", 1, null, TimeSpan.FromSeconds(5)), cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(10));

        var bodies = new List<Func<Task>>
        {
            async () =>
            {
                for (var i = 0; i < ExpiredReadAttempts; i++)
                    _ = await cache.GetEntryAsync(SharedKey, cancellationToken);
            },
            async () => _ = await cache.TryAddAsync(SharedKey, fresh, cancellationToken),
        };
        await ThreadRace.RunAsync(bodies, RaceTimeout, cancellationToken);

        ILocalCacheStats stats = cache;
        var current = await cache.GetEntryAsync(SharedKey, cancellationToken);
        return current != null && string.Equals(current.Value, "fresh", StringComparison.Ordinal) && stats.EntryCount == 1 && stats.EstimatedBytes == freshBytes;
    }

    /// <summary>Reads a stored key and a never-stored key, live and raw.</summary>
    /// <param name="cache">The cache.</param>
    /// <param name="stableKey">A key that stays stored with the test value.</param>
    /// <param name="absentKey">A key never stored.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="true" /> when a read missed the stored key or found the never-stored one.</returns>
    private static async Task<bool> ReadsWrongAsync(PhysicalCache<string> cache, CacheKey stableKey, CacheKey absentKey, CancellationToken cancellationToken)
    {
        var stable = await cache.GetValueAsync(stableKey, cancellationToken);
        var stableRaw = await cache.RawReader.GetEntryRawAsync(stableKey, cancellationToken);
        var absent = await cache.GetValueAsync(absentKey, cancellationToken);
        var absentRaw = await cache.RawReader.GetEntryRawAsync(absentKey, cancellationToken);
        return !stable.Found || !IsValue(stable.Value) || stableRaw == null || !IsValue(stableRaw.Value) || absent.Found || absentRaw != null;
    }

    private static async Task<CacheKey[]> StoreStableKeysAsync(PhysicalCache<string> cache, CancellationToken cancellationToken)
    {
        var stable = CreateKeys("stable", StableKeyCount);
        foreach (var key in stable)
            await cache.SetAsync(key, new NodeCacheEntry<string>("v"), cancellationToken);

        return stable;
    }

    private static CacheKey[] CreateKeys(string prefix, int count)
    {
        var keys = new CacheKey[count];
        for (var i = 0; i < count; i++)
            keys[i] = CacheKey.Default(prefix + i.ToString(CultureInfo.InvariantCulture));

        return keys;
    }

    private static Func<Task> CreateLoadWorker(PhysicalCache<string> cache, CacheKey[] keys, int seed, int iterations, CancellationToken cancellationToken) =>
        async () =>
        {
            var state = seed;
            for (var i = 0; i < iterations; i++)
            {
                var key = keys[Next(ref state, keys.Length)];
                var entry = new NodeCacheEntry<string>(new string('x', 1 + Next(ref state, MaxLoadValueLength)));
                switch (Next(ref state, LoadOperationCount))
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

    /// <summary>
    /// A value of four fields that every write keeps equal, so a read mixing two writes shows. It is wider than one vector move, so a plain copy
    /// of it can tear.
    /// </summary>
    /// <param name="A">The first field.</param>
    /// <param name="B">The second field.</param>
    /// <param name="C">The third field.</param>
    /// <param name="D">The fourth field.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct Quad(long A, long B, long C, long D)
    {
        /// <summary>Gets a value indicating whether every field holds the same number, as every write leaves them.</summary>
        internal bool IsWhole => A == B && B == C && C == D;
    }
}
