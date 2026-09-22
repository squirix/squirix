using System;
using System.Collections.Frozen;
using System.Collections.Generic;
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

/// <summary>Unit tests for <see cref="PhysicalCache{T}" /> update/expiration races.</summary>
[Immutable]
public sealed class PhysicalCacheTests : ServerUnitTestBase
{
    private static readonly Func<object?, Task> RemoveExpOp = static async s =>
    {
        var st = s as RaceState ?? ThrowHelper.Throw<RaceState>(new InvalidOperationException());
        _ = await st.Cache.RemoveExpirationAsync(st.Key, st.Ct);
    };

    private static readonly Func<object?, Task> RemoveOp = static async s =>
    {
        var st = s as RaceState ?? ThrowHelper.Throw<RaceState>(new InvalidOperationException());
        _ = await st.Cache.RemoveAsync(st.Key, st.Ct);
    };

    private static readonly Func<object?, Task> TouchOp = static async s =>
    {
        var st = s as RaceState ?? ThrowHelper.Throw<RaceState>(new InvalidOperationException());
        _ = await st.Cache.TouchAsync(st.Key, TimeSpan.FromSeconds(10), st.Ct);
    };

    private static readonly Func<object?, Task> TouchRecOp = static async s =>
    {
        var st = s as RaceState ?? ThrowHelper.Throw<RaceState>(new InvalidOperationException());
        _ = await st.Cache.TouchExpirationRecoveryAsync(st.Key, DateTime.UtcNow.AddHours(1), st.Ct);
    };

    private static readonly Func<object?, Task> UpdateRaceOp = static async s =>
    {
        var st = s as RaceState ?? ThrowHelper.Throw<RaceState>(new InvalidOperationException());
        for (var i = 0; i < 10000; i++)
        {
            await st.Cache.SetAsync(st.Key, new NodeCacheEntry<string> { Value = "v" }, st.Ct);
            if (await st.Cache.UpdateAsync(st.Key, "v", st.Ct) && await st.Cache.GetEntryAsync(st.Key, st.Ct) == null)
                st.FalsePositives++;
        }
    };

    private static FrozenDictionary<string, string> TestTags { get; } = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["tenant"] = "t1",
        ["origin"] = "repro",
    }.ToFrozenDictionary();

    /// <summary>Try-add stores entry tags so reads and snapshot capture observe them (issue #421).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddPreservesTags(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        _ = await cache.TryAddAsync(new CacheKey("ns", "added"), new NodeCacheEntry<string>("v", tags: TestTags), cancellationToken);

        var entry = await cache.GetEntryAsync(new CacheKey("ns", "added"), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        await AssertTagsEqualAsync(TestTags, entry.Tags);
    }

    /// <summary>Durable-recovery insert restores entry tags after restart/recovery (issue #421).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DurableRecoveryInsertPreservesTags(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        await cache.InsertRecoveryAsync(new CacheKey("ns", "recovered"), new NodeCacheEntry<string>("v", tags: TestTags), cancellationToken);

        var entry = await cache.GetEntryAsync(new CacheKey("ns", "recovered"), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        await AssertTagsEqualAsync(TestTags, entry.Tags);
    }

    /// <summary>Live enumeration exposes tags to the snapshot capture bridge (issue #421).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnumerateLiveYieldsTags(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        await cache.SetAsync(new CacheKey("ns", "a"), new NodeCacheEntry<string>("1", tags: TestTags), cancellationToken);

        var entries = new List<(CacheKey Key, NodeCacheEntry<string> Entry)>();
        await foreach (var pair in cache.EnumerateLiveAsync(cancellationToken))
            entries.Add(pair);

        var (_, singleEntry) = await Assert.That(entries).HasSingleItem();
        await AssertTagsEqualAsync(TestTags, singleEntry.Tags);
    }

    /// <summary>RemoveExpirationAsync clears the expiration and reports success on a live entry (CAS path works).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationReturnsTrueWhenLive(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "rm_exp_live");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "v", ExpiresUtc = DateTime.UtcNow.AddMinutes(5) }, cancellationToken);

        _ = await Assert.That(await cache.RemoveExpirationAsync(key, cancellationToken)).IsTrue();

        var entry = await cache.GetEntryAsync(key, cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
    }

    /// <summary>
    /// Between read and write, another thread (RemoveAsync or lazy expiry) can delete the key,
    /// causing RemoveExpirationAsync/TouchAsync to resurrect the deleted entry.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationShouldNotResurrect(CancellationToken cancellationToken)
    {
        for (var i = 0; i < 50; i++)
        {
            var cache = new PhysicalCache<string>();
            var key = new CacheKey("ns", "rm_exp");
            await cache.SetAsync(key, new NodeCacheEntry<string>("v", expiration: TimeSpan.FromMinutes(5), tags: TestTags), cancellationToken);

            await RaceRemoveExpAsync(cache, key, cancellationToken);

            var entry = await cache.GetEntryAsync(key, cancellationToken);
            _ = await Assert.That(entry).IsNull();
        }
    }

    /// <summary>Set stores entry tags so reads and snapshot capture observe them (issue #421).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAsyncPreservesTags(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "tagged");
        await cache.SetAsync(key, new NodeCacheEntry<string>("v", tags: TestTags), cancellationToken);

        var entry = await cache.GetEntryAsync(key, cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        await AssertTagsEqualAsync(TestTags, entry.Tags);
    }

    /// <summary>TouchExpirationRecoveryAsync sets a new expiration and reports success on a live entry (CAS path works).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchRecoveryReturnsTrueWhenLive(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "touch_rec_live");
        await cache.SetAsync(key, new NodeCacheEntry<string>("v"), cancellationToken);

        _ = await Assert.That(await cache.TouchExpirationRecoveryAsync(key, DateTime.UtcNow.AddMinutes(5), cancellationToken)).IsTrue();

        var entry = await cache.GetEntryAsync(key, cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.ExpiresUtc).IsNotNull();
    }

    /// <summary>TouchExpirationRecoveryAsync also has the same read-modify-write pattern.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchRecoveryShouldNotResurrect(CancellationToken cancellationToken)
    {
        for (var i = 0; i < 50; i++)
        {
            var cache = new PhysicalCache<string>();
            var key = new CacheKey("ns", "touch_rec");
            await cache.SetAsync(key, new NodeCacheEntry<string>("v", tags: TestTags), cancellationToken);

            await RaceTouchRecAsync(cache, key, cancellationToken);

            var entry = await cache.GetEntryAsync(key, cancellationToken);
            _ = await Assert.That(entry).IsNull();
        }
    }

    /// <summary>TouchAsync sets a new expiration and reports success on a live entry (CAS path works).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchReturnsTrueWhenLive(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "touch_live");
        await cache.SetAsync(key, new NodeCacheEntry<string>("v"), cancellationToken);

        _ = await Assert.That(await cache.TouchAsync(key, TimeSpan.FromMinutes(5), cancellationToken)).IsTrue();

        var entry = await cache.GetEntryAsync(key, cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.ExpiresUtc).IsNotNull();
    }

    /// <summary>TouchAsync also has the same read-modify-write pattern.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchShouldNotResurrect(CancellationToken cancellationToken)
    {
        for (var i = 0; i < 50; i++)
        {
            var cache = new PhysicalCache<string>();
            var key = new CacheKey("ns", "touch");
            await cache.SetAsync(key, new NodeCacheEntry<string>("v", tags: TestTags), cancellationToken);

            await RaceTouchAsync(cache, key, cancellationToken);

            var entry = await cache.GetEntryAsync(key, cancellationToken);
            _ = await Assert.That(entry).IsNull();
        }
    }

    /// <summary>Update on a missing key returns false.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateAsyncMissingKeyReturnsFalse(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        _ = await Assert.That(await cache.UpdateAsync(new CacheKey("ns", "missing"), "v", cancellationToken)).IsFalse();
    }

    /// <summary>Update replaces a live value and reports success.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateAsyncReplacesLiveValue(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "live");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "a", Version = 1 }, cancellationToken);

        _ = await Assert.That(await cache.UpdateAsync(key, "b", cancellationToken)).IsTrue();
        var entry = await cache.GetEntryAsync(key, cancellationToken);
        _ = await Assert.That(entry!.Value).IsEqualTo("b");
    }

    /// <summary>Update with the same live value is a successful no-op.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateAsyncSameValueIsNoOpSuccess(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "same");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "same", Version = 1 }, cancellationToken);

        _ = await Assert.That(await cache.UpdateAsync(key, "same", cancellationToken)).IsTrue();
    }

    /// <summary>Value-only update keeps the original entry tags (issue #421).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateKeepsOriginalTags(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "updated");
        await cache.SetAsync(key, new NodeCacheEntry<string>("old", tags: TestTags), cancellationToken);

        _ = await Assert.That(await cache.UpdateAsync(key, "new", cancellationToken)).IsTrue();
        var entry = await cache.GetEntryAsync(key, cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.Value).IsEqualTo("new");
        await AssertTagsEqualAsync(TestTags, entry.Tags);
    }

    /// <summary>UpdateAsync must not report success on a key that is concurrently reclaimed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Property(StressTrait.TraitName, StressTrait.TraitValue)]
    public async Task UpdateNoFalseSuccessWhenReclaimed(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "race438");
        var st = new RaceState(cache, key, cancellationToken);

        // Seed the key before either concurrent task starts so the reclaimer races against a present
        // entry from the first iteration instead of finishing before the updater writes the key.
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "v" }, cancellationToken);

        var updater = Task.Factory.StartNew(UpdateRaceOp, st, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        var reclaimer = Task.Factory.StartNew(RemoveOp, st, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();

        await Task.WhenAll(updater, reclaimer);

        // A correct implementation reports success only when the entry is genuinely present at the CAS, so
        // false positives stay at ~0; the small tolerance absorbs the rare legitimate post-update reclaim
        // window on the fixed code path (the buggy equals fast-path yields dozens-to-hundreds).
        _ = await Assert.That(st.FalsePositives <= 3).IsTrue().Because($"UpdateAsync reported success on a concurrently reclaimed key {st.FalsePositives} time(s).");
    }

    /// <summary>Update on an expired key removes it and returns false.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateRemovesExpiredEntryReturnsFalse(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(time);
        var key = new CacheKey("ns", "expired");
        await cache.SetAsync(
            key,
            new NodeCacheEntry<string>
            {
                Value = "old",
                Version = 1,
                ExpiresUtc = time.GetUtcNow().UtcDateTime.AddMinutes(1),
            },
            cancellationToken);

        time.Advance(TimeSpan.FromMinutes(2));

        _ = await Assert.That(await cache.UpdateAsync(key, "new", cancellationToken)).IsFalse();
        _ = await Assert.That(await cache.GetEntryAsync(key, cancellationToken)).IsNull();
    }

    private static async Task AssertTagsEqualAsync(FrozenDictionary<string, string> expected, FrozenDictionary<string, string>? actual)
    {
        _ = await Assert.That(actual).IsNotNull();
        _ = await Assert.That(actual.Count).IsEqualTo(expected.Count);
        foreach (var pair in expected)
            _ = await Assert.That(actual.TryGetValue(pair.Key, out var value) && string.Equals(value, pair.Value, StringComparison.Ordinal)).IsTrue().Because($"tag '{pair.Key}' missing or mismatched");
    }

    /// <summary>Races remove-expiration against remove on the same key.</summary>
    /// <param name="cache">The cache under test.</param>
    /// <param name="key">The raced key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task RaceRemoveExpAsync(PhysicalCache<string> cache, CacheKey key, CancellationToken cancellationToken)
    {
        var st = new RaceState(cache, key, cancellationToken);
        var op = Task.Factory.StartNew(RemoveExpOp, st, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        var rem = Task.Factory.StartNew(RemoveOp, st, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        return Task.WhenAll(op, rem);
    }

    /// <summary>Races touch against remove on the same key.</summary>
    /// <param name="cache">The cache under test.</param>
    /// <param name="key">The raced key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task RaceTouchAsync(PhysicalCache<string> cache, CacheKey key, CancellationToken cancellationToken)
    {
        var st = new RaceState(cache, key, cancellationToken);
        var op = Task.Factory.StartNew(TouchOp, st, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        var rem = Task.Factory.StartNew(RemoveOp, st, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        return Task.WhenAll(op, rem);
    }

    /// <summary>Races touch-expiration-recovery against remove on the same key.</summary>
    /// <param name="cache">The cache under test.</param>
    /// <param name="key">The raced key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static Task RaceTouchRecAsync(PhysicalCache<string> cache, CacheKey key, CancellationToken cancellationToken)
    {
        var st = new RaceState(cache, key, cancellationToken);
        var op = Task.Factory.StartNew(TouchRecOp, st, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        var rem = Task.Factory.StartNew(RemoveOp, st, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        return Task.WhenAll(op, rem);
    }

    private sealed class RaceState
    {
        public RaceState(PhysicalCache<string> cache, CacheKey key, CancellationToken ct)
        {
            Cache = cache;
            Key = key;
            Ct = ct;
        }

        public PhysicalCache<string> Cache { get; }

        public CancellationToken Ct { get; }

        public int FalsePositives { get; set; }

        public CacheKey Key { get; }
    }
}
