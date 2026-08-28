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
using Xunit;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Unit tests for <see cref="PhysicalCache{T}" /> update/expiration races.</summary>
[Immutable]
public sealed class PhysicalCacheTests : ServerUnitTestBase
{
    private static readonly Func<object?, Task> RemoveExpOp = static async s =>
    {
        var st = s as RaceState ?? throw new InvalidOperationException();
        _ = await st.Cache.RemoveExpirationAsync(st.Key, st.Ct);
    };

    private static readonly Func<object?, Task> RemoveOp = static async s =>
    {
        var st = s as RaceState ?? throw new InvalidOperationException();
        _ = await st.Cache.RemoveAsync(st.Key, st.Ct);
    };

    private static readonly Func<object?, Task> UpdateRaceOp = static async s =>
    {
        var st = s as RaceState ?? throw new InvalidOperationException();
        for (var i = 0; i < 10000; i++)
        {
            await st.Cache.SetAsync(st.Key, new NodeCacheEntry<string> { Value = "v" }, st.Ct);
            if (await st.Cache.UpdateAsync(st.Key, "v", st.Ct) && (await st.Cache.GetEntryAsync(st.Key, st.Ct)) == null)
                st.FalsePositives++;
        }
    };

    private static readonly Func<object?, Task> TouchOp = static async s =>
    {
        var st = s as RaceState ?? throw new InvalidOperationException();
        _ = await st.Cache.TouchAsync(st.Key, TimeSpan.FromSeconds(10), st.Ct);
    };

    private static readonly Func<object?, Task> TouchRecOp = static async s =>
    {
        var st = s as RaceState ?? throw new InvalidOperationException();
        _ = await st.Cache.TouchExpirationRecoveryAsync(st.Key, DateTime.UtcNow.AddHours(1), st.Ct);
    };

    private static FrozenDictionary<string, string> TestTags { get; } = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["tenant"] = "t1",
        ["origin"] = "repro",
    }.ToFrozenDictionary();

    /// <summary>Durable-recovery insert restores entry tags after restart/recovery (issue #421).</summary>
    [Fact]
    public async Task DurableRecoveryInsertPreservesTags()
    {
        var cache = new PhysicalCache<string>();
        await cache.InsertRecoveryAsync(new CacheKey("ns", "recovered"), new NodeCacheEntry<string>("v", tags: TestTags), DefaultCancellationToken);

        var entry = await cache.GetEntryAsync(new CacheKey("ns", "recovered"), DefaultCancellationToken);
        Assert.NotNull(entry);
        AssertTagsEqual(TestTags, entry.Tags);
    }

    /// <summary>Live enumeration exposes tags to the snapshot capture bridge (issue #421).</summary>
    [Fact]
    public async Task EnumerateLiveYieldsTags()
    {
        var cache = new PhysicalCache<string>();
        await cache.SetAsync(new CacheKey("ns", "a"), new NodeCacheEntry<string>("1", tags: TestTags), DefaultCancellationToken);

        var entries = new List<(CacheKey Key, NodeCacheEntry<string> Entry)>();
        await foreach (var pair in cache.EnumerateLiveAsync(DefaultCancellationToken))
            entries.Add(pair);

        var (_, singleEntry) = Assert.Single(entries);
        AssertTagsEqual(TestTags, singleEntry.Tags);
    }

    /// <summary>RemoveExpirationAsync clears the expiration and reports success on a live entry (CAS path works).</summary>
    [Fact]
    public async Task RemoveExpirationReturnsTrueWhenLive()
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "rm_exp_live");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "v", ExpiresUtc = DateTime.UtcNow.AddMinutes(5) }, DefaultCancellationToken);

        Assert.True(await cache.RemoveExpirationAsync(key, DefaultCancellationToken));

        var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Null(entry.ExpiresUtc);
    }

    /// <summary>
    /// Between read and write, another thread (RemoveAsync or lazy expiry) can delete the key,
    /// causing RemoveExpirationAsync/TouchAsync to resurrect the deleted entry.
    /// </summary>
    [Fact]
    public async Task RemoveExpirationShouldNotResurrect()
    {
        for (var i = 0; i < 50; i++)
        {
            var cache = new PhysicalCache<string>();
            var key = new CacheKey("ns", "rm_exp");
            await cache.SetAsync(key, new NodeCacheEntry<string>("v", expiration: TimeSpan.FromMinutes(5), tags: TestTags), DefaultCancellationToken);

            await RaceRemoveExpAsync(cache, key);

            var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
            Assert.Null(entry);
        }
    }

    /// <summary>Set stores entry tags so reads and snapshot capture observe them (issue #421).</summary>
    [Fact]
    public async Task SetAsyncPreservesTags()
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "tagged");
        await cache.SetAsync(key, new NodeCacheEntry<string>("v", tags: TestTags), DefaultCancellationToken);

        var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
        Assert.NotNull(entry);
        AssertTagsEqual(TestTags, entry.Tags);
    }

    /// <summary>TouchExpirationRecoveryAsync sets a new expiration and reports success on a live entry (CAS path works).</summary>
    [Fact]
    public async Task TouchRecoveryReturnsTrueWhenLive()
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "touch_rec_live");
        await cache.SetAsync(key, new NodeCacheEntry<string>("v"), DefaultCancellationToken);

        Assert.True(await cache.TouchExpirationRecoveryAsync(key, DateTime.UtcNow.AddMinutes(5), DefaultCancellationToken));

        var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
        Assert.NotNull(entry);
        _ = Assert.NotNull(entry.ExpiresUtc);
    }

    /// <summary>TouchExpirationRecoveryAsync also has the same read-modify-write pattern.</summary>
    [Fact]
    public async Task TouchRecoveryShouldNotResurrect()
    {
        for (var i = 0; i < 50; i++)
        {
            var cache = new PhysicalCache<string>();
            var key = new CacheKey("ns", "touch_rec");
            await cache.SetAsync(key, new NodeCacheEntry<string>("v", tags: TestTags), DefaultCancellationToken);

            await RaceTouchRecAsync(cache, key);

            var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
            Assert.Null(entry);
        }
    }

    /// <summary>TouchAsync sets a new expiration and reports success on a live entry (CAS path works).</summary>
    [Fact]
    public async Task TouchReturnsTrueWhenLive()
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "touch_live");
        await cache.SetAsync(key, new NodeCacheEntry<string>("v"), DefaultCancellationToken);

        Assert.True(await cache.TouchAsync(key, TimeSpan.FromMinutes(5), DefaultCancellationToken));

        var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
        Assert.NotNull(entry);
        _ = Assert.NotNull(entry.ExpiresUtc);
    }

    /// <summary>TouchAsync also has the same read-modify-write pattern.</summary>
    [Fact]
    public async Task TouchShouldNotResurrect()
    {
        for (var i = 0; i < 50; i++)
        {
            var cache = new PhysicalCache<string>();
            var key = new CacheKey("ns", "touch");
            await cache.SetAsync(key, new NodeCacheEntry<string>("v", tags: TestTags), DefaultCancellationToken);

            await RaceTouchAsync(cache, key);

            var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
            Assert.Null(entry);
        }
    }

    /// <summary>Try-add stores entry tags so reads and snapshot capture observe them (issue #421).</summary>
    [Fact]
    public async Task TryAddPreservesTags()
    {
        var cache = new PhysicalCache<string>();
        _ = await cache.TryAddAsync(new CacheKey("ns", "added"), new NodeCacheEntry<string>("v", tags: TestTags), DefaultCancellationToken);

        var entry = await cache.GetEntryAsync(new CacheKey("ns", "added"), DefaultCancellationToken);
        Assert.NotNull(entry);
        AssertTagsEqual(TestTags, entry.Tags);
    }

    /// <summary>Update on a missing key returns false.</summary>
    [Fact]
    public async Task UpdateAsyncMissingKeyReturnsFalse()
    {
        var cache = new PhysicalCache<string>();
        Assert.False(await cache.UpdateAsync(new CacheKey("ns", "missing"), "v", DefaultCancellationToken));
    }

    /// <summary>Update replaces a live value and reports success.</summary>
    [Fact]
    public async Task UpdateAsyncReplacesLiveValue()
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "live");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "a", Version = 1 }, DefaultCancellationToken);

        Assert.True(await cache.UpdateAsync(key, "b", DefaultCancellationToken));
        var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
        Assert.Equal("b", entry!.Value);
    }

    /// <summary>Update with the same live value is a successful no-op.</summary>
    [Fact]
    public async Task UpdateAsyncSameValueIsNoOpSuccess()
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "same");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "same", Version = 1 }, DefaultCancellationToken);

        Assert.True(await cache.UpdateAsync(key, "same", DefaultCancellationToken));
    }

    /// <summary>Value-only update keeps the original entry tags (issue #421).</summary>
    [Fact]
    public async Task UpdateKeepsOriginalTags()
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "updated");
        await cache.SetAsync(key, new NodeCacheEntry<string>("old", tags: TestTags), DefaultCancellationToken);

        Assert.True(await cache.UpdateAsync(key, "new", DefaultCancellationToken));
        var entry = await cache.GetEntryAsync(key, DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("new", entry.Value);
        AssertTagsEqual(TestTags, entry.Tags);
    }

    /// <summary>Update on an expired key removes it and returns false.</summary>
    [Fact]
    public async Task UpdateRemovesExpiredEntryReturnsFalse()
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
            DefaultCancellationToken);

        time.Advance(TimeSpan.FromMinutes(2));

        Assert.False(await cache.UpdateAsync(key, "new", DefaultCancellationToken));
        Assert.Null(await cache.GetEntryAsync(key, DefaultCancellationToken));
    }

    /// <summary>UpdateAsync must not report success on a key that is concurrently reclaimed.</summary>
    [Fact]
    [Trait(StressTrait.TraitName, StressTrait.TraitValue)]
    public async Task UpdateNoFalseSuccessWhenReclaimed()
    {
        var cache = new PhysicalCache<string>();
        var key = new CacheKey("ns", "race438");
        var st = new RaceState(cache, key, DefaultCancellationToken);

        // Seed the key before either concurrent task starts so the reclaimer races against a present
        // entry from the first iteration instead of finishing before the updater writes the key.
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "v" }, DefaultCancellationToken);

        var updater = Task.Factory.StartNew(UpdateRaceOp, st, DefaultCancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        var reclaimer = Task.Factory.StartNew(RemoveOp, st, DefaultCancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();

        await Task.WhenAll(updater, reclaimer);

        // A correct implementation reports success only when the entry is genuinely present at the CAS, so
        // false positives stay at ~0; the small tolerance absorbs the rare legitimate post-update reclaim
        // window on the fixed code path (the buggy equals fast-path yields dozens-to-hundreds).
        Assert.True(st.FalsePositives <= 3, $"UpdateAsync reported success on a concurrently reclaimed key {st.FalsePositives} time(s).");
    }

    private static void AssertTagsEqual(FrozenDictionary<string, string> expected, FrozenDictionary<string, string>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Count, actual.Count);
        foreach (var pair in expected)
            Assert.True(actual.TryGetValue(pair.Key, out var value) && string.Equals(value, pair.Value, StringComparison.Ordinal), $"tag '{pair.Key}' missing or mismatched");
    }

    private static Task RaceRemoveExpAsync(PhysicalCache<string> cache, CacheKey key)
    {
        var st = new RaceState(cache, key, DefaultCancellationToken);
        var op = Task.Factory.StartNew(RemoveExpOp, st, DefaultCancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        var rem = Task.Factory.StartNew(RemoveOp, st, DefaultCancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        return Task.WhenAll(op, rem);
    }

    private static Task RaceTouchAsync(PhysicalCache<string> cache, CacheKey key)
    {
        var st = new RaceState(cache, key, DefaultCancellationToken);
        var op = Task.Factory.StartNew(TouchOp, st, DefaultCancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        var rem = Task.Factory.StartNew(RemoveOp, st, DefaultCancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        return Task.WhenAll(op, rem);
    }

    private static Task RaceTouchRecAsync(PhysicalCache<string> cache, CacheKey key)
    {
        var st = new RaceState(cache, key, DefaultCancellationToken);
        var op = Task.Factory.StartNew(TouchRecOp, st, DefaultCancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        var rem = Task.Factory.StartNew(RemoveOp, st, DefaultCancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
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

        public CacheKey Key { get; }

        public int FalsePositives { get; set; }
    }
}
