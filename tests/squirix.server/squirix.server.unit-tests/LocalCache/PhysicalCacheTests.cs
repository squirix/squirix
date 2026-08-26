using System;
using System.Collections.Frozen;
using System.Collections.Generic;
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
    private static FrozenDictionary<string, string> TestTags { get; } = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["tenant"] = "t1",
        ["origin"] = "repro",
    }.ToFrozenDictionary();

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

    /// <summary>Update on a missing key returns false.</summary>
    [Fact]
    public async Task UpdateAsyncMissingKeyReturnsFalse()
    {
        var cache = new PhysicalCache<string>();
        Assert.False(await cache.UpdateAsync(new CacheKey("ns", "missing"), "v", DefaultCancellationToken));
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

    /// <summary>Durable-recovery insert restores entry tags after restart/recovery (issue #421).</summary>
    [Fact]
    public async Task DurableRecoveryInsertPreservesTags()
    {
        var cache = new PhysicalCache<string>();
        await cache.InsertForDurableRecoveryAsync(new CacheKey("ns", "recovered"), new NodeCacheEntry<string>("v", tags: TestTags), DefaultCancellationToken);

        var entry = await cache.GetEntryAsync(new CacheKey("ns", "recovered"), DefaultCancellationToken);
        Assert.NotNull(entry);
        AssertTagsEqual(TestTags, entry.Tags);
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

    private static void AssertTagsEqual(FrozenDictionary<string, string> expected, FrozenDictionary<string, string>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Count, actual.Count);
        foreach (var pair in expected)
            Assert.True(actual.TryGetValue(pair.Key, out var value) && string.Equals(value, pair.Value, StringComparison.Ordinal), $"tag '{pair.Key}' missing or mismatched");
    }
}
