using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>
/// The snapshot capture bridge must carry entry tags from the live store into snapshot-ready
/// entries; losing them here silently drops user metadata on every snapshot-based recovery
/// while journal-only replay preserves it. See issue #421.
/// </summary>
[Immutable]
public sealed class SnapshotCaptureBridgeTests : ServerUnitTestBase
{
    private static FrozenDictionary<string, string> Tags { get; } = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["tenant"] = "t1",
    }.ToFrozenDictionary();

    /// <summary>Captured entries keep their tags and expired entries stay excluded.</summary>
    [Fact]
    public async Task CaptureCarriesTagsAndSkipsExpired()
    {
        var time = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(time);
        await cache.SetAsync(new CacheKey("ns", "tagged"), new NodeCacheEntry<string>("v", tags: Tags), DefaultCancellationToken);
        await cache.SetAsync(
            new CacheKey("ns", "expiring"),
            new NodeCacheEntry<string>("e", 1, time.GetUtcNow().UtcDateTime.AddSeconds(1), tags: Tags),
            DefaultCancellationToken);

        time.Advance(TimeSpan.FromSeconds(2));

        var target = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>();
        await new LocalCacheSnapshotCapture<string>(cache).CaptureEntriesAsync(target, time.GetUtcNow().UtcDateTime, DefaultCancellationToken);

        var (capturedKey, capturedEntry) = Assert.Single(target);
        Assert.Equal(new CacheKey("ns", "tagged"), capturedKey);
        AssertTagsEqual(Tags, capturedEntry.Tags);
    }

    private static void AssertTagsEqual(FrozenDictionary<string, string> expected, FrozenDictionary<string, string>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Count, actual.Count);
        foreach (var pair in expected)
            Assert.True(actual.TryGetValue(pair.Key, out var value) && string.Equals(value, pair.Value, StringComparison.Ordinal), $"tag '{pair.Key}' missing or mismatched");
    }
}
