using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

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
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureCarriesTagsAndSkipsExpired(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(time);
        await cache.SetAsync(new CacheKey("ns", "tagged"), new NodeCacheEntry<string>("v", tags: Tags), cancellationToken);
        await cache.SetAsync(new CacheKey("ns", "expiring"), new NodeCacheEntry<string>("e", 1, time.GetUtcNow().UtcDateTime.AddSeconds(1), tags: Tags), cancellationToken);

        time.Advance(TimeSpan.FromSeconds(2));

        var target = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>();
        await new LocalCacheSnapshotCapture<string>(cache).CaptureEntriesAsync(target, time.GetUtcNow().UtcDateTime, cancellationToken);

        var (capturedKey, capturedEntry) = await Assert.That(target).HasSingleItem();
        _ = await Assert.That(capturedKey).IsEqualTo(new CacheKey("ns", "tagged"));
        await AssertTagsEqual(Tags, capturedEntry.Tags);
    }

    private static async Task AssertTagsEqual(FrozenDictionary<string, string> expected, FrozenDictionary<string, string>? actual)
    {
        _ = await Assert.That(actual).IsNotNull();
        _ = await Assert.That(actual.Count).IsEqualTo(expected.Count);
        foreach (var pair in expected)
            _ = await Assert.That(actual.TryGetValue(pair.Key, out var value) && string.Equals(value, pair.Value, StringComparison.Ordinal)).IsTrue().Because($"tag '{pair.Key}' missing or mismatched");
    }
}
