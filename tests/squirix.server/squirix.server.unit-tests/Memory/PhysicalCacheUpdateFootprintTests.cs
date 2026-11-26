using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>An update records the estimate of the entry it leaves, whatever the value, deadline and tags of the entry.</summary>
[Immutable]
public sealed class PhysicalCacheUpdateFootprintTests : ServerUnitTestBase
{
    private static readonly CacheEntrySizeEstimator<StoredValue> Estimator = new();
    private static readonly CacheKey Key = new("ns", "k");
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>After an update the recorded bytes equal the full estimate of the stored entry.</summary>
    /// <param name="jsonValue">Whether the updated value is a JSON object rather than a string.</param>
    /// <param name="withDeadline">Whether the entry has a deadline.</param>
    /// <param name="withTags">Whether the entry has tags.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false, false)]
    [Arguments(false, true, false)]
    [Arguments(false, false, true)]
    [Arguments(false, true, true)]
    [Arguments(true, false, false)]
    [Arguments(true, true, false)]
    [Arguments(true, false, true)]
    [Arguments(true, true, true)]
    public async Task UpdateRecordsEstimateOfResultingEntry(bool jsonValue, bool withDeadline, bool withTags, CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<StoredValue>(new FakeTimeProvider(Start), Estimator);
        DateTime? expiresUtc = withDeadline ? Start.UtcDateTime.AddHours(1) : null;
        var tags = withTags ? new Dictionary<string, string>(StringComparer.Ordinal) { ["color"] = "blue", ["tier"] = "gold" }.ToFrozenDictionary(StringComparer.Ordinal) : null;
        cache.Set(Key, new NodeCacheEntry<StoredValue>(StoredValue.FromText("initial"), 1, expiresUtc, tags: tags), cancellationToken);

        var updated = jsonValue ? StoredValue.FromJson(System.Text.Encoding.UTF8.GetBytes("""{"name":"value","items":[1,2,3],"nested":{"flag":true}}""")) : StoredValue.FromText("a different value");
        var updatedHere = cache.Update(Key, updated, cancellationToken);

        var stored = cache.RawReader.GetEntryRaw(Key, cancellationToken);
        _ = await Assert.That(updatedHere).IsTrue();
        _ = await Assert.That(stored).IsNotNull();
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(Estimator.EstimateBytes(Key, new NodeCacheEntry<StoredValue>(updated, stored!.Version, expiresUtc, tags: tags), false));
    }

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;
}
