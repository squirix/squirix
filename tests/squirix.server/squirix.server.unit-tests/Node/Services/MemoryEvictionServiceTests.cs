using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Tests cache reclamation under estimated memory pressure.</summary>
[Immutable]
public sealed class MemoryEvictionServiceTests : ServerUnitTestBase
{
    /// <summary>A pass preserves hot entries and stops below the high threshold.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MemorySweepRemovesOnlyEnoughColdEntries(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<object?>(TimeProvider.System, new ObjectCacheEntrySizeEstimator(), new EvictionOptions { EvictOnMemoryPressure = true });
        ILocalCacheStats stats = cache;
        var local = new ClientCache<object?>(cache, cache);
        cache.Set(CacheKey.Default("a"), new NodeCacheEntry<object?> { Value = new string('a', 512) }, cancellationToken);
        var one = stats.EstimatedBytes;
        using var sweep = new MemoryEvictionService(cache, new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = 2 * one }), local, null, TimeProvider.System, NullLogger<MemoryEvictionService>.Instance);
        await sweep.SweepAsync(cancellationToken);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);
        foreach (var key in new[] { "b", "c", "d" })
            cache.Set(CacheKey.Default(key), new NodeCacheEntry<object?> { Value = new string('b', 512) }, cancellationToken);

        _ = cache.GetValue(CacheKey.Default("a"), cancellationToken);
        await sweep.SweepAsync(cancellationToken);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);
        _ = await Assert.That(cache.GetValue(CacheKey.Default("a"), cancellationToken).Found).IsTrue();
    }
}
