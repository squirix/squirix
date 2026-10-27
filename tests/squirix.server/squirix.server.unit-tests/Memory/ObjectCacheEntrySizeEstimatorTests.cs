using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for <see cref="ObjectCacheEntrySizeEstimator" />.</summary>
[Immutable]
public sealed class ObjectCacheEntrySizeEstimatorTests : ServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "item";

    /// <summary>Complex object payloads use encoded entry size instead of the 128-byte fallback.</summary>
    [Test]
    public async Task UnknownObjectPayloadUsesEncodedEntrySize()
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var typedEstimator = new CacheEntrySizeEstimator<object?>();
        var key = new CacheKey(CacheName, Key);
        var entry = new NodeCacheEntry<object?> { Value = new ObjectCacheDataPayload { Data = new string('x', 16_384) }, Version = 1 };

        var estimated = estimator.EstimateBytes(key, entry, false);
        var typedFallback = typedEstimator.EstimateBytes(key, entry, false);

        _ = await Assert.That(estimated > typedFallback).IsTrue();
        _ = await Assert.That(estimator.HasUnknownPayloadMagnitude(entry, false)).IsFalse();
    }
}
