using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Unit tests for <see cref="ObjectCacheEntrySizeEstimator" />.</summary>
public sealed class ObjectCacheEntrySizeEstimatorTests : UnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "item";

    /// <summary>Complex object payloads use encoded entry size instead of the 128-byte fallback.</summary>
    [Fact]
    public void UnknownObjectPayloadUsesEncodedEntrySize()
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var typedEstimator = new CacheEntrySizeEstimator<object?>();
        var key = new CacheKey(CacheName, Key);
        var entry = new NodeCacheEntry<object?> { Value = new { Data = new string('x', 16_384) }, Version = 1 };

        var estimated = estimator.EstimateBytes(key, entry, false);
        var typedFallback = typedEstimator.EstimateBytes(key, entry, false);

        Assert.True(estimated > typedFallback);
        Assert.False(estimator.HasUnknownPayloadMagnitude(entry, false));
    }
}
