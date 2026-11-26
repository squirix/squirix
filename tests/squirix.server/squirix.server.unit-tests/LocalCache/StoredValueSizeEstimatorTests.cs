using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

/// <summary>Unit tests for <see cref="CacheEntrySizeEstimator{T}" />.</summary>
[Immutable]
public sealed class StoredValueSizeEstimatorTests : ServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "item";

    /// <summary>JSON is measured from the encoded bytes already owned by the server.</summary>
    [Test]
    public async Task JsonValueWeighsItsBytes()
    {
        var estimator = new CacheEntrySizeEstimator<StoredValue>();
        var key = new CacheKey(CacheName, Key);
        var small = new NodeCacheEntry<StoredValue>(StoredValue.FromJson(System.Text.Encoding.UTF8.GetBytes("[]")));
        var large = new NodeCacheEntry<StoredValue>(StoredValue.FromJson(Encoding.UTF8.GetBytes("[" + new string('9', 5000) + "]")));

        _ = await Assert.That(estimator.EstimateBytes(key, large, false) - estimator.EstimateBytes(key, small, false)).IsEqualTo(5000L);
        _ = await Assert.That(estimator.MeasureValue(small.Value)).IsEqualTo(2L + JsonUtf8Value.OverheadBytes);
        _ = await Assert.That(estimator.HasUnknownPayloadMagnitude(large, false)).IsFalse();
    }

    /// <summary>Memory usage counts the UTF-8 bytes retained for text.</summary>
    [Test]
    public async Task TextCountsTheRetainedRepresentation()
    {
        var estimator = new CacheEntrySizeEstimator<StoredValue>();
        var key = new CacheKey(CacheName, Key);
        var entry = new NodeCacheEntry<StoredValue>(StoredValue.FromText(new string('x', 4096) + "界😀"));
        var empty = estimator.EstimateBytes(key, new NodeCacheEntry<StoredValue>(default), false);

        _ = await Assert.That(estimator.MeasureValue(entry.Value)).IsEqualTo(4103L);
        _ = await Assert.That(estimator.EstimateBytes(key, entry, false) - empty).IsEqualTo(4103L);
    }
}
