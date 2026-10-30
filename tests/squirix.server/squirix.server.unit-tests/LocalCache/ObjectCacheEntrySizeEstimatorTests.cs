using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.LocalCache;

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

    /// <summary>A structured value weighs its text plus a fixed overhead, whatever its shape, and the entry estimate adds the same to the key and node share.</summary>
    [Test]
    public async Task JsonValueWeighsItsBytes()
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var key = new CacheKey(CacheName, Key);
        var smallValue = JsonValueKit.Create("""{"a":1}""");
        var largeValue = JsonValueKit.Create("""{"a":""" + new string('9', 5_000) + "}");
        var small = new NodeCacheEntry<object?> { Value = smallValue, Version = 1 };
        var large = new NodeCacheEntry<object?> { Value = largeValue, Version = 1 };

        var delta = estimator.EstimateBytes(key, large, false) - estimator.EstimateBytes(key, small, false);

        _ = await Assert.That(delta).IsEqualTo(largeValue.Length - smallValue.Length);
        _ = await Assert.That(estimator.MeasureValue(smallValue)).IsEqualTo(smallValue.Length + JsonUtf8Value.OverheadBytes);
        _ = await Assert.That(estimator.HasUnknownPayloadMagnitude(small, false)).IsFalse();
    }

    /// <summary>An element, an object and the JSON value they are stored as weigh the same, so an entry weighs the same before and after a restart.</summary>
    [Test]
    public async Task NormalizedFormsWeighTheSame()
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var key = new CacheKey(CacheName, Key);
        var payload = new IdPayload { Id = 12 };
        var stored = JsonValueKit.Create("""{"Id":12}""");
        var element = JsonElement.Parse(Encoding.UTF8.GetString(stored.Bytes.Span));

        var asObject = estimator.EstimateBytes(key, new NodeCacheEntry<object?> { Value = payload, Version = 1 }, false);
        var asElement = estimator.EstimateBytes(key, new NodeCacheEntry<object?> { Value = element, Version = 1 }, false);
        var asValue = estimator.EstimateBytes(key, new NodeCacheEntry<object?> { Value = stored, Version = 1 }, false);

        _ = await Assert.That(asObject).IsEqualTo(asValue);
        _ = await Assert.That(asElement).IsEqualTo(asValue);
        _ = await Assert.That(estimator.EstimateBytesForEncodedLength(key, new NodeCacheEntry<object?> { Value = stored, Version = 1 }, 12_345)).IsEqualTo(asValue);
    }
}
