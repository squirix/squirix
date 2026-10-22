using System;
using System.Text.Json;
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

/// <summary>Golden payload sizes for <see cref="CacheEntrySizeEstimator{T}" /> type arms.</summary>
[Immutable]
public sealed class CacheEntrySizeEstimatorTests : ServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "item";

    /// <summary>
    /// An entry with a deadline, absolute or relative, costs the deadline index share (two heap slots of 40) more than the same entry without
    /// one, for typed and for JSON values; the entry node keeps a deadline field either way.
    /// </summary>
    [Test]
    public async Task DeadlineAddsIndexShare()
    {
        var key = new CacheKey(CacheName, Key);
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var typed = new CacheEntrySizeEstimator<object?>();
        var withoutDeadline = typed.EstimateBytes(key, new NodeCacheEntry<object?>("ab"), false);

        _ = await Assert.That(typed.EstimateBytes(key, new NodeCacheEntry<object?>("ab", 1, deadline), false) - withoutDeadline).IsEqualTo(80L);
        _ = await Assert.That(typed.EstimateBytes(key, new NodeCacheEntry<object?>("ab", 1, null, TimeSpan.FromMinutes(1)), false) - withoutDeadline).IsEqualTo(80L);

        // The encoded length of a JSON entry holds its 8-byte deadline field; the index share comes on top.
        var json = JsonElement.Parse("""{"a":[1,"b"]}""");
        var objects = new ObjectCacheEntrySizeEstimator();
        var jsonWithoutDeadline = objects.EstimateBytes(key, new NodeCacheEntry<object?>(json), false);
        _ = await Assert.That(objects.EstimateBytes(key, new NodeCacheEntry<object?>(json, 1, deadline), false) - jsonWithoutDeadline).IsEqualTo(8L + 80L);
    }

    /// <summary>Each payload kind maps to its documented golden size.</summary>
    [Test]
    public async Task EstimateBytesMatchesGoldenPerType()
    {
        const sbyte sbyteValue = 1;
        const byte byteValue = 2;
        const short shortValue = 3;
        const ushort ushortValue = 4;
        const uint uintValue = 6;
        const float floatValue = 7f;
        const ulong ulongValue = 9;
        var cases = new (object? Value, long Expected)[]
        {
            ("ab", 172),
            (new byte[] { 1, 2, 3 }, 173),
            (true, 171),
            ('x', 172),
            (sbyteValue, 178),
            (byteValue, 178),
            (shortValue, 178),
            (ushortValue, 178),
            (5, 178),
            (uintValue, 178),
            (floatValue, 178),
            (8L, 178),
            (ulongValue, 178),
            (10d, 178),
            (11m, 186),
            (new object(), 298),
        };

        var estimator = new CacheEntrySizeEstimator<object?>();
        foreach (var (value, expected) in cases)
            _ = await Assert.That(estimator.EstimateBytes(new CacheKey(CacheName, Key), new NodeCacheEntry<object?> { Value = value, Version = 1 }, false)).IsEqualTo(expected);
    }

    /// <summary>Null payloads cost the base overhead only (112 for the entry node, 32 for the key object, 16 for bucket slots, + 6 + 4).</summary>
    [Test]
    public async Task EstimateBytesNullIsBaseOverhead()
    {
        var estimator = new CacheEntrySizeEstimator<object?>();

        _ = await Assert.That(estimator.EstimateBytes(new CacheKey(CacheName, Key), new NodeCacheEntry<object?> { Value = null, Version = 1 }, false)).IsEqualTo(170);
    }
}
