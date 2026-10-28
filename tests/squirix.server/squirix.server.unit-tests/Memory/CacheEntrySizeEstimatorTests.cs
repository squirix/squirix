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
    /// An entry with a deadline, absolute or relative, costs the deadline index share (two heap slots of 32) more than the same entry without
    /// one, for typed and for JSON values; the entry node keeps a deadline field either way.
    /// </summary>
    [Test]
    public async Task DeadlineAddsIndexShare()
    {
        var key = new CacheKey(CacheName, Key);
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var typed = new CacheEntrySizeEstimator<object?>();
        var withoutDeadline = typed.EstimateBytes(key, new NodeCacheEntry<object?>("ab"), false);

        _ = await Assert.That(typed.EstimateBytes(key, new NodeCacheEntry<object?>("ab", 1, deadline), false) - withoutDeadline).IsEqualTo(64L);
        _ = await Assert.That(typed.EstimateBytes(key, new NodeCacheEntry<object?>("ab", 1, null, TimeSpan.FromMinutes(1)), false) - withoutDeadline).IsEqualTo(64L);

        // A JSON value weighs its bytes, so the deadline adds the index share alone.
        var json = JsonElement.Parse("""{"a":[1,"b"]}""");
        var objects = new ObjectCacheEntrySizeEstimator();
        var jsonWithoutDeadline = objects.EstimateBytes(key, new NodeCacheEntry<object?>(json), false);
        _ = await Assert.That(objects.EstimateBytes(key, new NodeCacheEntry<object?>(json, 1, deadline), false) - jsonWithoutDeadline).IsEqualTo(64L);
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
            ("ab", 148),
            (new byte[] { 1, 2, 3 }, 149),
            (true, 147),
            ('x', 148),
            (sbyteValue, 154),
            (byteValue, 154),
            (shortValue, 154),
            (ushortValue, 154),
            (5, 154),
            (uintValue, 154),
            (floatValue, 154),
            (8L, 154),
            (ulongValue, 154),
            (10d, 154),
            (11m, 162),
            (JsonValueKit.Create("""{"a":1}"""), 209),
            (new object(), 274),
        };

        var estimator = new CacheEntrySizeEstimator<object?>();
        foreach (var (value, expected) in cases)
            _ = await Assert.That(estimator.EstimateBytes(new CacheKey(CacheName, Key), new NodeCacheEntry<object?> { Value = value, Version = 1 }, false)).IsEqualTo(expected);
    }

    /// <summary>Null payloads cost the base overhead only (120 for the entry node with its key, 16 for bucket slots, + 6 + 4).</summary>
    [Test]
    public async Task EstimateBytesNullIsBaseOverhead()
    {
        var estimator = new CacheEntrySizeEstimator<object?>();

        _ = await Assert.That(estimator.EstimateBytes(new CacheKey(CacheName, Key), new NodeCacheEntry<object?> { Value = null, Version = 1 }, false)).IsEqualTo(146);
    }
}
