using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Golden payload sizes for <see cref="CacheEntrySizeEstimator{T}" /> type arms.</summary>
[Immutable]
public sealed class CacheEntrySizeEstimatorTests : ServerUnitTestBase
{
    private const string CacheName = "orders";
    private const string Key = "item";

    /// <summary>Null payloads cost the base overhead only (96 + 6 + 4 + 8).</summary>
    [Fact]
    public void EstimateBytesNullIsBaseOverhead()
    {
        var estimator = new CacheEntrySizeEstimator<object?>();

        Assert.Equal(114, estimator.EstimateBytes(new CacheKey(CacheName, Key), new NodeCacheEntry<object?> { Value = null, Version = 1 }, false));
    }

    /// <summary>Each payload kind maps to its documented golden size.</summary>
    [Fact]
    public void EstimateBytesMatchesGoldenPerType()
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
            ("ab", 116),
            (new byte[] { 1, 2, 3 }, 117),
            (true, 115),
            ('x', 116),
            (sbyteValue, 115),
            (byteValue, 115),
            (shortValue, 116),
            (ushortValue, 116),
            (5, 118),
            (uintValue, 118),
            (floatValue, 118),
            (8L, 122),
            (ulongValue, 122),
            (10d, 122),
            (11m, 130),
            (new object(), 242),
        };

        var estimator = new CacheEntrySizeEstimator<object?>();
        foreach (var (value, expected) in cases)
            Assert.Equal(expected, estimator.EstimateBytes(new CacheKey(CacheName, Key), new NodeCacheEntry<object?> { Value = value, Version = 1 }, false));
    }
}
