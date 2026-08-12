using System;
using System.IO;
using System.Text.Json;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for <see cref="CacheEntryCodec" />.</summary>
public sealed class CacheEntryCodecTests : ServerUnitTestBase
{
    /// <summary>Primitive values round-trip through the codec.</summary>
    /// <param name="value">Value under test.</param>
    [Theory]
    [InlineData(null)]
    [InlineData(true)]
    [InlineData(false)]
    [InlineData("hello")]
    [InlineData(42)]
    [InlineData(42L)]
    [InlineData(3.14d)]
    public static void RoundTripsPrimitiveValues(object? value)
    {
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 7 };
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        BufferKit.WithBuffer(
            length,
            (entry, value),
            static (ctx, buffer) =>
            {
                CacheEntryCodec.Write(ctx.entry, buffer);
                Assert.True(CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out var bytesRead));
                Assert.Equal(buffer.Length, bytesRead);
                Assert.True(ValueEquals(ctx.value, roundTrip!.Value));
                Assert.Equal(7, roundTrip.Version);
            });
    }

    /// <summary>Decimal and byte[] values round-trip through the codec.</summary>
    [Fact]
    public void RoundTripsDecimalAndByteArrayValues()
    {
        RoundTripValue(12.5m);
        byte[] payload = [9, 8, 7];
        RoundTripValue(payload);
    }

    /// <summary>Complex JSON values round-trip through the codec as JsonElement trees.</summary>
    [Fact]
    public void RoundTripsJsonElementValue()
    {
        using var document = JsonDocument.Parse("""{"id":42,"tags":["a","b"]}""");
        var entry = new NodeCacheEntry<object?> { Value = document.RootElement.Clone(), Version = 2 };
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        BufferKit.WithBuffer(
            length,
            entry,
            static (e, buffer) =>
            {
                CacheEntryCodec.Write(e, buffer);
                Assert.True(CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out _));
                var element = Assert.IsType<JsonElement>(roundTrip!.Value);
                Assert.Equal(42, element.GetProperty("id").GetInt32());
                Assert.Equal(2, element.GetProperty("tags").GetArrayLength());
            });
    }

    /// <summary>Metadata and tags round-trip through the codec.</summary>
    [Fact]
    public void RoundTripsMetadataAndTags()
    {
        var entry = new NodeCacheEntry<object?>("payload", 3, new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc), TimeSpan.FromMinutes(5), EntryTagsKit.RegionWest);
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        BufferKit.WithBuffer(
            length,
            entry,
            static (e, buffer) =>
            {
                CacheEntryCodec.Write(e, buffer);
                Assert.True(CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out _));
                Assert.Equal(e.Value, roundTrip!.Value);
                Assert.Equal(e.ExpiresUtc, roundTrip.ExpiresUtc);
                Assert.Equal(e.Expiration, roundTrip.Expiration);
                Assert.Equal(e.Version, roundTrip.Version);
                Assert.Equal("west", roundTrip.Tags?["region"]);
            });
    }

    /// <summary>Numeric and JsonElement coercions used by typed journal reads succeed.</summary>
    [Fact]
    public void TryMapEntryCoercesNumericAndJsonElementValues()
    {
        Assert.True(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>(42L), out var asInt));
        Assert.Equal(42, asInt!.Value);

        Assert.True(CacheEntryCodec.TryMapEntry<long>(new NodeCacheEntry<object?>(99L), out var asLong));
        Assert.Equal(99L, asLong!.Value);

        Assert.True(CacheEntryCodec.TryMapEntry<float>(new NodeCacheEntry<object?>(1.5d), out var asFloat));
        Assert.Equal(1.5f, asFloat!.Value);

        Assert.True(CacheEntryCodec.TryMapEntry<double>(new NodeCacheEntry<object?>(2.5d), out var asDouble));
        Assert.Equal(2.5d, asDouble!.Value);

        using var document = JsonDocument.Parse("""{"k":1}""");
        Assert.True(CacheEntryCodec.TryMapEntry<JsonElement>(new NodeCacheEntry<object?>(document.RootElement.Clone()), out var asJson));
        Assert.Equal(1, asJson!.Value.GetProperty("k").GetInt32());

        Assert.False(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>("nope"), out _));
        Assert.True(CacheEntryCodec.TryMapEntry<string>(new NodeCacheEntry<object?>(null), out var asNull));
        Assert.Null(asNull!.Value);
    }

    /// <summary>TryRead fails on truncated envelopes.</summary>
    [Fact]
    public void TryReadReturnsFalseForTruncatedEnvelope()
    {
        Assert.False(CacheEntryCodec.TryRead<object?>([], out _, out var bytesRead));
        Assert.Equal(0, bytesRead);
    }

    /// <summary>Write rejects destinations that are too small for the encoded entry.</summary>
    [Fact]
    public void WriteThrowsWhenDestinationIsTooSmall()
    {
        var entry = new NodeCacheEntry<object?> { Value = "abc", Version = 1 };
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(
            entry,
            length - 1,
            static (e, tooSmall) =>
            {
                Span<byte> destination = stackalloc byte[tooSmall];
                CacheEntryCodec.Write(e, destination);
            });
    }

    /// <summary>ComputeEncodedLength rejects tag dictionaries exceeding ushort.MaxValue entries.</summary>
    [Fact]
    public void ComputeEncodedLengthRejectsExcessiveTagCount()
    {
        var tags = EntryTagsKit.CreateCount(65_536);
        var entry = new NodeCacheEntry<object?>(null, tags: tags);
        _ = Assert.Throws<InvalidDataException>(() => CacheEntryCodec.ComputeEncodedLength(entry));
    }

    private static void RoundTripValue(object? value)
    {
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 4 };
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        BufferKit.WithBuffer(
            length,
            (entry, value),
            static (ctx, buffer) =>
            {
                CacheEntryCodec.Write(ctx.entry, buffer);
                Assert.True(CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out _));
                if (ctx.value is byte[] expectedBytes)
                    Assert.Equal(expectedBytes, Assert.IsType<byte[]>(roundTrip!.Value));
                else
                    Assert.Equal(ctx.value, roundTrip!.Value);
            });
    }

    private static bool ValueEquals(object? expected, object? actual) => expected switch
    {
        int i when actual is long l => i == l,
        int i when actual is int j => i == j,
        long l when actual is long r => l == r,
        double d when actual is double r => Math.Abs(d - r) < 0.0001,
        _ => Equals(expected, actual),
    };
}
