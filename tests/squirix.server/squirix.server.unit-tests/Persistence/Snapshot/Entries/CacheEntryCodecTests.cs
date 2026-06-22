using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot.Entries;

/// <summary>Unit tests for <see cref="CacheEntryCodec" />.</summary>
public sealed class CacheEntryCodecTests : UnitTestBase
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
    public void RoundTripsPrimitiveValues(object? value)
    {
        var entry = new CacheEntry<object?> { Value = value, Version = 7 };
        var buffer = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, buffer);

        Assert.True(CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out var bytesRead));
        Assert.Equal(buffer.Length, bytesRead);
        Assert.True(ValueEquals(value, roundTrip!.Value));
        Assert.Equal(7, roundTrip.Version);
    }

    /// <summary>Metadata and tags round-trip through the codec.</summary>
    [Fact]
    public void RoundTripsMetadataAndTags()
    {
        var entry = new CacheEntry<object?>
        {
            Value = "payload",
            ExpiresUtc = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc),
            Expiration = TimeSpan.FromMinutes(5),
            Version = 3,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["region"] = "west" }.ToFrozenDictionary(StringComparer.Ordinal),
        };
        var buffer = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, buffer);

        Assert.True(CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out _));
        Assert.Equal(entry.Value, roundTrip!.Value);
        Assert.Equal(entry.ExpiresUtc, roundTrip.ExpiresUtc);
        Assert.Equal(entry.Expiration, roundTrip.Expiration);
        Assert.Equal(entry.Version, roundTrip.Version);
        Assert.Equal("west", roundTrip.Tags?["region"]);
    }

    private static bool ValueEquals(object? expected, object? actual) =>
        expected switch
        {
            int i when actual is long l => i == l,
            int i when actual is int j => i == j,
            long l when actual is long r => l == r,
            double d when actual is double r => Math.Abs(d - r) < 0.0001,
            _ => Equals(expected, actual),
        };
}
