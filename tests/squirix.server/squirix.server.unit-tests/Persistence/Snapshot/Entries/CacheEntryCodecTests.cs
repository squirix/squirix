using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Text.Json;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.TestKit.Testing;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot.Entries;

/// <summary>Unit tests for <see cref="CacheEntryCodec" />.</summary>
public sealed class CacheEntryCodecTests : UnitTestBase
{
    /// <summary>Complex JSON values round-trip through the codec as JsonElement trees.</summary>
    [Fact]
    public void RoundTripsJsonElementValue()
    {
        using var document = JsonDocument.Parse("""{"id":42,"tags":["a","b"]}""");
        var entry = new CacheEntry<object?> { Value = document.RootElement.Clone(), Version = 2 };
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
        var entry = new CacheEntry<object?>
        {
            Value = "payload",
            ExpiresUtc = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc),
            Expiration = TimeSpan.FromMinutes(5),
            Version = 3,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["region"] = "west" }.ToFrozenDictionary(StringComparer.Ordinal),
        };
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

    private static bool ValueEquals(object? expected, object? actual) => expected switch
    {
        int i when actual is long l => i == l,
        int i when actual is int j => i == j,
        long l when actual is long r => l == r,
        double d when actual is double r => Math.Abs(d - r) < 0.0001,
        _ => Equals(expected, actual),
    };
}
