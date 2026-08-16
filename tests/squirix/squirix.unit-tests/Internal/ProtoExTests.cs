using System;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers ProtoEx object and typed primitive mapping arms.</summary>
[Immutable]
public sealed class ProtoExTests
{
    /// <summary>Exact typed primitive wire forms decode without struct wrapping.</summary>
    /// <param name="kind">Wire kind under test.</param>
    [Theory]
    [InlineData("string")]
    [InlineData("bool")]
    [InlineData("int")]
    [InlineData("long")]
    [InlineData("double")]
    [InlineData("null")]
    public async Task FromCacheValueAsyncExactPrimitivesAsync(string kind)
    {
        var serializer = new SystemTextJsonSerializer();
        var wire = kind switch
        {
            "string" => new CacheValue { StringValue = "hello" },
            "bool" => new CacheValue { BoolValue = true },
            "int" => new CacheValue { Int32Value = 11 },
            "long" => new CacheValue { Int64Value = 22L },
            "double" => new CacheValue { DoubleValue = 3.25d },
            _ => new CacheValue { NullValue = NullValue.NullValue },
        };

        switch (kind)
        {
            case "string":
                Assert.Equal("hello", await ProtoEx.FromCacheValueAsync<string>(wire, serializer));
                break;
            case "bool":
                Assert.True(await ProtoEx.FromCacheValueAsync<bool>(wire, serializer));
                break;
            case "int":
                Assert.Equal(11, await ProtoEx.FromCacheValueAsync<int>(wire, serializer));
                break;
            case "long":
                Assert.Equal(22L, await ProtoEx.FromCacheValueAsync<long>(wire, serializer));
                break;
            case "double":
                Assert.Equal(3.25d, await ProtoEx.FromCacheValueAsync<double>(wire, serializer));
                break;
            default:
                Assert.Null(await ProtoEx.FromCacheValueAsync<string>(wire, serializer));
                break;
        }
    }

    /// <summary>Mismatched primitive wire falls back through the struct wrapper path.</summary>
    [Fact]
    public async Task FromCacheValueMismatchedPrimitiveUsesWrapperAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        var wire = new CacheValue { Int32Value = 5 };

        Assert.Equal(5d, await ProtoEx.FromCacheValueAsync<double>(wire, serializer));
    }

    /// <summary>Struct-wrapped protobuf values deserialize to untyped objects.</summary>
    /// <param name="kind">Value kind to wrap.</param>
    [Theory]
    [InlineData("string")]
    [InlineData("bool")]
    [InlineData("number")]
    [InlineData("null")]
    public async Task FromCacheValueAsyncObjectReadsWrappedValuesAsync(string kind)
    {
        var serializer = new SystemTextJsonSerializer();
        var wrapped = kind switch
        {
            "string" => Value.ForString("hello"),
            "bool" => Value.ForBool(true),
            "number" => Value.ForNumber(3.5),
            _ => Value.ForNull(),
        };

        var cacheValue = new CacheValue
        {
            StructValue = new Struct
            {
                Fields = { ["\0squirix:scalar"] = wrapped },
            },
        };

        var result = await ProtoEx.FromCacheValueAsync<object>(cacheValue, serializer);
        switch (kind)
        {
            case "string":
                Assert.Equal("hello", result);
                break;
            case "bool":
                Assert.True(Assert.IsType<bool>(result));
                break;
            case "number":
                Assert.Equal(3.5d, result);
                break;
            default:
                Assert.Null(result);
                break;
        }
    }

    /// <summary>JsonElement values round-trip through entry mapping.</summary>
    [Fact]
    public async Task MapEntryRoundTripsJsonElementAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        using var document = JsonDocument.Parse("""{"x":1,"y":[true,null]}""");
        var entry = new CacheEntry<JsonElement> { Value = document.RootElement.Clone() };

        var wire = ProtoEx.MapEntryToProto(entry, serializer);
        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<JsonElement>(wire, serializer);

        Assert.Equal(1, roundTrip.Value.GetProperty("x").GetInt32());
        var y = roundTrip.Value.GetProperty("y");
        Assert.Equal(2, y.GetArrayLength());
        Assert.True(y[0].GetBoolean());
        Assert.Equal(JsonValueKind.Null, y[1].ValueKind);
    }

    /// <summary>Negative zero JSON values preserve sign through protobuf round-trip.</summary>
    [Fact]
    public async Task NegativeZeroPreservesSignAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        using var document = JsonDocument.Parse("-0.0");
        var entry = new CacheEntry<JsonElement> { Value = document.RootElement.Clone() };

        var wire = ProtoEx.MapEntryToProto(entry, serializer);
        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<JsonElement>(wire, serializer);

        Assert.Equal(JsonValueKind.Number, roundTrip.Value.ValueKind);
        Assert.Equal(0.0, roundTrip.Value.GetDouble());
        Assert.Equal("-0", roundTrip.Value.GetRawText());
    }

    /// <summary>Entry mapping round-trips typed values and expiration metadata.</summary>
    [Fact]
    public async Task MapEntryRoundTripsTypedValueAndExpirationAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        var entry = new CacheEntry<string>
        {
            Value = "payload",
            ExpiresUtc = new DateTime(2026, 8, 1, 10, 0, 0, DateTimeKind.Utc),
            Expiration = TimeSpan.FromMinutes(2),
        };

        var wire = ProtoEx.MapEntryToProto(entry, serializer);
        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<string>(wire, serializer);

        Assert.Equal("payload", roundTrip.Value);
        Assert.Equal(entry.ExpiresUtc, roundTrip.ExpiresUtc);
        Assert.Equal(entry.Expiration, roundTrip.Expiration);
    }
}
