using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Internal;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers ProtoEx object and typed primitive mapping arms.</summary>
public sealed class ProtoExTests
{
    /// <summary>Struct-wrapped protobuf values deserialize to untyped objects.</summary>
    /// <param name="kind">Value kind to wrap.</param>
    [Theory]
    [InlineData("string")]
    [InlineData("bool")]
    [InlineData("number")]
    [InlineData("null")]
    public async Task FromCacheValueAsyncObjectReadsWrappedValues(string kind)
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
                Fields = { ["value"] = wrapped },
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

    /// <summary>Exact typed primitive wire forms decode without struct wrapping.</summary>
    /// <param name="kind">Wire kind under test.</param>
    [Theory]
    [InlineData("string")]
    [InlineData("bool")]
    [InlineData("int")]
    [InlineData("long")]
    [InlineData("double")]
    [InlineData("null")]
    public async Task FromCacheValueAsyncExactPrimitives(string kind)
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
    public async Task FromCacheValueAsyncMismatchedPrimitiveUsesWrapper()
    {
        var serializer = new SystemTextJsonSerializer();
        var wire = new CacheValue { Int32Value = 5 };

        Assert.Equal(5d, await ProtoEx.FromCacheValueAsync<double>(wire, serializer));
    }

    /// <summary>Entry mapping round-trips typed values and expiration metadata.</summary>
    [Fact]
    public async Task MapEntryRoundTripsTypedValueAndExpiration()
    {
        var serializer = new SystemTextJsonSerializer();
        var entry = new CacheEntry<string>
        {
            Value = "payload",
            ExpiresUtc = new System.DateTime(2026, 8, 1, 10, 0, 0, System.DateTimeKind.Utc),
            Expiration = System.TimeSpan.FromMinutes(2),
        };

        var wire = ProtoEx.MapEntryToProto(entry, serializer);
        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<string>(wire, serializer);

        Assert.Equal("payload", roundTrip.Value);
        Assert.Equal(entry.ExpiresUtc, roundTrip.ExpiresUtc);
        Assert.Equal(entry.Expiration, roundTrip.Expiration);
    }

    /// <summary>JsonElement values round-trip through entry mapping.</summary>
    [Fact]
    public async Task MapEntryRoundTripsJsonElement()
    {
        var serializer = new SystemTextJsonSerializer();
        using var document = JsonDocument.Parse("""{"x":1,"y":[true,null]}""");
        var entry = new CacheEntry<JsonElement> { Value = document.RootElement.Clone() };

        var wire = ProtoEx.MapEntryToProto(entry, serializer);
        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<JsonElement>(wire, serializer);

        Assert.Equal(1, roundTrip.Value.GetProperty("x").GetInt32());
        Assert.Equal(2, roundTrip.Value.GetProperty("y").GetArrayLength());
    }
}
