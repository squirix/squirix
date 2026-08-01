using System;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Core;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Tests compact <see cref="CacheValue" /> gRPC scalar mapping.</summary>
public sealed class CacheValueGrpcMappingTests
{
    /// <summary>CLR <see cref="int" /> values use the dedicated int32 wire arm.</summary>
    [Fact]
    public void Int32EncodesAsInt32ValueWireForm()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(42);

        Assert.Equal(CacheValue.KindOneofCase.Int32Value, wire.KindCase);
        Assert.Equal(42, wire.Int32Value);
    }

    /// <summary>int32 wire values decode to typed <see cref="int" /> reads.</summary>
    [Fact]
    public async Task Int32ValueRoundTripsAsInt()
    {
        var wire = new CacheValue { Int32Value = 7 };

        Assert.Equal(7, await ServerProtoEx.MapCacheValueAsync<int>(wire));
    }

    /// <summary>CLR <see cref="long" /> values outside int32 range keep the int64 wire arm.</summary>
    [Fact]
    public void Int64EncodesAsInt64ValueWireForm()
    {
        const long value = int.MaxValue + 1L;
        var wire = ServerProtoEx.CacheValueToGrpcValue(value);

        Assert.Equal(CacheValue.KindOneofCase.Int64Value, wire.KindCase);
        Assert.Equal(value, wire.Int64Value);
    }

    /// <summary>Exact primitive wire forms decode without struct wrapping.</summary>
    /// <param name="kind">Wire kind under test.</param>
    [Theory]
    [InlineData("string")]
    [InlineData("bool")]
    [InlineData("long")]
    [InlineData("double")]
    [InlineData("null")]
    public async Task ExactPrimitiveWireFormsDecode(string kind)
    {
        var wire = kind switch
        {
            "string" => new CacheValue { StringValue = "hello" },
            "bool" => new CacheValue { BoolValue = true },
            "long" => new CacheValue { Int64Value = 99L },
            "double" => new CacheValue { DoubleValue = 1.5d },
            _ => new CacheValue { NullValue = NullValue.NullValue },
        };

        switch (kind)
        {
            case "string":
                Assert.Equal("hello", await ServerProtoEx.MapCacheValueAsync<string>(wire));
                break;
            case "bool":
                Assert.True(await ServerProtoEx.MapCacheValueAsync<bool>(wire));
                break;
            case "long":
                Assert.Equal(99L, await ServerProtoEx.MapCacheValueAsync<long>(wire));
                break;
            case "double":
                Assert.Equal(1.5d, await ServerProtoEx.MapCacheValueAsync<double>(wire));
                break;
            default:
                Assert.Null(await ServerProtoEx.MapCacheValueAsync<string>(wire));
                break;
        }
    }

    /// <summary>Primitive wire with a mismatched CLR type falls back through struct wrapping.</summary>
    [Fact]
    public async Task MismatchedPrimitiveWireFallsBackThroughStructWrap()
    {
        var wire = new CacheValue { Int64Value = 42L };

        Assert.Equal(42, await ServerProtoEx.MapCacheValueAsync<int>(wire));
    }

    /// <summary>Struct-wrapped values decode for object and typed targets.</summary>
    [Fact]
    public async Task StructWrappedValuesDecode()
    {
        var wire = new CacheValue
        {
            StructValue = new Struct
            {
                Fields = { ["value"] = Value.ForString("wrapped") },
            },
        };

        Assert.Equal("wrapped", await ServerProtoEx.MapCacheValueAsync<string>(wire));
        Assert.Equal("wrapped", await ServerProtoEx.MapCacheValueAsync<object>(wire));
    }

    /// <summary>Complex object payloads round-trip through MapToProto / MapFromProto.</summary>
    [Fact]
    public async Task ComplexObjectRoundTripsThroughEntryMapping()
    {
        var source = new NodeCacheEntry<SamplePayload>
        {
            Value = new SamplePayload { Id = 7, Name = "alpha", Tags = ["a", "b"] },
            Version = 3,
            ExpiresUtc = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc),
            Expiration = TimeSpan.FromMinutes(5),
        };

        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<SamplePayload>();

        Assert.NotNull(roundTrip.Value);
        Assert.Equal(7, roundTrip.Value.Id);
        Assert.Equal("alpha", roundTrip.Value.Name);
        Assert.Equal(2, roundTrip.Value.Tags.Length);
        Assert.Equal("a", roundTrip.Value.Tags[0]);
        Assert.Equal("b", roundTrip.Value.Tags[1]);
        Assert.Equal(source.ExpiresUtc, roundTrip.ExpiresUtc);
        Assert.Equal(source.Expiration, roundTrip.Expiration);
    }

    /// <summary>JsonElement payloads encode and decode through struct mapping.</summary>
    [Fact]
    public async Task JsonElementPayloadRoundTripsThroughEntryMapping()
    {
        using var document = JsonDocument.Parse("""{"n":1,"ok":true,"items":[1,2]}""");
        var source = new NodeCacheEntry<JsonElement>
        {
            Value = document.RootElement.Clone(),
            Version = 1,
        };

        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        Assert.Equal(1, roundTrip.Value.GetProperty("n").GetInt32());
        Assert.True(roundTrip.Value.GetProperty("ok").GetBoolean());
        Assert.Equal(2, roundTrip.Value.GetProperty("items").GetArrayLength());
    }

    /// <summary>Unset KindCase maps to the typed default.</summary>
    [Fact]
    public async Task UnsetKindCaseReturnsTypedDefault()
    {
        var wire = new CacheValue();

        Assert.Equal(0, await ServerProtoEx.MapCacheValueAsync<int>(wire));
        Assert.Null(await ServerProtoEx.MapCacheValueAsync<string>(wire));
    }

    private sealed class SamplePayload
    {
        public int Id { get; init; }

        public string Name { get; init; } = string.Empty;

        public string[] Tags { get; init; } = [];
    }
}
