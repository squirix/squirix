using System;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Tests compact <see cref="CacheValue" /> gRPC mapping for wrapped, struct, negative-zero and null/unset wire forms.</summary>
[Immutable]
public sealed class CacheValueGrpcWrappedValueMappingTests
{
    /// <summary>Negative zero JSON values preserve sign through server protobuf round-trip.</summary>
    [Fact]
    public async Task NegativeZeroPreservesSignAsync()
    {
        using var document = JsonDocument.Parse("-0.0");
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        Assert.Equal(JsonValueKind.Number, roundTrip.Value.ValueKind);
        Assert.Equal(0.0, roundTrip.Value.GetDouble());
        Assert.Equal("-0", roundTrip.Value.GetRawText());
    }

    /// <summary>Negative zero preserves IEEE 754 sign bit through JSON round-trip.</summary>
    [Fact]
    public async Task NegativeZeroRoundTripsPreservedAsync()
    {
        using var document = JsonDocument.Parse("-0.0");
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        Assert.Equal(JsonValueKind.Number, roundTrip.Value.ValueKind);
        var roundTripDouble = roundTrip.Value.GetDouble();
        Assert.Equal(0.0, roundTripDouble);
        Assert.Equal(BitConverter.DoubleToInt64Bits(-0.0), BitConverter.DoubleToInt64Bits(roundTripDouble));
    }

    /// <summary>Negative zero wire value preserves IEEE 754 sign bit through gRPC mapping.</summary>
    [Fact]
    public async Task NegativeZeroWireValuePreservedAsync()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(-0.0);
        var roundTrip = await ServerProtoEx.MapCacheValueAsync<double>(wire);

        Assert.Equal(BitConverter.DoubleToInt64Bits(-0.0), BitConverter.DoubleToInt64Bits(roundTrip));
    }

    /// <summary>Struct-wrapped values decode for object and typed targets.</summary>
    [Fact]
    public async Task StructWrappedValuesDecodeAsync()
    {
        var wire = new CacheValue
        {
            StructValue = new Struct
            {
                Fields = { ["\0squirix:scalar"] = Value.ForString("wrapped") },
            },
        };

        Assert.Equal("wrapped", await ServerProtoEx.MapCacheValueAsync<string>(wire));
        Assert.Equal("wrapped", await ServerProtoEx.MapCacheValueAsync<object>(wire));
        var numberWire = new CacheValue { StructValue = new Struct { Fields = { ["\0squirix:scalar"] = Value.ForNumber(1.5d) } } };
        Assert.Equal(1.5d, await ServerProtoEx.MapCacheValueAsync<double>(numberWire));
        var boolWire = new CacheValue { StructValue = new Struct { Fields = { ["\0squirix:scalar"] = Value.ForBool(true) } } };
        Assert.True(await ServerProtoEx.MapCacheValueAsync<bool>(boolWire));
    }

    /// <summary>Unset KindCase maps to the typed default.</summary>
    [Fact]
    public async Task UnsetKindCaseReturnsTypedDefaultAsync()
    {
        var wire = new CacheValue();

        Assert.Equal(0, await ServerProtoEx.MapCacheValueAsync<int>(wire));
        Assert.Null(await ServerProtoEx.MapCacheValueAsync<string>(wire));
    }

    /// <summary>Wrapped list and nested struct values decode for object targets.</summary>
    [Fact]
    public async Task WrappedListStructDecodeJsonElementAsync()
    {
        var listWire = new CacheValue
        {
            StructValue = new Struct
            {
                Fields =
                {
                    ["\0squirix:scalar"] = new Value
                    {
                        ListValue = new ListValue
                        {
                            Values =
                            {
                                Value.ForNumber(1),
                                Value.ForBool(false),
                                Value.ForNull(),
                            },
                        },
                    },
                },
            },
        };
        var listElement = Assert.IsType<JsonElement>(await ServerProtoEx.MapCacheValueAsync<object>(listWire));
        Assert.Equal(JsonValueKind.Array, listElement.ValueKind);
        Assert.Equal(3, listElement.GetArrayLength());

        var structWire = new CacheValue
        {
            StructValue = new Struct
            {
                Fields =
                {
                    ["\0squirix:scalar"] = Value.ForStruct(new Struct { Fields = { ["inner"] = Value.ForString("x") } }),
                },
            },
        };
        var structElement = Assert.IsType<JsonElement>(await ServerProtoEx.MapCacheValueAsync<object>(structWire));
        Assert.Equal("x", structElement.GetProperty("inner").GetString());
    }

    /// <summary>Wrapped protobuf null and unset values decode as null for object targets.</summary>
    [Fact]
    public async Task WrappedNullAndUnsetDecodeAsNullAsync()
    {
        var nullWire = new CacheValue
        {
            StructValue = new Struct { Fields = { ["\0squirix:scalar"] = Value.ForNull() } },
        };
        Assert.Null(await ServerProtoEx.MapCacheValueAsync<object>(nullWire));

        var unsetWire = new CacheValue
        {
            StructValue = new Struct { Fields = { ["\0squirix:scalar"] = new Value() } },
        };
        Assert.Null(await ServerProtoEx.MapCacheValueAsync<object>(unsetWire));
    }

    /// <summary>Non-object JsonElement values encode through the single-field envelope.</summary>
    /// <param name="json">JSON literal under test.</param>
    [Theory]
    [InlineData("\"text\"")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("null")]
    [InlineData("12")]
    [InlineData("1.25")]
    [InlineData("[1,{\"k\":2},null]")]
    public async Task NonObjectJsonElementRoundTripsAsync(string json)
    {
        using var document = JsonDocument.Parse(json);
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };

        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        Assert.Equal(document.RootElement.ToString(), roundTrip.Value.ToString());
    }

    /// <summary>Nested object numbers preserve int64 and decimal precision.</summary>
    [Fact]
    public async Task NestedJsonNumbersPreservePrecisionAsync()
    {
        using var document = JsonDocument.Parse("""{"big":9007199254740993,"dec":123.456,"ok":true}""");
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        Assert.Equal("9007199254740993", roundTrip.Value.GetProperty("big").GetRawText());
        Assert.Equal("123.456", roundTrip.Value.GetProperty("dec").GetRawText());
        Assert.True(roundTrip.Value.GetProperty("ok").GetBoolean());
    }
}
