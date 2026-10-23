using System;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Tests compact <see cref="CacheValue" /> gRPC mapping for wrapped, struct, negative-zero and null/unset wire forms.</summary>
[Immutable]
public sealed class CacheValueGrpcWrappedValueMappingTests
{
    /// <summary>Negative zero JSON values preserve sign through server protobuf round-trip.</summary>
    [Test]
    public async Task NegativeZeroPreservesSignAsync()
    {
        using var document = JsonDocument.Parse("-0.0");
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        _ = await Assert.That(roundTrip.Value.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(roundTrip.Value.GetDouble()).IsEqualTo(0.0);
        _ = await Assert.That(roundTrip.Value.GetRawText()).IsEqualTo("-0");
    }

    /// <summary>Negative zero preserves IEEE 754 sign bit through JSON round-trip.</summary>
    [Test]
    public async Task NegativeZeroRoundTripsPreservedAsync()
    {
        using var document = JsonDocument.Parse("-0.0");
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        _ = await Assert.That(roundTrip.Value.ValueKind).IsEqualTo(JsonValueKind.Number);
        var roundTripDouble = roundTrip.Value.GetDouble();
        _ = await Assert.That(roundTripDouble).IsEqualTo(0.0);
        _ = await Assert.That(BitConverter.DoubleToInt64Bits(roundTripDouble)).IsEqualTo(BitConverter.DoubleToInt64Bits(-0.0));
    }

    /// <summary>Negative zero wire value preserves IEEE 754 sign bit through gRPC mapping.</summary>
    [Test]
    public async Task NegativeZeroWireValuePreservedAsync()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(-0.0);
        var roundTrip = await ServerProtoEx.MapCacheValueAsync<double>(wire);

        _ = await Assert.That(BitConverter.DoubleToInt64Bits(roundTrip)).IsEqualTo(BitConverter.DoubleToInt64Bits(-0.0));
    }

    /// <summary>Nested object numbers preserve int64 and decimal precision.</summary>
    [Test]
    public async Task NestedJsonNumbersPreservePrecisionAsync()
    {
        using var document = JsonDocument.Parse("""{"big":9007199254740993,"dec":123.456,"ok":true}""");
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        _ = await Assert.That(roundTrip.Value.GetProperty("big").GetRawText()).IsEqualTo("9007199254740993");
        _ = await Assert.That(roundTrip.Value.GetProperty("dec").GetRawText()).IsEqualTo("123.456");
        _ = await Assert.That(roundTrip.Value.GetProperty("ok").GetBoolean()).IsTrue();
    }

    /// <summary>Non-object JsonElement values encode through the single-field envelope.</summary>
    /// <param name="json">JSON literal under test.</param>
    [Test]
    [Arguments("\"text\"")]
    [Arguments("true")]
    [Arguments("false")]
    [Arguments("null")]
    [Arguments("12")]
    [Arguments("1.25")]
    [Arguments("[1,{\"k\":2},null]")]
    public async Task NonObjectJsonElementRoundTripsAsync(string json)
    {
        using var document = JsonDocument.Parse(json);
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };

        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        _ = await Assert.That(roundTrip.Value.ToString()).IsEqualTo(document.RootElement.ToString());
    }

    /// <summary>Struct-wrapped values decode for object and typed targets.</summary>
    [Test]
    public async Task StructWrappedValuesDecodeAsync()
    {
        var wire = new CacheValue
        {
            StructValue = new Struct
            {
                Fields = { ["\0squirix:scalar"] = Value.ForString("wrapped") },
            },
        };

        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<string>(wire)).IsEqualTo("wrapped");
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(wire)).IsEqualTo("wrapped");
        var numberWire = new CacheValue { StructValue = new Struct { Fields = { ["\0squirix:scalar"] = Value.ForNumber(1.5d) } } };
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<double>(numberWire)).IsEqualTo(1.5d);
        var boolWire = new CacheValue { StructValue = new Struct { Fields = { ["\0squirix:scalar"] = Value.ForBool(true) } } };
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<bool>(boolWire)).IsTrue();
    }

    /// <summary>Unset KindCase maps to the typed default.</summary>
    [Test]
    public async Task UnsetKindCaseReturnsTypedDefaultAsync()
    {
        var wire = new CacheValue();

        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<int>(wire)).IsEqualTo(0);
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<string>(wire)).IsNull();
    }

    /// <summary>Wrapped list and nested struct values decode for object targets.</summary>
    [Test]
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
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(listWire)).IsTypeOf<JsonElement>();
        var listElement = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(listWire)).IsTypeOf<JsonElement>();
        _ = await Assert.That(listElement.ValueKind).IsEqualTo(JsonValueKind.Array);
        _ = await Assert.That(listElement.GetArrayLength()).IsEqualTo(3);

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
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(structWire)).IsTypeOf<JsonElement>();
        var structElement = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(structWire)).IsTypeOf<JsonElement>();
        _ = await Assert.That(structElement.GetProperty("inner").GetString()).IsEqualTo("x");
    }

    /// <summary>Wrapped protobuf null and unset values decode as null for object targets.</summary>
    [Test]
    public async Task WrappedNullAndUnsetDecodeAsNullAsync()
    {
        var nullWire = new CacheValue
        {
            StructValue = new Struct { Fields = { ["\0squirix:scalar"] = Value.ForNull() } },
        };
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(nullWire)).IsNull();

        var unsetWire = new CacheValue
        {
            StructValue = new Struct { Fields = { ["\0squirix:scalar"] = new Value() } },
        };
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(unsetWire)).IsNull();
    }
}
