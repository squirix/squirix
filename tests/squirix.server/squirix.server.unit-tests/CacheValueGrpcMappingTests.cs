using System;
using System.Globalization;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Tests compact <see cref="CacheValue" /> gRPC scalar mapping.</summary>
[Immutable]
public sealed class CacheValueGrpcMappingTests
{
    /// <summary>Array numbers preserve int64 and decimal precision.</summary>
    [Test]
    public async Task ArrayNumbersPreservePrecisionAsync()
    {
        using var document = JsonDocument.Parse("[9007199254740993,123.456,null]");
        var source = new NodeCacheEntry<JsonElement> { Value = document.RootElement.Clone(), Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        _ = await Assert.That(roundTrip.Value.GetArrayLength()).IsEqualTo(3);
        _ = await Assert.That(roundTrip.Value[0].GetRawText()).IsEqualTo("9007199254740993");
        _ = await Assert.That(roundTrip.Value[1].GetRawText()).IsEqualTo("123.456");
        _ = await Assert.That(roundTrip.Value[2].ValueKind).IsEqualTo(JsonValueKind.Null);
    }

    /// <summary>Compact value encoding covers every CLR primitive arm.</summary>
    [Test]
    public async Task CacheValueToGrpcValueCoversPrimitiveArms()
    {
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue<string>(null).KindCase).IsEqualTo(CacheValue.KindOneofCase.NullValue);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue("s").StringValue).IsEqualTo("s");
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(true).BoolValue).IsTrue();
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(1.25d).DoubleValue).IsEqualTo(1.25d);
        _ = await Assert.That(ServerProtoEx.CacheValueToGrpcValue(new SamplePayload { Id = 1, Name = "n" }).KindCase).IsEqualTo(CacheValue.KindOneofCase.StructValue);
    }

    /// <summary>Complex object payloads round-trip through MapToProto / MapFromProto.</summary>
    [Test]
    public async Task ComplexObjectRoundTripsViaMappingAsync()
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

        _ = await Assert.That(roundTrip.Value).IsNotNull();
        _ = await Assert.That(roundTrip.Value.Id).IsEqualTo(7);
        _ = await Assert.That(roundTrip.Value.Name).IsEqualTo("alpha");
        _ = await Assert.That(roundTrip.Value.Tags.Length).IsEqualTo(2);
        _ = await Assert.That(roundTrip.Value.Tags[0]).IsEqualTo("a");
        _ = await Assert.That(roundTrip.Value.Tags[1]).IsEqualTo("b");
        _ = await Assert.That(roundTrip.ExpiresUtc).IsEqualTo(source.ExpiresUtc);
        _ = await Assert.That(roundTrip.Expiration).IsEqualTo(source.Expiration);
    }

    /// <summary>Decimal values preserve precision through struct round-trips.</summary>
    [Test]
    public async Task DecimalKeepsPrecisionRoundTripAsync()
    {
        var source = new NodeCacheEntry<object?> { Value = 123.456m, Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<object?>();

        var element = await Assert.That(roundTrip.Value).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(element.GetRawText()).IsEqualTo("123.456");
    }

    /// <summary>Default-valued value-type payloads encode through the scalar path, not as protobuf null.</summary>
    [Test]
    public async Task DefaultedValueTypesNotEncodedNullAsync()
    {
        var intWire = new NodeCacheEntry<int> { Value = 0, Version = 1 }.MapToProto();
        _ = await Assert.That(intWire.Value.Fields["\0squirix:scalar"].KindCase).IsEqualTo(Value.KindOneofCase.NumberValue);
        _ = await Assert.That((await intWire.MapFromProtoAsync<int>()).Value).IsEqualTo(0);

        var boolWire = new NodeCacheEntry<bool> { Value = false, Version = 1 }.MapToProto();
        _ = await Assert.That(boolWire.Value.Fields["\0squirix:scalar"].KindCase).IsEqualTo(Value.KindOneofCase.BoolValue);
        _ = await Assert.That((await boolWire.MapFromProtoAsync<bool>()).Value).IsFalse();

        var doubleWire = new NodeCacheEntry<double> { Value = 0.0, Version = 1 }.MapToProto();
        _ = await Assert.That(doubleWire.Value.Fields["\0squirix:scalar"].KindCase).IsEqualTo(Value.KindOneofCase.NumberValue);
        _ = await Assert.That((await doubleWire.MapFromProtoAsync<double>()).Value).IsEqualTo(0.0);

        var structWire = new NodeCacheEntry<SamplePayload> { Value = new SamplePayload(), Version = 1 }.MapToProto();
        _ = await Assert.That(structWire.Value.Fields.Count).IsEqualTo(3);
        var payload = (await structWire.MapFromProtoAsync<SamplePayload>()).Value;
        _ = await Assert.That(payload).IsNotNull();
        _ = await Assert.That(payload.Id).IsEqualTo(0);
        _ = await Assert.That(payload.Name).IsEqualTo(string.Empty);
    }

    /// <summary>Exact primitive wire forms decode without struct wrapping.</summary>
    /// <param name="kind">Wire kind under test.</param>
    [Test]
    [Arguments("string")]
    [Arguments("bool")]
    [Arguments("long")]
    [Arguments("double")]
    [Arguments("null")]
    public async Task ExactPrimitiveWireFormsDecodeAsync(string kind)
    {
        var wire = kind switch
        {
            "string" => new CacheValue { StringValue = "hello" },
            "bool" => new CacheValue { BoolValue = true },
            "long" => new CacheValue { Int64Value = 99L },
            "double" => new CacheValue { DoubleValue = 1.5d },
            _ => new CacheValue { NullValue = NullValue.NullValue },
        };

        if (string.Equals(kind, "string", StringComparison.Ordinal))
            _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<string>(wire)).IsEqualTo("hello");
        else if (string.Equals(kind, "bool", StringComparison.Ordinal))
            _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<bool>(wire)).IsTrue();
        else if (string.Equals(kind, "long", StringComparison.Ordinal))
            _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<long>(wire)).IsEqualTo(99L);
        else if (string.Equals(kind, "double", StringComparison.Ordinal))
            _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<double>(wire)).IsEqualTo(1.5d);
        else
            _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<string>(wire)).IsNull();
    }

    /// <summary>CLR <see cref="int" /> values use the dedicated int32 wire arm.</summary>
    [Test]
    public async Task Int32EncodesAsInt32ValueWireForm()
    {
        var wire = ServerProtoEx.CacheValueToGrpcValue(42);

        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.Int32Value);
        _ = await Assert.That(wire.Int32Value).IsEqualTo(42);
    }

    /// <summary>int32 wire values decode to typed <see cref="int" /> reads.</summary>
    [Test]
    public async Task Int32ValueRoundTripsAsIntAsync()
    {
        var wire = new CacheValue { Int32Value = 7 };

        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<int>(wire)).IsEqualTo(7);
    }

    /// <summary>CLR <see cref="long" /> values outside the int32 range keep the int64 wire arm.</summary>
    [Test]
    public async Task Int64EncodesAsInt64ValueWireForm()
    {
        const long value = int.MaxValue + 1L;
        var wire = ServerProtoEx.CacheValueToGrpcValue(value);

        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.Int64Value);
        _ = await Assert.That(wire.Int64Value).IsEqualTo(value);
    }

    /// <summary>Large int64 values preserve precision through struct round-trips.</summary>
    [Test]
    public async Task Int64PreservesPrecisionRoundTripAsync()
    {
        const long big = 9_007_199_254_740_993L;
        var source = new NodeCacheEntry<object?> { Value = big, Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<object?>();

        var element = await Assert.That(roundTrip.Value).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(element.GetRawText()).IsEqualTo(big.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>Large int64 compact wire values preserve precision when decoded as decimal or JsonElement.</summary>
    [Test]
    public async Task Int64WireValueKeepsPrecisionAsync()
    {
        const long big = 9_007_199_254_740_993L;
        var wire = new CacheValue { Int64Value = big };

        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<decimal>(wire)).IsEqualTo(big);

        var element = await ServerProtoEx.MapCacheValueAsync<JsonElement>(wire);
        _ = await Assert.That(element.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(element.GetRawText()).IsEqualTo(big.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>JsonElement payloads encode and decode through struct mapping.</summary>
    [Test]
    public async Task JsonElementPayloadRoundTripsAsync()
    {
        using var document = JsonDocument.Parse("""{"n":1,"ok":true,"items":[1,2]}""");
        var source = new NodeCacheEntry<JsonElement>
        {
            Value = document.RootElement.Clone(),
            Version = 1,
        };

        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        _ = await Assert.That(roundTrip.Value.GetProperty("n").GetInt32()).IsEqualTo(1);
        _ = await Assert.That(roundTrip.Value.GetProperty("ok").GetBoolean()).IsTrue();
        var items = roundTrip.Value.GetProperty("items");
        _ = await Assert.That(items.GetArrayLength()).IsEqualTo(2);
        _ = await Assert.That(items[0].GetInt32()).IsEqualTo(1);
        _ = await Assert.That(items[1].GetInt32()).IsEqualTo(2);
    }

    /// <summary>Large Int64 wire values preserve exact precision for typed long and JsonElement reads.</summary>
    [Test]
    public async Task LargeInt64PreservesExactValueAsync()
    {
        const long big = 9_007_199_254_740_993L;
        var wire = new CacheValue { Int64Value = big };

        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<long>(wire)).IsEqualTo(big);

        var element = await ServerProtoEx.MapCacheValueAsync<JsonElement>(wire);
        _ = await Assert.That(element.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(element.GetInt64()).IsEqualTo(big);
        _ = await Assert.That(element.GetRawText()).IsEqualTo(big.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>MapFromProto ignores zero timestamps and preserves relative expiration.</summary>
    [Test]
    public async Task MapFromProtoIgnoresZeroTimestampAsync()
    {
        var wire = new CacheEntryWire
        {
            Value = new Struct { Fields = { ["\0squirix:scalar"] = Value.ForString("exp") } },
            ExpiresUtc = new Timestamp { Seconds = 0, Nanos = 0 },
            Expiration = Duration.FromTimeSpan(TimeSpan.FromSeconds(9)),
        };

        var entry = await wire.MapFromProtoAsync<string>();
        _ = await Assert.That(entry.Value).IsEqualTo("exp");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
        _ = await Assert.That(entry.Expiration).IsEqualTo(TimeSpan.FromSeconds(9));
    }

    /// <summary>Primitive wire with a mismatched CLR type falls back through struct wrapping.</summary>
    [Test]
    public async Task MismatchedPrimitiveWireFallsBackAsync()
    {
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<int>(new CacheValue { Int64Value = 42L })).IsEqualTo(42);
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<int>(new CacheValue { Int32Value = 7 })).IsEqualTo(7);
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<string>(new CacheValue { StringValue = "x" })).IsEqualTo("x");
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<bool>(new CacheValue { BoolValue = true })).IsTrue();
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<double>(new CacheValue { DoubleValue = 1.5d })).IsEqualTo(1.5d);
    }

    /// <summary>Object-typed reads cover every compact wire kind.</summary>
    /// <param name="kind">Wire kind under test.</param>
    [Test]
    [Arguments("string")]
    [Arguments("bool")]
    [Arguments("int32")]
    [Arguments("int64")]
    [Arguments("double")]
    [Arguments("null")]
    [Arguments("none")]
    public async Task ObjectTypedReadsCoverWireKindsAsync(string kind)
    {
        var wire = kind switch
        {
            "string" => new CacheValue { StringValue = "obj" },
            "bool" => new CacheValue { BoolValue = true },
            "int32" => new CacheValue { Int32Value = 3 },
            "int64" => new CacheValue { Int64Value = 9L },
            "double" => new CacheValue { DoubleValue = 2.5d },
            "null" => new CacheValue { NullValue = NullValue.NullValue },
            _ => new CacheValue(),
        };

        var mapped = await ServerProtoEx.MapCacheValueAsync<object>(wire);
        switch (kind)
        {
            case "string":
                _ = await Assert.That(mapped).IsEqualTo("obj");
                break;
            case "bool":
                _ = await Assert.That(mapped).IsTypeOf<bool>();
                _ = await Assert.That(mapped is true).IsTrue();
                break;
            case "int32":
                _ = await Assert.That(mapped).IsEqualTo(3);
                break;
            case "int64":
                _ = await Assert.That(mapped).IsEqualTo(9L);
                break;
            case "double":
                _ = await Assert.That(mapped).IsEqualTo(2.5d);
                break;
            default:
                _ = await Assert.That(mapped).IsNull();
                break;
        }
    }

    /// <summary>Entry mapping encodes and decodes primitive CLR values through the struct envelope.</summary>
    /// <param name="kind">Primitive kind under test.</param>
    [Test]
    [Arguments("null")]
    [Arguments("string")]
    [Arguments("int")]
    [Arguments("long")]
    [Arguments("double")]
    [Arguments("bool")]
    public async Task PrimitiveEntryValuesRoundTripAsync(string kind)
    {
        switch (kind)
        {
            case "null":
            {
                var wire = new NodeCacheEntry<object?> { Value = null, Version = 1 }.MapToProto();
                var roundTrip = await wire.MapFromProtoAsync<object?>();
                _ = await Assert.That(roundTrip.Value).IsNull();
                break;
            }

            case "string":
            {
                var wire = new NodeCacheEntry<string> { Value = "text", Version = 1 }.MapToProto();
                _ = await Assert.That((await wire.MapFromProtoAsync<string>()).Value).IsEqualTo("text");
                break;
            }

            case "int":
            {
                var wire = new NodeCacheEntry<int> { Value = 11, Version = 1 }.MapToProto();
                _ = await Assert.That((await wire.MapFromProtoAsync<int>()).Value).IsEqualTo(11);
                break;
            }

            case "long":
            {
                var wire = new NodeCacheEntry<long> { Value = 12L, Version = 1 }.MapToProto();
                _ = await Assert.That((await wire.MapFromProtoAsync<long>()).Value).IsEqualTo(12L);
                break;
            }

            case "double":
            {
                var wire = new NodeCacheEntry<double> { Value = 3.5d, Version = 1 }.MapToProto();
                _ = await Assert.That((await wire.MapFromProtoAsync<double>()).Value).IsEqualTo(3.5d);
                break;
            }

            default:
            {
                var wire = new NodeCacheEntry<bool> { Value = true, Version = 1 }.MapToProto();
                _ = await Assert.That((await wire.MapFromProtoAsync<bool>()).Value).IsTrue();
                break;
            }
        }
    }

    /// <summary>A user object with a single property named "value" round-trips as an object, not a scalar.</summary>
    [Test]
    public async Task SingleValueObjectRoundTripsAsObjectAsync()
    {
        var source = new NodeCacheEntry<ValuePayload> { Value = new ValuePayload { Value = "x" }, Version = 1 };
        var wire = source.MapToProto();
        var roundTrip = await wire.MapFromProtoAsync<ValuePayload>();

        _ = await Assert.That(roundTrip.Value).IsNotNull();
        _ = await Assert.That(roundTrip.Value.Value).IsEqualTo("x");
    }

    /// <summary>Multi-field structs deserialize for an object and typed targets.</summary>
    [Test]
    public async Task StructDeserializesObjectAndTypedAsync()
    {
        var multi = new Struct
        {
            Fields =
            {
                ["Id"] = Value.ForNumber(5),
                ["Name"] = Value.ForString("multi"),
                ["Tags"] = new Value { ListValue = new ListValue { Values = { Value.ForString("t") } } },
            },
        };
        var wire = new CacheValue { StructValue = multi };

        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(wire)).IsTypeOf<JsonElement>();
        var asObject = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(wire)).IsTypeOf<JsonElement>();
        _ = await Assert.That(asObject.GetProperty("Id").GetInt32()).IsEqualTo(5);
        _ = await Assert.That(asObject.GetProperty("Name").GetString()).IsEqualTo("multi");

        var typed = await ServerProtoEx.MapCacheValueAsync<SamplePayload>(wire);
        _ = await Assert.That(typed).IsNotNull();
        _ = await Assert.That(typed.Id).IsEqualTo(5);
        _ = await Assert.That(typed.Name).IsEqualTo("multi");
        await SequenceAssert.EqualAsync(["t"], typed.Tags, StringComparer.Ordinal);
    }
}
