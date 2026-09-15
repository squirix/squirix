using System;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers ProtoEx object and typed primitive mapping arms.</summary>
[Immutable]
public sealed class ProtoExTests
{
    /// <summary>Struct-wrapped protobuf values deserialize to untyped objects.</summary>
    /// <param name="kind">Value kind to wrap.</param>
    [Test]
    [Arguments("string")]
    [Arguments("bool")]
    [Arguments("number")]
    [Arguments("null")]
    public async Task AsyncObjectReadsWrappedValuesAsync(string kind)
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
                _ = await Assert.That(result).IsEqualTo("hello");
                break;
            case "bool":
                _ = await Assert.That(result).IsTypeOf<bool>();
                _ = await Assert.That(result is true).IsTrue();
                break;
            case "number":
                _ = await Assert.That(result).IsEqualTo(3.5d);
                break;
            default:
                _ = await Assert.That(result).IsNull();
                break;
        }
    }

    /// <summary>Exact typed primitive wire forms decode without struct wrapping.</summary>
    /// <param name="kind">Wire kind under test.</param>
    [Test]
    [Arguments("string")]
    [Arguments("bool")]
    [Arguments("int")]
    [Arguments("long")]
    [Arguments("double")]
    [Arguments("null")]
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

        if (string.Equals(kind, "string", StringComparison.Ordinal))
            _ = await Assert.That(await ProtoEx.FromCacheValueAsync<string>(wire, serializer)).IsEqualTo("hello");
        else if (string.Equals(kind, "bool", StringComparison.Ordinal))
            _ = await Assert.That(await ProtoEx.FromCacheValueAsync<bool>(wire, serializer)).IsTrue();
        else if (string.Equals(kind, "int", StringComparison.Ordinal))
            _ = await Assert.That(await ProtoEx.FromCacheValueAsync<int>(wire, serializer)).IsEqualTo(11);
        else if (string.Equals(kind, "long", StringComparison.Ordinal))
            _ = await Assert.That(await ProtoEx.FromCacheValueAsync<long>(wire, serializer)).IsEqualTo(22L);
        else if (string.Equals(kind, "double", StringComparison.Ordinal))
            _ = await Assert.That(await ProtoEx.FromCacheValueAsync<double>(wire, serializer)).IsEqualTo(3.25d);
        else
            _ = await Assert.That(await ProtoEx.FromCacheValueAsync<string>(wire, serializer)).IsNull();
    }

    /// <summary>Entry mapping round-trips typed values and expiration metadata.</summary>
    [Test]
    public async Task MapEntryRoundTripValueAndExpiryAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        var entry = new CacheEntry<string>
        {
            Value = "payload",
            ExpiresUtc = new DateTime(2026, 8, 1, 10, 0, 0, DateTimeKind.Utc),
            Expiration = TimeSpan.FromMinutes(2),
        };

        var wire = ProtoEx.MapEntryToProto(entry, serializer);

        // Golden wire metadata: expiry must map to exact protobuf time forms
        // (2026-08-01T10:00:00Z = unix 1785578400, 2 minutes = 120 seconds),
        // verified against the calendar, not against the reverse mapping.
        _ = await Assert.That(wire.Value.Fields[ValueEnvelope.ScalarEnvelopeKey].StringValue).IsEqualTo("payload");
        _ = await Assert.That(wire.ExpiresUtc.Seconds).IsEqualTo(1785578400L);
        _ = await Assert.That(wire.Expiration.Seconds).IsEqualTo(120L);

        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<string>(wire, serializer);

        _ = await Assert.That(roundTrip.Value).IsEqualTo("payload");
        _ = await Assert.That(roundTrip.ExpiresUtc).IsEqualTo(entry.ExpiresUtc);
        _ = await Assert.That(roundTrip.Expiration).IsEqualTo(entry.Expiration);
    }

    /// <summary>JsonElement values round-trip through entry mapping.</summary>
    [Test]
    public async Task MapEntryRoundTripsJsonElementAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        using var document = JsonDocument.Parse("""{"x":1,"y":[true,null]}""");
        var entry = new CacheEntry<JsonElement> { Value = document.RootElement.Clone() };

        var wire = ProtoEx.MapEntryToProto(entry, serializer);

        // Golden wire struct: the mapping must emit exact proto fields,
        // not merely something the reverse mapping can read back.
        _ = await Assert.That(wire.Value.Fields.Count).IsEqualTo(2);
        var xWire = wire.Value.Fields["x"].StructValue;
        _ = await Assert.That(xWire.Fields[ValueEnvelope.NumberEnvelopeInt64Key].StringValue).IsEqualTo("1");
        var yWire = wire.Value.Fields["y"].ListValue;
        _ = await Assert.That(yWire.Values.Count).IsEqualTo(2);
        _ = await Assert.That(yWire.Values[0].BoolValue).IsTrue();
        _ = await Assert.That(yWire.Values[1].KindCase).IsEqualTo(Value.KindOneofCase.NullValue);

        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<JsonElement>(wire, serializer);

        _ = await Assert.That(roundTrip.Value.GetProperty("x").GetInt32()).IsEqualTo(1);
        var y = roundTrip.Value.GetProperty("y");
        _ = await Assert.That(y.GetArrayLength()).IsEqualTo(2);
        _ = await Assert.That(y[0].GetBoolean()).IsTrue();
        _ = await Assert.That(y[1].ValueKind).IsEqualTo(JsonValueKind.Null);
    }

    /// <summary>Mismatched primitive wire falls back through the struct wrapper path.</summary>
    [Test]
    public async Task MismatchedPrimitiveUsesWrapperAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        var wire = new CacheValue { Int32Value = 5 };

        _ = await Assert.That(await ProtoEx.FromCacheValueAsync<double>(wire, serializer)).IsEqualTo(5d);
    }

    /// <summary>Negative zero JSON values preserve sign through protobuf round-trip.</summary>
    [Test]
    public async Task NegativeZeroPreservesSignAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        using var document = JsonDocument.Parse("-0.0");
        var entry = new CacheEntry<JsonElement> { Value = document.RootElement.Clone() };

        var wire = ProtoEx.MapEntryToProto(entry, serializer);

        // Golden wire scalar: negative zero must survive as a negative double,
        // not collapse to plain zero inside the envelope.
        var enumerator = wire.Value.Fields.GetEnumerator();
        _ = await Assert.That(enumerator.MoveNext()).IsTrue();
        var scalar = enumerator.Current;
        _ = await Assert.That(enumerator.MoveNext()).IsFalse();
        _ = await Assert.That(scalar.Key).IsEqualTo(ValueEnvelope.ScalarEnvelopeKey);
        _ = await Assert.That(scalar.Value.KindCase).IsEqualTo(Value.KindOneofCase.NumberValue);
        _ = await Assert.That(scalar.Value.NumberValue).IsEqualTo(0d);
        _ = await Assert.That(double.IsNegative(scalar.Value.NumberValue)).IsTrue();

        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<JsonElement>(wire, serializer);

        _ = await Assert.That(roundTrip.Value.ValueKind).IsEqualTo(JsonValueKind.Number);
        _ = await Assert.That(roundTrip.Value.GetDouble()).IsEqualTo(0.0);
        _ = await Assert.That(roundTrip.Value.GetRawText()).IsEqualTo("-0");
    }
}
