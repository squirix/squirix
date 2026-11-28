using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Rocks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using Squirix.Transport;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers how WireMapping maps cache values: native kinds for scalars, one JSON text for everything else.</summary>
[Immutable]
public sealed class WireMappingTests
{
    /// <summary>Every scalar kind reads as an untyped object of its CLR type, and JSON text reads as an element.</summary>
    [Test]
    public async Task ObjectReadsEveryKind()
    {
        var serializer = new SystemTextJsonSerializer();

        _ = await Assert.That(WireMapping.Read<object>(new CacheValue { StringValue = "hello" }, serializer)).IsEqualTo("hello");
        _ = await Assert.That(WireMapping.Read<object>(new CacheValue { BoolValue = true }, serializer) is true).IsTrue();
        _ = await Assert.That(WireMapping.Read<object>(new CacheValue { Int64Value = 22L }, serializer)).IsTypeOf<long>();
        _ = await Assert.That(WireMapping.Read<object>(new CacheValue { DoubleValue = 3.5d }, serializer)).IsEqualTo(3.5d);
        _ = await Assert.That(WireMapping.Read<object>(new CacheValue { NullValue = NullValue.NullValue }, serializer)).IsNull();
        var element = await Assert.That(WireMapping.Read<object>(JsonWire("""{"a":[1,2]}"""), serializer)).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.GetProperty("a").GetArrayLength()).IsEqualTo(2);
    }

    /// <summary>A number from JSON text reads as an element that keeps every digit.</summary>
    [Test]
    public async Task ObjectReadsNumberAsElement()
    {
        var serializer = new SystemTextJsonSerializer();

        var element = await Assert.That(WireMapping.Read<object>(JsonWire("12345678901234567890"), serializer)).IsTypeOf<JsonElement>();

        _ = await Assert.That(element.GetRawText()).IsEqualTo("12345678901234567890");
    }

    /// <summary>Exact typed primitive wire forms decode without the serializer.</summary>
    [Test]
    public async Task TypedReadsExactPrimitives()
    {
        var serializer = new ISquirixSerializerCreateExpectations().Instance();

        _ = await Assert.That(WireMapping.Read<string>(new CacheValue { StringValue = "hello" }, serializer)).IsEqualTo("hello");
        _ = await Assert.That(WireMapping.Read<bool>(new CacheValue { BoolValue = true }, serializer)).IsTrue();
        _ = await Assert.That(WireMapping.Read<int>(new CacheValue { Int64Value = 11L }, serializer)).IsEqualTo(11);
        _ = await Assert.That(WireMapping.Read<long>(new CacheValue { Int64Value = 22L }, serializer)).IsEqualTo(22L);
        _ = await Assert.That(WireMapping.Read<double>(new CacheValue { DoubleValue = 3.25d }, serializer)).IsEqualTo(3.25d);
        _ = await Assert.That(WireMapping.Read<string>(new CacheValue { NullValue = NullValue.NullValue }, serializer)).IsNull();
    }

    /// <summary>A typed cache deserializes JSON text straight from the received bytes.</summary>
    [Test]
    public async Task TypedReadsJsonTextThroughSerializer()
    {
        ClientSerializerMetadata.RegisterContext(CustomCacheValueJsonContext.Default);
        var serializer = new SystemTextJsonSerializer();

        var value = WireMapping.Read<CustomCacheValue>(JsonWire("""{"Count":7,"Name":"dto"}"""), serializer);

        _ = await Assert.That(value!.Count).IsEqualTo(7);
        _ = await Assert.That(value.Name).IsEqualTo("dto");
    }

    /// <summary>A decimal keeps every digit through the wire.</summary>
    [Test]
    public async Task DecimalKeepsPrecision()
    {
        const decimal precise = 12345678901234567.123456789m;
        var serializer = new SystemTextJsonSerializer();

        var wire = WireMapping.CreateWireEntry<decimal>(precise, serializer);

        _ = await Assert.That(wire.Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(WireMapping.Read<decimal>(wire.Value, serializer)).IsEqualTo(precise);
    }

    /// <summary>A scalar read by a type that is not its native one is deserialized through the serializer.</summary>
    [Test]
    public async Task MismatchedScalarUsesSerializer()
    {
        var serializer = new SystemTextJsonSerializer();

        _ = await Assert.That(WireMapping.Read<double>(new CacheValue { Int64Value = 5L }, serializer)).IsEqualTo(5d);
        _ = await Assert.That(WireMapping.Read<decimal>(new CacheValue { Int64Value = 9_007_199_254_740_993L }, serializer)).IsEqualTo(9_007_199_254_740_993m);
    }

    /// <summary>A value the server sent without a kind is a protocol failure, not a null.</summary>
    [Test]
    public async Task ValueWithoutKindIsRejected()
    {
        var serializer = new SystemTextJsonSerializer();

        var failure = ExceptionAssert.For<SquirixTransportException>().Throws(serializer, static value => _ = WireMapping.Read<string>(new CacheValue(), value));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(CacheTransportStatusCode.Internal);
    }

    /// <summary>Scalars are written as their native kinds, and an int travels as an int64.</summary>
    [Test]
    public async Task ScalarsWriteNativeKinds()
    {
        var serializer = new SystemTextJsonSerializer();

        _ = await Assert.That(WireMapping.CreateWireEntry<string?>(null, serializer).Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.NullValue);
        _ = await Assert.That(WireMapping.CreateWireEntry<string>("s", serializer).Value!.StringValue).IsEqualTo("s");
        _ = await Assert.That(WireMapping.CreateWireEntry<int>(0, serializer).Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.Int64Value);
        _ = await Assert.That(WireMapping.CreateWireEntry<long>(long.MinValue, serializer).Value!.Int64Value).IsEqualTo(long.MinValue);
        _ = await Assert.That(WireMapping.CreateWireEntry<double>(0.0, serializer).Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.DoubleValue);
        _ = await Assert.That(WireMapping.CreateWireEntry<bool>(false, serializer).Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.BoolValue);
    }

    /// <summary>A JSON element is written as its scalar kind, or for a number, an object or an array as its own text.</summary>
    /// <param name="text">The JSON text of the element.</param>
    /// <param name="kind">The expected wire kind.</param>
    [Test]
    [Arguments("\"s\"", "StringValue")]
    [Arguments("true", "BoolValue")]
    [Arguments("null", "NullValue")]
    [Arguments("-0.0", "JsonUtf8")]
    [Arguments("12345678901234567890", "JsonUtf8")]
    [Arguments("""{"x":1,"y":[true,null]}""", "JsonUtf8")]
    public async Task JsonElementWritesByKind(string text, string kind)
    {
        var serializer = new SystemTextJsonSerializer();

        var wire = WireMapping.CreateWireEntry<JsonElement>(JsonElement.Parse(text), serializer);

        _ = await Assert.That(wire.Value!.KindCase).IsEqualTo(System.Enum.Parse<CacheValue.KindOneofCase>(kind));
        _ = await Assert.That(!string.Equals(kind, nameof(CacheValue.KindOneofCase.JsonUtf8), StringComparison.Ordinal) || string.Equals(Encoding.UTF8.GetString(wire.Value.JsonUtf8.Span), text, StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>An element parsed with comments and trailing commas is written as strict JSON text.</summary>
    [Test]
    public async Task LenientJsonElementWritesStrictText()
    {
        var serializer = new SystemTextJsonSerializer();
        var element = JsonElement.Parse("""{"x":1, /* note */ "y":[true,],}""", new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true });

        var wire = WireMapping.CreateWireEntry<JsonElement>(element, serializer);

        _ = await Assert.That(Encoding.UTF8.GetString(wire.Value!.JsonUtf8.Span)).IsEqualTo("""{"x":1,"y":[true]}""");
    }

    /// <summary>An object is written as the bytes the serializer produced, without a copy.</summary>
    [Test]
    public async Task ObjectWritesSerializerBytesWithoutCopy()
    {
        var produced = Encoding.UTF8.GetBytes("""{"Count":1,"Name":"n"}""");
        var expectations = new ISquirixSerializerCreateExpectations();
        _ = expectations.Setups.SerializeToUtf8Bytes(Arg.Any<CustomCacheValue?>()).ReturnValue(produced);

        var wire = WireMapping.CreateWireEntry<CustomCacheValue>(new CustomCacheValue { Count = 1, Name = "n" }, expectations.Instance());

        _ = await Assert.That(wire.Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(wire.Value.JsonUtf8.Span.Overlaps(produced, out var offset)).IsTrue();
        _ = await Assert.That(offset).IsEqualTo(0);
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

        var wire = WireMapping.CreateWireEntry(entry.Value, serializer) with { ExpiresUtc = entry.ExpiresUtc, Expiration = entry.Expiration };

        _ = await Assert.That(wire.Value!.StringValue).IsEqualTo("payload");

        var roundTrip = WireMapping.MapWireEntryToCacheEntry<string>(wire, serializer);

        _ = await Assert.That(roundTrip.Value).IsEqualTo("payload");
        _ = await Assert.That(roundTrip.ExpiresUtc).IsEqualTo(entry.ExpiresUtc);
        _ = await Assert.That(roundTrip.Expiration).IsEqualTo(entry.Expiration);
    }

    /// <summary>A missing required duration in a server reply produces a stable transport error.</summary>
    [Test]
    public async Task MissingDurationIsRejectedAsync()
    {
        var failure = ExceptionAssert.For<SquirixTransportException>().Throws<TimeSpan?>(null, static remaining => _ = WireMapping.ReadDuration(remaining));
        _ = await Assert.That(failure.StatusCode).IsEqualTo(CacheTransportStatusCode.Internal);
    }

    private static CacheValue JsonWire(string json) => new() { JsonUtf8 = Encoding.UTF8.GetBytes(json) };
}
