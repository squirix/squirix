using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Rocks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers how ProtoEx maps cache values: native kinds for scalars, one JSON text for everything else.</summary>
[Immutable]
public sealed class ProtoExTests
{
    /// <summary>Every scalar kind reads as an untyped object of its CLR type, and JSON text reads as an element.</summary>
    [Test]
    public async Task ObjectReadsEveryKind()
    {
        var serializer = new SystemTextJsonSerializer();

        _ = await Assert.That(await ProtoEx.ReadAsync<object>(new CacheValue { StringValue = "hello" }, serializer)).IsEqualTo("hello");
        _ = await Assert.That(await ProtoEx.ReadAsync<object>(new CacheValue { BoolValue = true }, serializer) is true).IsTrue();
        _ = await Assert.That(await ProtoEx.ReadAsync<object>(new CacheValue { Int64Value = 22L }, serializer)).IsTypeOf<long>();
        _ = await Assert.That(await ProtoEx.ReadAsync<object>(new CacheValue { DoubleValue = 3.5d }, serializer)).IsEqualTo(3.5d);
        _ = await Assert.That(await ProtoEx.ReadAsync<object>(new CacheValue { NullValue = NullValue.NullValue }, serializer)).IsNull();
        var element = await Assert.That(await ProtoEx.ReadAsync<object>(JsonWire("""{"a":[1,2]}"""), serializer)).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.GetProperty("a").GetArrayLength()).IsEqualTo(2);
    }

    /// <summary>A number from JSON text reads as an element that keeps every digit.</summary>
    [Test]
    public async Task ObjectReadsNumberAsElement()
    {
        var serializer = new SystemTextJsonSerializer();

        var element = await Assert.That(await ProtoEx.ReadAsync<object>(JsonWire("12345678901234567890"), serializer)).IsTypeOf<JsonElement>();

        _ = await Assert.That(element.GetRawText()).IsEqualTo("12345678901234567890");
    }

    /// <summary>Exact typed primitive wire forms decode without the serializer.</summary>
    [Test]
    public async Task TypedReadsExactPrimitives()
    {
        var serializer = new ISquirixSerializerCreateExpectations().Instance();

        _ = await Assert.That(await ProtoEx.ReadAsync<string>(new CacheValue { StringValue = "hello" }, serializer)).IsEqualTo("hello");
        _ = await Assert.That(await ProtoEx.ReadAsync<bool>(new CacheValue { BoolValue = true }, serializer)).IsTrue();
        _ = await Assert.That(await ProtoEx.ReadAsync<int>(new CacheValue { Int64Value = 11L }, serializer)).IsEqualTo(11);
        _ = await Assert.That(await ProtoEx.ReadAsync<long>(new CacheValue { Int64Value = 22L }, serializer)).IsEqualTo(22L);
        _ = await Assert.That(await ProtoEx.ReadAsync<double>(new CacheValue { DoubleValue = 3.25d }, serializer)).IsEqualTo(3.25d);
        _ = await Assert.That(await ProtoEx.ReadAsync<string>(new CacheValue { NullValue = NullValue.NullValue }, serializer)).IsNull();
    }

    /// <summary>A typed cache deserializes JSON text straight from the received bytes.</summary>
    [Test]
    public async Task TypedReadsJsonTextThroughSerializer()
    {
        ClientSerializerMetadata.RegisterContext(CustomCacheValueJsonContext.Default);
        var serializer = new SystemTextJsonSerializer();

        var value = await ProtoEx.ReadAsync<CustomCacheValue>(JsonWire("""{"Count":7,"Name":"dto"}"""), serializer);

        _ = await Assert.That(value!.Count).IsEqualTo(7);
        _ = await Assert.That(value.Name).IsEqualTo("dto");
    }

    /// <summary>A decimal keeps every digit through the wire.</summary>
    [Test]
    public async Task DecimalKeepsPrecision()
    {
        const decimal precise = 12345678901234567.123456789m;
        var serializer = new SystemTextJsonSerializer();

        var wire = ProtoEx.MapEntryToProto(new CacheEntry<decimal> { Value = precise }, serializer);

        _ = await Assert.That(wire.Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(await ProtoEx.ReadAsync<decimal>(wire.Value, serializer)).IsEqualTo(precise);
    }

    /// <summary>A scalar read by a type that is not its native one is deserialized through the serializer.</summary>
    [Test]
    public async Task MismatchedScalarUsesSerializer()
    {
        var serializer = new SystemTextJsonSerializer();

        _ = await Assert.That(await ProtoEx.ReadAsync<double>(new CacheValue { Int64Value = 5L }, serializer)).IsEqualTo(5d);
        _ = await Assert.That(await ProtoEx.ReadAsync<decimal>(new CacheValue { Int64Value = 9_007_199_254_740_993L }, serializer)).IsEqualTo(9_007_199_254_740_993m);
    }

    /// <summary>A value the server sent without a kind is a protocol failure, not a null.</summary>
    [Test]
    public async Task ValueWithoutKindIsRejected()
    {
        var serializer = new SystemTextJsonSerializer();

        var failure = ExceptionAssert.For<RpcException>().Throws(serializer, static value => _ = ProtoEx.ReadAsync<string>(new CacheValue(), value).AsTask());

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Internal);
    }

    /// <summary>Scalars are written as their native kinds, and an int travels as an int64.</summary>
    [Test]
    public async Task ScalarsWriteNativeKinds()
    {
        var serializer = new SystemTextJsonSerializer();

        _ = await Assert.That(ProtoEx.MapEntryToProto(new CacheEntry<string?> { Value = null }, serializer).Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.NullValue);
        _ = await Assert.That(ProtoEx.MapEntryToProto(new CacheEntry<string> { Value = "s" }, serializer).Value.StringValue).IsEqualTo("s");
        _ = await Assert.That(ProtoEx.MapEntryToProto(new CacheEntry<int> { Value = 0 }, serializer).Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.Int64Value);
        _ = await Assert.That(ProtoEx.MapEntryToProto(new CacheEntry<long> { Value = long.MinValue }, serializer).Value.Int64Value).IsEqualTo(long.MinValue);
        _ = await Assert.That(ProtoEx.MapEntryToProto(new CacheEntry<double> { Value = 0.0 }, serializer).Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.DoubleValue);
        _ = await Assert.That(ProtoEx.MapEntryToProto(new CacheEntry<bool> { Value = false }, serializer).Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.BoolValue);
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

        var wire = ProtoEx.MapEntryToProto(new CacheEntry<JsonElement> { Value = JsonElement.Parse(text) }, serializer);

        _ = await Assert.That(wire.Value.KindCase).IsEqualTo(System.Enum.Parse<CacheValue.KindOneofCase>(kind));
        _ = await Assert.That(!string.Equals(kind, nameof(CacheValue.KindOneofCase.JsonUtf8), StringComparison.Ordinal) || string.Equals(wire.Value.JsonUtf8.ToStringUtf8(), text, StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>An element parsed with comments and trailing commas is written as strict JSON text.</summary>
    [Test]
    public async Task LenientJsonElementWritesStrictText()
    {
        var serializer = new SystemTextJsonSerializer();
        var element = JsonElement.Parse("""{"x":1, /* note */ "y":[true,],}""", new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true });

        var wire = ProtoEx.MapEntryToProto(new CacheEntry<JsonElement> { Value = element }, serializer);

        _ = await Assert.That(wire.Value.JsonUtf8.ToStringUtf8()).IsEqualTo("""{"x":1,"y":[true]}""");
    }

    /// <summary>An object is written as the bytes the serializer produced, without a copy.</summary>
    [Test]
    public async Task ObjectWritesSerializerBytesWithoutCopy()
    {
        var produced = Encoding.UTF8.GetBytes("""{"Count":1,"Name":"n"}""");
        var expectations = new ISquirixSerializerCreateExpectations();
        _ = expectations.Setups.SerializeToUtf8Bytes(Arg.Any<CustomCacheValue?>()).ReturnValue(produced);

        var wire = ProtoEx.MapEntryToProto(new CacheEntry<CustomCacheValue> { Value = new CustomCacheValue { Count = 1, Name = "n" } }, expectations.Instance());

        _ = await Assert.That(wire.Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(wire.Value.JsonUtf8.Memory.Span.Overlaps(produced, out var offset)).IsTrue();
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

        var wire = ProtoEx.MapEntryToProto(entry, serializer);

        // Golden wire metadata: expiry must map to exact protobuf time forms
        // (2026-08-01T10:00:00Z = unix 1785578400, 2 minutes = 120 seconds),
        // verified against the calendar, not against the reverse mapping.
        _ = await Assert.That(wire.Value.StringValue).IsEqualTo("payload");
        _ = await Assert.That(wire.ExpiresUtc.Seconds).IsEqualTo(1785578400L);
        _ = await Assert.That(wire.Expiration.Seconds).IsEqualTo(120L);

        var roundTrip = await ProtoEx.MapProtoEntryToCacheEntryAsync<string>(wire, serializer);

        _ = await Assert.That(roundTrip.Value).IsEqualTo("payload");
        _ = await Assert.That(roundTrip.ExpiresUtc).IsEqualTo(entry.ExpiresUtc);
        _ = await Assert.That(roundTrip.Expiration).IsEqualTo(entry.Expiration);
    }

    /// <summary>A non-normalized server duration is reported as a stable RPC failure instead of an unhandled exception.</summary>
    /// <param name="seconds">The wire seconds.</param>
    /// <param name="nanos">The wire nanoseconds.</param>
    [Test]
    [Arguments(0L, 1_000_000_000)]
    [Arguments(0L, -1_000_000_000)]
    [Arguments(1L, -1)]
    [Arguments(-1L, 1)]
    public async Task MalformedDurationIsRejectedAsync(long seconds, int nanos)
    {
        var duration = new Duration { Seconds = seconds, Nanos = nanos };

        var failure = ExceptionAssert.For<RpcException>().Throws(duration, static value => _ = ProtoEx.ReadDuration(value));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Internal);
    }

    /// <summary>A missing server duration is reported as a stable RPC failure instead of an unhandled exception.</summary>
    [Test]
    public async Task MissingDurationIsRejectedAsync()
    {
        var failure = ExceptionAssert.For<RpcException>().Throws<Duration?>(null, static value => _ = ProtoEx.ReadDuration(value));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Internal);
    }

    /// <summary>A non-normalized server timestamp is reported as a stable RPC failure instead of an unhandled exception.</summary>
    /// <param name="seconds">The wire seconds.</param>
    /// <param name="nanos">The wire nanoseconds.</param>
    [Test]
    [Arguments(0L, 1_000_000_000)]
    [Arguments(0L, -1)]
    [Arguments(-62_135_596_801L, 0)]
    [Arguments(253_402_300_800L, 0)]
    public async Task MalformedTimestampIsRejectedAsync(long seconds, int nanos)
    {
        var timestamp = new Timestamp { Seconds = seconds, Nanos = nanos };

        var failure = ExceptionAssert.For<RpcException>().Throws(timestamp, static value => _ = ProtoEx.ReadTimestamp(value));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Internal);
    }

    /// <summary>A malformed expiration inside an entry fails entry mapping with the stable RPC failure.</summary>
    [Test]
    public async Task MalformedEntryExpirationIsRejectedAsync()
    {
        var serializer = new SystemTextJsonSerializer();
        var wire = ProtoEx.MapEntryToProto(new CacheEntry<string> { Value = "payload" }, serializer);
        wire.ExpiresUtc = new Timestamp { Seconds = 1L, Nanos = -1 };

        var failure = ExceptionAssert.For<RpcException>().Throws(
            (Wire: wire, Serializer: serializer),
            static state => _ = ProtoEx.MapProtoEntryToCacheEntryAsync<string>(state.Wire, state.Serializer).AsTask());

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Internal);
    }

    /// <summary>Boundary timestamps and durations are accepted.</summary>
    [Test]
    public async Task NormalizedBoundariesAreAcceptedAsync()
    {
        _ = await Assert.That(ProtoEx.ReadTimestamp(new Timestamp { Seconds = 253_402_300_799L, Nanos = 999_999_999 }).Year).IsEqualTo(9999);
        _ = await Assert.That(ProtoEx.ReadTimestamp(new Timestamp { Seconds = -62_135_596_800L }).Year).IsEqualTo(1);
        _ = await Assert.That(ProtoEx.ReadDuration(new Duration { Seconds = -2L, Nanos = -500_000_000 })).IsEqualTo(TimeSpan.FromSeconds(-2.5));
    }

    private static CacheValue JsonWire(string json) => new() { JsonUtf8 = ByteString.CopyFromUtf8(json) };
}
