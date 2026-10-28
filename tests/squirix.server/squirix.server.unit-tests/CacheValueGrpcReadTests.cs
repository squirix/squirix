using System;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Maps cache values from the wire into stored values.</summary>
[Immutable]
public sealed class CacheValueGrpcReadTests : ServerUnitTestBase
{
    private const string ObjectText = """{"Id":5,"Name":"multi","Tags":["t"]}""";

    /// <summary>An object cache reads every scalar kind as the CLR value of that kind, and an int travels back as a long.</summary>
    [Test]
    public async Task ObjectReadsScalarKinds()
    {
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(new CacheValue { StringValue = "obj" })).IsEqualTo("obj");
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(new CacheValue { BoolValue = true }) is true).IsTrue();
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(new CacheValue { Int64Value = 9L })).IsTypeOf<long>();
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(new CacheValue { DoubleValue = 2.5d })).IsEqualTo(2.5d);
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<object>(new CacheValue { NullValue = NullValue.NullValue })).IsNull();
        var intEntry = await new NodeCacheEntry<object?> { Value = 3, Version = 1 }.MapToProto().MapFromProtoAsync<object?>();
        _ = await Assert.That(intEntry.Value).IsTypeOf<long>();
        _ = await Assert.That(intEntry.Value).IsEqualTo(3L);
    }

    /// <summary>An object cache keeps JSON text as a value over the request bytes, not as a parsed or copied document.</summary>
    [Test]
    public async Task ObjectKeepsJsonTextAsValue()
    {
        var wire = JsonValueKit.Wire(ObjectText);

        var read = await ServerProtoEx.MapCacheValueAsync<object>(wire);

        var value = (await Assert.That(read).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(Encoding.UTF8.GetString(value.Bytes.Span)).IsEqualTo(ObjectText);
        _ = await Assert.That(value.Bytes.Span.Overlaps(wire.JsonUtf8.Memory.Span, out var offset)).IsTrue();
        _ = await Assert.That(offset).IsEqualTo(0);
    }

    /// <summary>Text that is not exactly one valid JSON value is an invalid argument, and nothing is stored.</summary>
    /// <param name="json">The text sent as the JSON kind.</param>
    [Test]
    [Arguments("")]
    [Arguments("{\"a\":1} {\"b\":2}")]
    [Arguments("{\"a\":1,}")]
    [Arguments("// c\n{}")]
    [Arguments("NaN")]
    [Arguments("{\"a\":")]
    public async Task InvalidJsonIsInvalidArgument(string json)
    {
        var entry = new CacheEntryWire { Value = JsonValueKit.Wire(json) };

        var failure = NodeExceptionAssert.For<RpcException>().Throws(entry, static value => _ = value.MapFromProtoAsync<object?>().AsTask());

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>JSON bytes with a byte order mark or invalid UTF-8, and nesting beyond 64 levels, are invalid arguments.</summary>
    [Test]
    public async Task MalformedJsonBytesAreInvalidArguments()
    {
        var bom = new CacheEntryWire { Value = new CacheValue { JsonUtf8 = ByteString.CopyFrom(0xEF, 0xBB, 0xBF, 0x7B, 0x7D) } };
        var utf8 = new CacheEntryWire { Value = new CacheValue { JsonUtf8 = ByteString.CopyFrom(0x22, 0xFF, 0x22) } };
        var deep = new CacheEntryWire { Value = JsonValueKit.Wire(new string('[', 65) + new string(']', 65)) };

        foreach (var entry in new[] { bom, utf8, deep })
        {
            var failure = NodeExceptionAssert.For<RpcException>().Throws(entry, static value => _ = value.MapFromProtoAsync<object?>().AsTask());
            _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        }
    }

    /// <summary>An entry without a value, and a value without a kind, are invalid arguments.</summary>
    [Test]
    public async Task MissingValueIsInvalidArgument()
    {
        foreach (var entry in new[] { new CacheEntryWire(), new CacheEntryWire { Value = new CacheValue() } })
        {
            var failure = NodeExceptionAssert.For<RpcException>().Throws(entry, static value => _ = value.MapFromProtoAsync<object?>().AsTask());
            _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        }
    }

    /// <summary>A typed cache reads exact scalar kinds directly.</summary>
    [Test]
    public async Task TypedReadsExactScalars()
    {
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<string>(new CacheValue { StringValue = "hello" })).IsEqualTo("hello");
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<bool>(new CacheValue { BoolValue = true })).IsTrue();
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<long>(new CacheValue { Int64Value = 99L })).IsEqualTo(99L);
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<int>(new CacheValue { Int64Value = 7L })).IsEqualTo(7);
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<double>(new CacheValue { DoubleValue = 1.5d })).IsEqualTo(1.5d);
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<string>(CacheValueWireNull())).IsNull();
    }

    /// <summary>A typed cache deserializes an object from JSON text.</summary>
    [Test]
    public async Task TypedReadsObjectFromJson()
    {
        var payload = await ServerProtoEx.MapCacheValueAsync<SamplePayload>(JsonValueKit.Wire(ObjectText));

        _ = await Assert.That(payload).IsNotNull();
        _ = await Assert.That(payload!.Id).IsEqualTo(5);
        _ = await Assert.That(payload.Name).IsEqualTo("multi");
        await SequenceAssert.EqualAsync(["t"], payload.Tags, StringComparer.Ordinal);
    }

    /// <summary>A parsed request copies its bytes fields, so the stored value stays the same when the receive buffer is reused.</summary>
    [Test]
    public async Task StoredJsonValueSurvivesBufferOverwrite()
    {
        var buffer = new CacheEntryWire { Value = JsonValueKit.Wire(ObjectText) }.ToByteArray();
        var parsed = CacheEntryWire.Parser.ParseFrom(buffer);
        var entry = await parsed.MapFromProtoAsync<object?>();

        buffer.AsSpan().Fill(byte.MaxValue);

        var json = (await Assert.That(entry.Value).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(Encoding.UTF8.GetString(json.Bytes.Span)).IsEqualTo(ObjectText);
    }

    /// <summary>A typed cache of objects with a property named value round-trips the object, not a scalar.</summary>
    [Test]
    public async Task SingleValuePropertyObjectRoundTrips()
    {
        var source = new NodeCacheEntry<ValuePayload> { Value = new ValuePayload { Value = "x" }, Version = 1 };

        var roundTrip = await source.MapToProto().MapFromProtoAsync<ValuePayload>();

        _ = await Assert.That(roundTrip.Value!.Value).IsEqualTo("x");
    }

    /// <summary>A complex object round-trips with its deadlines.</summary>
    [Test]
    public async Task ComplexObjectRoundTripsWithDeadlines()
    {
        var source = new NodeCacheEntry<SamplePayload>
        {
            Value = new SamplePayload { Id = 7, Name = "alpha", Tags = ["a", "b"] },
            Version = 3,
            ExpiresUtc = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc),
            Expiration = TimeSpan.FromMinutes(5),
        };

        var roundTrip = await source.MapToProto().MapFromProtoAsync<SamplePayload>();

        _ = await Assert.That(roundTrip.Value!.Id).IsEqualTo(7);
        _ = await Assert.That(roundTrip.Value.Name).IsEqualTo("alpha");
        await SequenceAssert.EqualAsync(["a", "b"], roundTrip.Value.Tags, StringComparer.Ordinal);
        _ = await Assert.That(roundTrip.ExpiresUtc).IsEqualTo(source.ExpiresUtc);
        _ = await Assert.That(roundTrip.Expiration).IsEqualTo(source.Expiration);
    }

    /// <summary>A number element keeps every digit through the wire and back to an element.</summary>
    /// <param name="text">The JSON text of the number.</param>
    [Test]
    [Arguments("9007199254740993")]
    [Arguments("12345678901234567890")]
    [Arguments("1.2345678901234567890123")]
    [Arguments("-0.0")]
    public async Task NumberElementKeepsDigits(string text)
    {
        var wire = new NodeCacheEntry<JsonElement> { Value = JsonElement.Parse(text), Version = 1 }.MapToProto();

        var roundTrip = await wire.MapFromProtoAsync<JsonElement>();

        _ = await Assert.That(roundTrip.Value.GetRawText()).IsEqualTo(text);
    }

    /// <summary>Decimals keep every digit through the wire.</summary>
    [Test]
    public async Task DecimalKeepsPrecision()
    {
        const decimal precise = 12345678901234567.123456789m;
        var wire = new NodeCacheEntry<decimal> { Value = precise, Version = 1 }.MapToProto();

        _ = await Assert.That((await wire.MapFromProtoAsync<decimal>()).Value).IsEqualTo(precise);
        var asObject = await wire.MapFromProtoAsync<object?>();
        var json = (await Assert.That(asObject.Value).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(json.ToJsonElement().GetDecimal()).IsEqualTo(precise);
    }

    /// <summary>A scalar read by a type that is not its native one is deserialized through JSON.</summary>
    [Test]
    public async Task MismatchedScalarFallsBackThroughJson()
    {
        const long big = 9_007_199_254_740_993L;

        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<decimal>(new CacheValue { Int64Value = big })).IsEqualTo(big);
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<double>(new CacheValue { Int64Value = 3L })).IsEqualTo(3d);
        var element = await ServerProtoEx.MapCacheValueAsync<JsonElement>(new CacheValue { Int64Value = big });
        _ = await Assert.That(element.GetRawText()).IsEqualTo(big.ToString(CultureInfo.InvariantCulture));
        _ = await Assert.That(await ServerProtoEx.MapCacheValueAsync<JsonElement>(new CacheValue { StringValue = "s" })).IsNotDefault();
    }

    /// <summary>An int64 outside the int range does not read as an int.</summary>
    [Test]
    public void OutOfRangeIntegerDoesNotReadAsInt() =>
        _ = NodeExceptionAssert.For<JsonException>().Throws(new CacheValue { Int64Value = int.MaxValue + 1L }, static wire => _ = ServerProtoEx.MapCacheValueAsync<int>(wire).AsTask());

    /// <summary>Zero timestamps are ignored and a relative expiration is kept.</summary>
    [Test]
    public async Task ZeroTimestampIsIgnored()
    {
        var wire = new CacheEntryWire
        {
            Value = new CacheValue { StringValue = "exp" },
            ExpiresUtc = new Timestamp { Seconds = 0, Nanos = 0 },
            Expiration = Duration.FromTimeSpan(TimeSpan.FromSeconds(9)),
        };

        var entry = await wire.MapFromProtoAsync<string>();

        _ = await Assert.That(entry.Value).IsEqualTo("exp");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
        _ = await Assert.That(entry.Expiration).IsEqualTo(TimeSpan.FromSeconds(9));
    }

    private static CacheValue CacheValueWireNull() => new() { NullValue = NullValue.NullValue };
}
