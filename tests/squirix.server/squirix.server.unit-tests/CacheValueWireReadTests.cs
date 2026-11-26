using System;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Serialization.Wire;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Maps cache values from the wire into stored values.</summary>
[Immutable]
public sealed class CacheValueWireReadTests : ServerUnitTestBase
{
    private const string ObjectText = """{"Id":5,"Name":"multi","Tags":["t"]}""";

    /// <summary>Scalar kinds survive the wire round trip in their inline representation.</summary>
    [Test]
    public async Task StoredScalarsRoundTrip()
    {
        var integer = new NodeCacheEntry<StoredValue>(StoredValue.FromInteger(long.MinValue)).MapToWire().MapFromWire<StoredValue>().Value;
        _ = await Assert.That(integer.Kind).IsEqualTo(StoredValue.ValueKind.Integer);
        _ = await Assert.That(integer.Integer).IsEqualTo(long.MinValue);
        _ = await Assert.That(ServerWireMapping.MapCacheValue<StoredValue>(new CacheValue { DoubleValue = 2.5d }).Double).IsEqualTo(2.5d);
        _ = await Assert.That(ServerWireMapping.MapCacheValue<StoredValue>(new CacheValue { BoolValue = true }).Boolean).IsTrue();
        _ = await Assert.That(ServerWireMapping.MapCacheValue<StoredValue>(CacheValueWireNull()).Kind).IsEqualTo(StoredValue.ValueKind.Null);
    }

    /// <summary>A JSON value keeps the full owned request array and its original text.</summary>
    [Test]
    public async Task StoredJsonReusesOwnedBytes()
    {
        var wire = JsonValueKit.Wire(ObjectText);
        var value = ServerWireMapping.MapCacheValue<StoredValue>(wire);
        _ = await Assert.That(value.Kind).IsEqualTo(StoredValue.ValueKind.Json);
        _ = await Assert.That(Encoding.UTF8.GetString(value.Bytes)).IsEqualTo(ObjectText);
        _ = await Assert.That(wire.JsonUtf8.Span.Overlaps(value.Bytes, out var offset)).IsTrue();
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

        var failure = NodeExceptionAssert.For<ServerCallException>().Throws(entry, static value => _ = value.MapFromWire<StoredValue>());

        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
    }

    /// <summary>JSON bytes with a byte order mark or invalid UTF-8, and nesting beyond 64 levels, are invalid arguments.</summary>
    [Test]
    public async Task MalformedJsonBytesAreInvalidArguments()
    {
        var bom = new CacheEntryWire { Value = new CacheValue { JsonUtf8 = new byte[] { 0xEF, 0xBB, 0xBF, 0x7B, 0x7D } } };
        var utf8 = new CacheEntryWire { Value = new CacheValue { JsonUtf8 = new byte[] { 0x22, 0xFF, 0x22 } } };
        var deep = new CacheEntryWire { Value = JsonValueKit.Wire(new string('[', 65) + new string(']', 65)) };

        foreach (var entry in new[] { bom, utf8, deep })
        {
            var failure = NodeExceptionAssert.For<ServerCallException>().Throws(entry, static value => _ = value.MapFromWire<StoredValue>());
            _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        }
    }

    /// <summary>An entry without a value, and a value without a kind, are invalid arguments.</summary>
    [Test]
    public async Task MissingValueIsInvalidArgument()
    {
        foreach (var entry in new[] { new CacheEntryWire(), new CacheEntryWire { Value = new CacheValue() } })
        {
            var failure = NodeExceptionAssert.For<ServerCallException>().Throws(entry, static value => _ = value.MapFromWire<StoredValue>());
            _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        }
    }

    /// <summary>A typed cache reads exact scalar kinds directly.</summary>
    [Test]
    public async Task TypedReadsExactScalars()
    {
        _ = await Assert.That(ServerWireMapping.MapCacheValue<string>(new CacheValue { StringValue = "hello" })).IsEqualTo("hello");
        _ = await Assert.That(ServerWireMapping.MapCacheValue<bool>(new CacheValue { BoolValue = true })).IsTrue();
        _ = await Assert.That(ServerWireMapping.MapCacheValue<long>(new CacheValue { Int64Value = 99L })).IsEqualTo(99L);
        _ = await Assert.That(ServerWireMapping.MapCacheValue<int>(new CacheValue { Int64Value = 7L })).IsEqualTo(7);
        _ = await Assert.That(ServerWireMapping.MapCacheValue<double>(new CacheValue { DoubleValue = 1.5d })).IsEqualTo(1.5d);
        _ = await Assert.That(ServerWireMapping.MapCacheValue<string>(CacheValueWireNull())).IsNull();
    }

    /// <summary>A typed cache deserializes an object from JSON text.</summary>
    [Test]
    public async Task TypedReadsObjectFromJson()
    {
        var payload = ServerWireMapping.MapCacheValue<SamplePayload>(JsonValueKit.Wire(ObjectText));

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
        var entry = parsed.MapFromWire<StoredValue>();

        buffer.AsSpan().Fill(byte.MaxValue);

        var json = entry.Value;
        _ = await Assert.That(json.Kind).IsEqualTo(StoredValue.ValueKind.Json);
        _ = await Assert.That(Encoding.UTF8.GetString(json.Bytes)).IsEqualTo(ObjectText);
    }

    /// <summary>A typed cache of objects with a property named value round-trips the object, not a scalar.</summary>
    [Test]
    public async Task SingleValuePropertyObjectRoundTrips()
    {
        var source = new NodeCacheEntry<ValuePayload> { Value = new ValuePayload { Value = "x" }, Version = 1 };

        var roundTrip = source.MapToWire().MapFromWire<ValuePayload>();

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

        var roundTrip = source.MapToWire().MapFromWire<SamplePayload>();

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
        var wire = new NodeCacheEntry<JsonElement> { Value = JsonElement.Parse(text), Version = 1 }.MapToWire();

        var roundTrip = wire.MapFromWire<JsonElement>();

        _ = await Assert.That(roundTrip.Value.GetRawText()).IsEqualTo(text);
    }

    /// <summary>Decimals keep every digit through the wire.</summary>
    [Test]
    public async Task DecimalKeepsPrecision()
    {
        const decimal precise = 12345678901234567.123456789m;
        var wire = new NodeCacheEntry<decimal> { Value = precise, Version = 1 }.MapToWire();

        _ = await Assert.That(wire.MapFromWire<decimal>().Value).IsEqualTo(precise);
        var stored = wire.MapFromWire<StoredValue>().Value;
        _ = await Assert.That(stored.Kind).IsEqualTo(StoredValue.ValueKind.Json);
        _ = await Assert.That(JsonElement.Parse(stored.Bytes).GetDecimal()).IsEqualTo(precise);
    }

    /// <summary>A scalar read by a type that is not its native one is deserialized through JSON.</summary>
    [Test]
    public async Task MismatchedScalarFallsBackThroughJson()
    {
        const long big = 9_007_199_254_740_993L;

        _ = await Assert.That(ServerWireMapping.MapCacheValue<decimal>(new CacheValue { Int64Value = big })).IsEqualTo(big);
        _ = await Assert.That(ServerWireMapping.MapCacheValue<double>(new CacheValue { Int64Value = 3L })).IsEqualTo(3d);
        var element = ServerWireMapping.MapCacheValue<JsonElement>(new CacheValue { Int64Value = big });
        _ = await Assert.That(element.GetRawText()).IsEqualTo(big.ToString(CultureInfo.InvariantCulture));
        _ = await Assert.That(ServerWireMapping.MapCacheValue<JsonElement>(new CacheValue { StringValue = "s" })).IsNotDefault();
    }

    /// <summary>An int64 outside the int range does not read as an int.</summary>
    [Test]
    public void OutOfRangeIntegerDoesNotReadAsInt() =>
        _ = NodeExceptionAssert.For<JsonException>().Throws(new CacheValue { Int64Value = int.MaxValue + 1L }, static wire => _ = ServerWireMapping.MapCacheValue<int>(wire));

    /// <summary>An explicit epoch deadline and a relative expiration are both preserved.</summary>
    [Test]
    public async Task EpochTimestampIsPreservedTest()
    {
        var wire = new CacheEntryWire
        {
            Value = new CacheValue { StringValue = "exp" },
            ExpiresUtc = DateTime.UnixEpoch,
            Expiration = TimeSpan.FromSeconds(9),
        };

        var entry = wire.MapFromWire<string>();

        _ = await Assert.That(entry.Value).IsEqualTo("exp");
        _ = await Assert.That(entry.ExpiresUtc).IsEqualTo(DateTime.UnixEpoch);
        _ = await Assert.That(entry.Expiration).IsEqualTo(TimeSpan.FromSeconds(9));
    }

    private static CacheValue CacheValueWireNull() => new() { NullValue = NullValue.NullValue };
}
