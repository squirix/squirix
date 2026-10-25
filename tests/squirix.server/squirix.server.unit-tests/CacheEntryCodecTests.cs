using System;
using System.Buffers.Binary;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for <see cref="CacheEntryCodec" />.</summary>
[Immutable]
public sealed class CacheEntryCodecTests : ServerUnitTestBase
{
    /// <summary>Length computation matches the documented golden size for a minimal integer entry.</summary>
    [Test]
    public async Task ComputeEncodedLengthMatchesGolden()
    {
        var entry = new NodeCacheEntry<object?>(42, 4);

        // Expires flag (1) + expiration flag (1) + version (8) + empty tags (2) + int64 value (1 + 8).
        _ = await Assert.That(CacheEntryCodec.ComputeEncodedLength(entry)).IsEqualTo(21);
    }

    /// <summary>
    /// A decoded JSON tree has the text the serializer writes for the same tree: property order, numbers, escaped strings, nesting, empty
    /// containers and nulls.
    /// </summary>
    /// <param name="json">The JSON value.</param>
    [Test]
    [Arguments("""{"id":42,"tags":["a","b"],"nested":{"x":-1.5,"y":[true,false,null],"z":{}},"empty":[]}""")]
    [Arguments("""{"text":"café <b>&amp;</b> \"quoted\" \\ \n tab\t emoji 😀","big":9007199254740993,"small":-9223372036854775808}""")]
    [Arguments("""[1,2.25,"s",[[]],{"k":null}]""")]
    [Arguments("""{"a":1,"b":2,"a":3}""")]
    public async Task DecodedJsonMatchesSerializer(string json)
    {
        var decoded = RoundTripJson(json);

        _ = await Assert.That(decoded.GetRawText()).IsEqualTo(JsonNode.Parse(json)!.ToJsonString());
    }

    /// <summary>A decoded tree nested deeper than a JSON reader accepts by default fails to decode without throwing, however deep it is.</summary>
    /// <param name="depth">The nesting depth.</param>
    [Test]
    [Arguments(65)]
    [Arguments(1100)]
    public async Task DeepJsonFailsToDecode(int depth)
    {
        var json = new string('[', depth) + new string(']', depth);

        _ = await Assert.That(DecodeJson(json).ReadOk).IsFalse();
    }

    /// <summary>A decoded object tree nested deeper than a JSON reader accepts by default fails to decode without throwing.</summary>
    [Test]
    public async Task DeepJsonObjectFailsToDecode()
    {
        const int depth = 65;
        var json = new System.Text.StringBuilder().Insert(0, "{\"a\":", depth).ToString() + "1" + new string('}', depth);

        _ = await Assert.That(DecodeJson(json).ReadOk).IsFalse();
    }

    /// <summary>An integer outside the int range does not map into an int cache, and the long range still maps into a long cache.</summary>
    [Test]
    public async Task OutOfRangeIntegerDoesNotMapToInt()
    {
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>((long)int.MaxValue + 1), out _)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>((long)int.MinValue - 1), out _)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>(long.MaxValue), out _)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>((long)int.MaxValue), out var max)).IsTrue();
        _ = await Assert.That(max!.Value).IsEqualTo(int.MaxValue);
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>((long)int.MinValue), out var min)).IsTrue();
        _ = await Assert.That(min!.Value).IsEqualTo(int.MinValue);
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<long>(new NodeCacheEntry<object?>(long.MaxValue), out _)).IsTrue();
    }

    /// <summary>ComputeEncodedLength rejects tag dictionaries exceeding ushort.MaxValue entries.</summary>
    [Test]
    public void EncodedLengthRejectsExcessiveTagCount()
    {
        var tags = EntryTagsKit.CreateCount(65_536);
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(new NodeCacheEntry<object?>(null, tags: tags), static value => CacheEntryCodec.ComputeEncodedLength(value));
    }

    /// <summary>Property names and strings that are not valid UTF-8 decode with replacement characters.</summary>
    [Test]
    public async Task InvalidUtf8DecodesReplaced()
    {
        byte[] encoded =
        [
            0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
            0x07, 0x01, 0x00,
            0x01, 0x00, 0xFF,
            0x02, 0x01, 0x00, 0x00, 0x00, 0xFE,
        ];

        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(encoded, out var entry, out _)).IsTrue();
        var element = await Assert.That(entry!.Value).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.GetProperty("\uFFFD").GetString()).IsEqualTo("\uFFFD");
    }

    /// <summary>Numeric and JsonElement coercions used by typed journal reads succeed.</summary>
    [Test]
    public async Task MapEntryCoercesNumericAndJsonValues()
    {
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>(42L), out var asInt)).IsTrue();
        _ = await Assert.That(asInt!.Value).IsEqualTo(42);

        _ = await Assert.That(CacheEntryCodec.TryMapEntry<long>(new NodeCacheEntry<object?>(99L), out var asLong)).IsTrue();
        _ = await Assert.That(asLong!.Value).IsEqualTo(99L);

        _ = await Assert.That(CacheEntryCodec.TryMapEntry<float>(new NodeCacheEntry<object?>(1.5d), out var asFloat)).IsTrue();
        _ = await Assert.That(asFloat!.Value).IsEqualTo(1.5f);

        _ = await Assert.That(CacheEntryCodec.TryMapEntry<double>(new NodeCacheEntry<object?>(2.5d), out var asDouble)).IsTrue();
        _ = await Assert.That(asDouble!.Value).IsEqualTo(2.5d);

        using var document = JsonDocument.Parse("""{"k":1}""");
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<JsonElement>(new NodeCacheEntry<object?>(document.RootElement.Clone()), out var asJson)).IsTrue();
        _ = await Assert.That(asJson!.Value.GetProperty("k").GetInt32()).IsEqualTo(1);

        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>("nope"), out _)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<string>(new NodeCacheEntry<object?>(null), out var asNull)).IsTrue();
        _ = await Assert.That(asNull!.Value).IsNull();
    }

    /// <summary>A decoded tree nested as deep as a JSON reader accepts by default decodes.</summary>
    [Test]
    public async Task MaxDepthJsonDecodes()
    {
        var decoded = RoundTripJson(new string('[', 64) + new string(']', 64));

        _ = await Assert.That(decoded.ValueKind).IsEqualTo(JsonValueKind.Array);
    }

    /// <summary>Read decodes the golden wire bytes into the minimal integer entry.</summary>
    [Test]
    public async Task ReadReadsGoldenWireBytes()
    {
        byte[] golden =
        [
            0x00, 0x00,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
            0x04, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(golden, out var entry, out var bytesRead)).IsTrue();
        _ = await Assert.That(bytesRead).IsEqualTo(21);
        _ = await Assert.That(entry!.Value).IsEqualTo(42L);
        _ = await Assert.That(entry.Version).IsEqualTo(4);
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
        _ = await Assert.That(entry.Expiration).IsNull();
    }

    /// <summary>TryRead fails on truncated envelopes.</summary>
    [Test]
    public async Task ReadReturnsFalseForTruncatedEnvelope()
    {
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>([], out _, out var bytesRead)).IsFalse();
        _ = await Assert.That(bytesRead).IsEqualTo(0);
    }

    /// <summary>Decimal and byte[] values round-trip through the codec.</summary>
    [Test]
    public async Task RoundTripsDecimalAndByteArrayValues()
    {
        await RoundTripValueAsync(12.5m);
        byte[] payload = [9, 8, 7];
        await RoundTripValueAsync(payload);
    }

    /// <summary>Complex JSON values round-trip through the codec as JsonElement trees.</summary>
    [Test]
    public async Task RoundTripsJsonElementValue()
    {
        using var document = JsonDocument.Parse("""{"id":42,"tags":["a","b"]}""");
        var entry = new NodeCacheEntry<object?> { Value = document.RootElement.Clone(), Version = 2 };
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        var capture = new DecodedCapture();
        BufferKit.WithBuffer(
            length,
            (entry, capture),
            static (ctx, buffer) =>
            {
                CacheEntryCodec.Write(ctx.entry, buffer);
                ctx.capture.ReadOk = CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out _);
                ctx.capture.Entry = roundTrip;
            });

        _ = await Assert.That(capture.ReadOk).IsTrue();
        var decodedValue = capture.Entry!.Value;
        var element = await Assert.That(decodedValue).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.GetProperty("id").GetInt32()).IsEqualTo(42);
        _ = await Assert.That(element.GetProperty("tags").GetArrayLength()).IsEqualTo(2);
    }

    /// <summary>Metadata and tags round-trip through the codec.</summary>
    [Test]
    public async Task RoundTripsMetadataAndTags()
    {
        var entry = new NodeCacheEntry<object?>("payload", 3, new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc), TimeSpan.FromMinutes(5), EntryTagsKit.RegionWest);
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        var capture = new DecodedCapture();
        BufferKit.WithBuffer(
            length,
            (entry, capture),
            static (ctx, buffer) =>
            {
                CacheEntryCodec.Write(ctx.entry, buffer);
                ctx.capture.ReadOk = CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out _);
                ctx.capture.Entry = roundTrip;
            });

        _ = await Assert.That(capture.ReadOk).IsTrue();
        var decodedEntry = await Assert.That(capture.Entry).IsNotNull();
        _ = await Assert.That(decodedEntry.Value).IsEqualTo(entry.Value);
        _ = await Assert.That(decodedEntry.ExpiresUtc).IsEqualTo(entry.ExpiresUtc);
        _ = await Assert.That(decodedEntry.Expiration).IsEqualTo(entry.Expiration);
        _ = await Assert.That(decodedEntry.Version).IsEqualTo(entry.Version);
        _ = await Assert.That(decodedEntry.Tags?["region"]).IsEqualTo("west");
    }

    /// <summary>Primitive values round-trip through the codec.</summary>
    /// <param name="value">Value under test.</param>
    [Test]
    [Arguments(null)]
    [Arguments(true)]
    [Arguments(false)]
    [Arguments("hello")]
    [Arguments(42)]
    [Arguments(42L)]
    [Arguments(3.14d)]
    public async Task RoundTripsPrimitiveValues(object? value)
    {
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 7 };
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        var bytes = BufferKit.ToOwnedBytes(length, entry, static (state, buffer) => CacheEntryCodec.Write(state, buffer));

        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(bytes, out var roundTrip, out var bytesRead)).IsTrue();
        _ = await Assert.That(bytesRead).IsEqualTo(bytes.Length);
        _ = await Assert.That(ValueEquals(value, roundTrip!.Value)).IsTrue();
        _ = await Assert.That(roundTrip.Version).IsEqualTo(7);
    }

    /// <summary>Every prefix of an encoded JSON tree fails to decode without throwing.</summary>
    [Test]
    public async Task TruncatedJsonTreeIsRejected()
    {
        using var document = JsonDocument.Parse("""{"a":[1,"two",{"b":null,"c":[true,2.5]}],"d":{"e":"f"}}""");
        var entry = new NodeCacheEntry<object?> { Value = document.RootElement.Clone(), Version = 1 };
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, encoded);

        for (var length = 0; length < encoded.Length; length++)
            _ = await Assert.That(CacheEntryCodec.TryRead<object?>(encoded.AsSpan(0, length), out _, out _)).IsFalse();
    }

    /// <summary>An absolute expiry no DateTimeOffset holds makes TryRead return false rather than throw.</summary>
    [Test]
    public async Task ReadRejectsExpiresOutOfRange()
    {
        var entry = new NodeCacheEntry<object?>(42L, 1, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, encoded);

        // The expires flag opens the envelope and the Unix milliseconds follow it.
        BinaryPrimitives.WriteInt64LittleEndian(encoded.AsSpan(1), long.MaxValue);
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(encoded, out _, out _)).IsFalse();
    }

    /// <summary>Write emits the documented golden wire bytes for a minimal integer entry.</summary>
    [Test]
    public async Task WriteMatchesGoldenWireBytes()
    {
        var entry = new NodeCacheEntry<object?>(42, 4);
        var actual = WriteEntry(entry);

        // No-expiry flag, no-expiration flag, version 4, zero tags,
        // Int64 value-kind tag (4) with 42 little-endian.
        byte[] golden =
        [
            0x00, 0x00,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
            0x04, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        await SequenceAssert.EqualAsync(golden, actual);
        return;

        static byte[] WriteEntry(NodeCacheEntry<object?> value)
        {
            var length = CacheEntryCodec.ComputeEncodedLength(value);
            Span<byte> encoded = stackalloc byte[length];
            CacheEntryCodec.Write(value, encoded);
            var copy = new byte[length];
            encoded.CopyTo(copy);
            return copy;
        }
    }

    /// <summary>Write rejects destinations that are too small for the encoded entry.</summary>
    [Test]
    public void WriteThrowsWhenDestinationIsTooSmall()
    {
        var entry = new NodeCacheEntry<object?> { Value = "abc", Version = 1 };
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(
            entry,
            length - 1,
            static (e, tooSmall) =>
            {
                Span<byte> destination = stackalloc byte[tooSmall];
                CacheEntryCodec.Write(e, destination);
            });
    }

    private static JsonElement RoundTripJson(string json)
    {
        var capture = DecodeJson(json);

        return capture is { ReadOk: true, Entry.Value: JsonElement element }
            ? element
            : throw new InvalidOperationException("The JSON value did not decode.");
    }

    private static DecodedCapture DecodeJson(string json)
    {
        using var document = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = 2048 });
        var entry = new NodeCacheEntry<object?> { Value = document.RootElement.Clone(), Version = 1 };
        var capture = new DecodedCapture();
        BufferKit.WithBuffer(
            CacheEntryCodec.ComputeEncodedLength(entry),
            (entry, capture),
            static (ctx, buffer) =>
            {
                CacheEntryCodec.Write(ctx.entry, buffer);
                ctx.capture.ReadOk = CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out _);
                ctx.capture.Entry = roundTrip;
            });

        return capture;
    }

    private static async Task RoundTripValueAsync(object? value)
    {
        var entry = new NodeCacheEntry<object?> { Value = value, Version = 4 };
        var length = CacheEntryCodec.ComputeEncodedLength(entry);
        var capture = new DecodedCapture();
        BufferKit.WithBuffer(
            length,
            (entry, capture),
            static (ctx, buffer) =>
            {
                CacheEntryCodec.Write(ctx.entry, buffer);
                ctx.capture.ReadOk = CacheEntryCodec.TryRead<object?>(buffer, out var roundTrip, out _);
                ctx.capture.Entry = roundTrip;
            });

        _ = await Assert.That(capture.ReadOk).IsTrue();
        var decodedValue = capture.Entry!.Value;
        if (value is byte[] expectedBytes)
        {
            var decodedBytes = (await Assert.That(decodedValue).IsTypeOf<byte[]>())!;
            await SequenceAssert.EqualAsync(expectedBytes, decodedBytes);
        }
        else
        {
            _ = await Assert.That(decodedValue).IsEqualTo(value);
        }
    }

    private static bool ValueEquals(object? expected, object? actual) => expected switch
    {
        int i when actual is long l => i == l,
        int i when actual is int j => i == j,
        long l when actual is long r => l == r,
        double d when actual is double r => Math.Abs(d - r) < 0.0001,
        _ => Equals(expected, actual),
    };

    private sealed class DecodedCapture
    {
        public NodeCacheEntry<object?>? Entry { get; set; }

        public bool ReadOk { get; set; }
    }
}
