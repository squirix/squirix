using System;
using System.Globalization;
using System.Text;
using System.Text.Json;
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

/// <summary>Unit tests for the JSON value kind of <see cref="CacheEntryCodec" />.</summary>
[Immutable]
public sealed class CacheEntryJsonValueTests : ServerUnitTestBase
{
    private const int WideObjectPropertyCount = 70_000;

    /// <summary>A JSON value decodes to exactly the bytes it was encoded from: property order, duplicate names, white space, number text and escapes are kept.</summary>
    /// <param name="json">The JSON value.</param>
    [Test]
    [Arguments("""{"id":42,"tags":["a","b"],"nested":{"x":-1.5,"y":[true,false,null],"z":{}},"empty":[]}""")]
    [Arguments("""{"text":"café <b>&amp;</b> \"quoted\" \\ \n tab\t emoji 😀","big":9007199254740993,"small":-9223372036854775808}""")]
    [Arguments("""[1,2.25,"s",[[]],{"k":null}]""")]
    [Arguments("""{"a":1,"b":2,"a":3}""")]
    [Arguments("""  { "spaced" :  [ 1 , 2 ]  }  """)]
    [Arguments("12345678901234567890")]
    [Arguments("1.2345678901234567890123")]
    [Arguments("1E+400")]
    public async Task DecodedJsonKeepsInputBytes(string json)
    {
        var source = JsonValueKit.Create(json);

        var decoded = RoundTrip(source);

        await SequenceAssert.EqualMemoryAsync(source.Bytes, decoded.Bytes);
    }

    /// <summary>A decoded value owns its bytes: clearing the buffer it was read from does not change it.</summary>
    [Test]
    public async Task DecodedJsonDoesNotShareTheSourceBuffer()
    {
        const string text = """{"owned":true}""";
        var buffer = JsonValueKit.Encode(new NodeCacheEntry<object?> { Value = JsonValueKit.Create(text), Version = 1 });
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(buffer, out var decoded, out _)).IsTrue();

        buffer.AsSpan().Clear();

        var json = (await Assert.That(decoded!.Value).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(Encoding.UTF8.GetString(json.Bytes.Span)).IsEqualTo(text);
    }

    /// <summary>JSON bytes that are not valid UTF-8 do not decode.</summary>
    [Test]
    public async Task InvalidUtf8JsonIsRejected()
    {
        byte[] json = [0x22, 0xFF, 0x22];

        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(JsonValueKit.EncodeEntryAround(json), out _, out _)).IsFalse();
    }

    /// <summary>The computed length of a JSON value entry is the number of bytes written, and the kind, the length and the bytes follow the tags.</summary>
    [Test]
    public async Task JsonValueEncodesAsKindLengthAndBytes()
    {
        var source = JsonValueKit.Create("""{"a":1}""");
        var entry = new NodeCacheEntry<object?> { Value = source, Version = 1 };

        var encoded = JsonValueKit.Encode(entry);

        _ = await Assert.That(encoded.Length).IsEqualTo(CacheEntryCodec.ComputeEncodedLength(entry));
        await SequenceAssert.EqualAsync(JsonValueKit.EncodeEntryAround("""{"a":1}"""u8), encoded);
    }

    /// <summary>A value nested as deep as a JSON reader accepts by default decodes.</summary>
    [Test]
    public async Task MaxDepthJsonDecodes()
    {
        var json = new string('[', 64) + new string(']', 64);

        var decoded = RoundTrip(JsonValueKit.Create(json));

        _ = await Assert.That(decoded.Length).IsEqualTo(json.Length);
    }

    /// <summary>Numbers beyond the range of a long and of a double keep every digit through the codec.</summary>
    /// <param name="text">The number text.</param>
    [Test]
    [Arguments("12345678901234567890")]
    [Arguments("1.2345678901234567890123")]
    [Arguments("-9223372036854775809")]
    public async Task NumberElementKeepsPrecision(string text)
    {
        var entry = new NodeCacheEntry<object?> { Value = JsonElement.Parse(text), Version = 1 };

        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(JsonValueKit.Encode(entry), out var decoded, out _)).IsTrue();

        var json = (await Assert.That(decoded!.Value).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(Encoding.UTF8.GetString(json.Bytes.Span)).IsEqualTo(text);
    }

    /// <summary>An object with more properties than a 16-bit count holds round-trips whole.</summary>
    [Test]
    public async Task WideObjectRoundTrips()
    {
        var builder = new StringBuilder("{");
        for (var i = 0; i < WideObjectPropertyCount; i++)
        {
            var number = i.ToString(CultureInfo.InvariantCulture);
            _ = builder.Append(i == 0 ? string.Empty : ",").Append("\"p").Append(number).Append("\":").Append(number);
        }

        var source = JsonValueKit.Create(builder.Append('}').ToString());

        var decoded = RoundTrip(source);

        _ = await Assert.That(decoded.Length).IsEqualTo(source.Length);
        await SequenceAssert.EqualMemoryAsync(source.Bytes, decoded.Bytes);
    }

    /// <summary>An object element is stored as its own text and decodes as a JSON value with the same bytes.</summary>
    [Test]
    public async Task ObjectElementDecodesAsJsonValue()
    {
        const string text = """{"id":42,"tags":["a","b"]}""";
        var entry = new NodeCacheEntry<object?> { Value = JsonElement.Parse(text), Version = 2 };

        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(JsonValueKit.Encode(entry), out var decoded, out _)).IsTrue();

        var json = (await Assert.That(decoded!.Value).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(Encoding.UTF8.GetString(json.Bytes.Span)).IsEqualTo(text);
    }

    /// <summary>A scalar element is stored as the primitive it holds.</summary>
    /// <param name="text">The JSON text of the element.</param>
    /// <param name="expected">The primitive it decodes as.</param>
    [Test]
    [Arguments("\"plain\"", "plain")]
    [Arguments("true", true)]
    [Arguments("false", false)]
    [Arguments("null", null)]
    public async Task ScalarElementsDecodeAsPrimitives(string text, object? expected)
    {
        var entry = new NodeCacheEntry<object?> { Value = JsonElement.Parse(text), Version = 1 };

        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(JsonValueKit.Encode(entry), out var decoded, out _)).IsTrue();

        _ = await Assert.That(decoded!.Value).IsEqualTo(expected);
    }

    /// <summary>Every prefix of an encoded JSON value fails to decode without throwing.</summary>
    [Test]
    public async Task TruncatedJsonValueIsRejected()
    {
        var encoded = JsonValueKit.Encode(new NodeCacheEntry<object?> { Value = JsonValueKit.Create("""{"a":[1,"two",{"b":null,"c":[true,2.5]}],"d":{"e":"f"}}"""), Version = 1 });

        for (var length = 0; length < encoded.Length; length++)
            _ = await Assert.That(CacheEntryCodec.TryRead<object?>(encoded.AsSpan(0, length), out _, out _)).IsFalse();
    }

    /// <summary>Value kinds that no encoding assigns do not decode.</summary>
    /// <param name="kind">The value kind byte.</param>
    [Test]
    [Arguments(8)]
    [Arguments(9)]
    [Arguments(255)]
    public async Task UnassignedValueKindIsRejected(int kind)
    {
        var encoded = JsonValueKit.EncodeEntryAround("{}"u8);
        encoded[^(2 + sizeof(uint) + 1)] = byte.CreateChecked(kind);

        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(encoded, out _, out _)).IsFalse();
    }

    private static JsonUtf8Value RoundTrip(JsonUtf8Value source)
    {
        var encoded = JsonValueKit.Encode(new NodeCacheEntry<object?> { Value = source, Version = 1 });
        return CacheEntryCodec.TryRead<object?>(encoded, out var decoded, out _) && decoded!.Value is JsonUtf8Value json
            ? json
            : throw new InvalidOperationException("The JSON value did not decode.");
    }
}
