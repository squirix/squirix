using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
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

/// <summary>Unit tests for how <see cref="CacheEntryCodec" /> stores JSON tree strings and property names.</summary>
[Immutable]
public sealed class CacheEntryJsonTreeStringTests : ServerUnitTestBase
{
    private const byte ObjectKind = 7;
    private const byte StringKind = 2;

    /// <summary>
    /// String values and property names are stored as their unescaped UTF-8 whether the source JSON spelled them with escape sequences,
    /// with literal non-ASCII characters, or plainly; the stored bytes never carry the escaped spelling.
    /// </summary>
    /// <param name="json">The JSON value.</param>
    [Test]
    [Arguments("""{"k":"say \"hi\""}""")]
    [Arguments("""{"k":"back\\slash"}""")]
    [Arguments("""{"k":"line\nbreak\ttab"}""")]
    [Arguments("{\"k\":\"caf\\u00e9\"}")]
    [Arguments("{\"k\":\"\\ud83d\\ude00\"}")]
    [Arguments("""{"k":"café 😀"}""")]
    [Arguments("{\"q\\\"uote\":\"1\",\"back\\\\slash\":\"2\",\"new\\nline\":\"3\",\"caf\\u00e9\":\"4\",\"\\ud83d\\ude00\":\"5\"}")]
    [Arguments("""{"café":"x","😀":"y"}""")]
    [Arguments("{\"plain\":\"plain\",\"esc\\n\":\"esc\\n\",\"caf\\u00e9\":\"café\",\"name\":\"\\u00e9\",\"\":\"\",\"other\":\"\"}")]
    [Arguments("{\"outer\":{\"in\\u00e9\":\"v\\\\\"},\"empty\":{\"\":\"\"}}")]
    public async Task JsonTreeStringsEncodeAsUnescapedUtf8(string json)
    {
        using var document = JsonDocument.Parse(json);
        var element = document.RootElement;
        var expected = ExpectedTreeBytes(element);

        var length = CacheEntryCodec.ComputeValueLength(element);
        var actual = new byte[length];
        CacheEntryCodec.WriteValue(element, actual);

        _ = await Assert.That(length).IsEqualTo(expected.Length);
        await SequenceAssert.EqualAsync(expected, actual);
        _ = await Assert.That(RoundTrip(json).GetRawText()).IsEqualTo(JsonNode.Parse(json)!.ToJsonString());
    }

    /// <summary>A root string value, empty or escaped, is stored as its unescaped UTF-8.</summary>
    /// <param name="json">The JSON string literal.</param>
    [Test]
    [Arguments("\"\"")]
    [Arguments("\"\\u00e9\\\\\\n\\ud83d\\ude00\"")]
    [Arguments("\"café 😀\"")]
    public async Task JsonTreeRootStringEncodesAsUnescapedUtf8(string json)
    {
        using var document = JsonDocument.Parse(json);
        var element = document.RootElement;
        var expected = ExpectedTreeBytes(element);

        var length = CacheEntryCodec.ComputeValueLength(element);
        var actual = new byte[length];
        CacheEntryCodec.WriteValue(element, actual);

        _ = await Assert.That(length).IsEqualTo(expected.Length);
        await SequenceAssert.EqualAsync(expected, actual);
        _ = await Assert.That(RoundTripValue(json)).IsEqualTo(element.GetString());
    }

    private static void AppendExpected(ArrayBufferWriter<byte> output, JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.String)
        {
            var text = Encoding.UTF8.GetBytes(element.GetString() ?? string.Empty);
            AppendByte(output, StringKind);
            AppendUInt32(output, uint.CreateTruncating(text.Length));
            output.Write(text);
            return;
        }

        var count = 0;
        foreach (var unused in element.EnumerateObject())
            count++;

        AppendByte(output, ObjectKind);
        AppendUInt16(output, ushort.CreateTruncating(count));
        foreach (var property in element.EnumerateObject())
        {
            var name = Encoding.UTF8.GetBytes(property.Name);
            AppendUInt16(output, ushort.CreateTruncating(name.Length));
            output.Write(name);
            AppendExpected(output, property.Value);
        }
    }

    private static void AppendByte(ArrayBufferWriter<byte> output, byte value)
    {
        output.GetSpan(1)[0] = value;
        output.Advance(1);
    }

    private static void AppendUInt16(ArrayBufferWriter<byte> output, ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(output.GetSpan(sizeof(ushort)), value);
        output.Advance(sizeof(ushort));
    }

    private static void AppendUInt32(ArrayBufferWriter<byte> output, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(output.GetSpan(sizeof(uint)), value);
        output.Advance(sizeof(uint));
    }

    private static byte[] ExpectedTreeBytes(JsonElement element)
    {
        var output = new ArrayBufferWriter<byte>();
        AppendExpected(output, element);
        var bytes = new byte[output.WrittenCount];
        output.WrittenSpan.CopyTo(bytes);
        return bytes;
    }

    private static JsonElement RoundTrip(string json) => RoundTripValue(json) is JsonElement element
        ? element
        : throw new InvalidOperationException("The JSON value did not decode to a tree.");

    private static object? RoundTripValue(string json)
    {
        using var document = JsonDocument.Parse(json);
        var entry = new NodeCacheEntry<object?> { Value = document.RootElement.Clone(), Version = 1 };
        var buffer = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, buffer);
        return CacheEntryCodec.TryRead<object?>(buffer, out var decoded, out _)
            ? decoded?.Value
            : throw new InvalidOperationException("The JSON value did not decode.");
    }
}
