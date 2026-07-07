using System;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Encodes and decodes <see cref="JsonElement" /> trees without persisting UTF-8 JSON blobs.</summary>
internal static class BinaryJsonTreeCodec
{
    private const int MaxUtf16StringLength = ushort.MaxValue;

    public static int ComputeEncodedLength(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => 1,
        JsonValueKind.True or JsonValueKind.False => 2,
        JsonValueKind.String => 1 + 4 + Encoding.UTF8.GetByteCount(element.GetString() ?? string.Empty),
        JsonValueKind.Number => 1 + 8,
        JsonValueKind.Object => ComputeObjectLength(element),
        JsonValueKind.Array => ComputeArrayLength(element),
        _ => throw new InvalidDataException("Unsupported JSON value kind for binary tree encoding."),
    };

    public static int Write(JsonElement element, Span<byte> destination)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Null or JsonValueKind.Undefined:
                return WriteNull(destination);
            case JsonValueKind.True or JsonValueKind.False:
                return WriteBool(element.GetBoolean(), destination);
            case JsonValueKind.String:
                return WriteString(element.GetString() ?? string.Empty, destination);
            case JsonValueKind.Number:
                if (TryGetInteger(element, out var integer))
                {
                    destination[0] = ValueKind.Int64;
                    BinaryPrimitives.WriteInt64LittleEndian(destination[1..], integer);
                }
                else
                {
                    destination[0] = ValueKind.Double;
                    BinaryPrimitives.WriteDoubleLittleEndian(destination[1..], element.GetDouble());
                }

                return 1 + sizeof(long);

            case JsonValueKind.Object:
                return WriteObject(element, destination);
            case JsonValueKind.Array:
                return WriteArray(element, destination);
            default:
                throw new InvalidDataException("Unsupported JSON value kind for binary tree encoding.");
        }
    }

    public static bool TryRead(ReadOnlySpan<byte> source, out JsonElement element, out int bytesRead)
    {
        element = default;
        bytesRead = 0;
        if (!TryReadNode(source, out var node, out bytesRead))
            return false;

        element = node is null ? default : JsonSerializer.SerializeToElement(node, BinaryJsonTreeJsonContext.Default.JsonNode);
        return true;
    }

    private static int ComputeArrayLength(JsonElement element)
    {
        var length = 1 + 4;
        foreach (var item in element.EnumerateArray())
            length += ComputeEncodedLength(item);

        return length;
    }

    private static int ComputeObjectLength(JsonElement element)
    {
        var length = 1 + 2;
        foreach (var property in element.EnumerateObject())
        {
            var nameBytes = Encoding.UTF8.GetByteCount(property.Name);
            if (nameBytes > MaxUtf16StringLength)
                throw new InvalidDataException("Object property name exceeds maximum encoded length.");

            length += 2 + nameBytes + ComputeEncodedLength(property.Value);
        }

        return length;
    }

    private static int WriteArray(JsonElement element, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = ValueKind.Array;
        var count = element.GetArrayLength();
        BinaryPrimitives.WriteUInt32LittleEndian(destination[offset..], uint.CreateTruncating(count));
        offset += 4;
        foreach (var item in element.EnumerateArray())
            offset += Write(item, destination[offset..]);

        return offset;
    }

    private static int WriteBool(bool value, Span<byte> destination)
    {
        destination[0] = ValueKind.Bool;
        if (value)
            destination[1] = 1;
        else
            destination[1] = 0;
        return 2;
    }

    private static int WriteNull(Span<byte> destination)
    {
        destination[0] = ValueKind.Null;
        return 1;
    }

    private static int WriteObject(JsonElement element, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = ValueKind.Object;
        ushort propertyCount = 0;
        foreach (var property in element.EnumerateObject())
        {
            _ = property;
            propertyCount++;
        }

        BinaryPrimitives.WriteUInt16LittleEndian(destination[offset..], propertyCount);
        offset += 2;
        foreach (var property in element.EnumerateObject())
        {
            offset += WriteUtf8Prefixed(property.Name, destination[offset..]);
            offset += Write(property.Value, destination[offset..]);
        }

        return offset;
    }

    private static int WriteString(string text, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = ValueKind.String;
        var byteCount = Encoding.UTF8.GetByteCount(text);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[offset..], uint.CreateTruncating(byteCount));
        offset += 4;
        _ = Encoding.UTF8.GetBytes(text, destination[offset..]);
        return offset + byteCount;
    }

    private static int WriteUtf8Prefixed(string text, Span<byte> destination)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        if (byteCount > MaxUtf16StringLength)
            throw new InvalidDataException("Object property name exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, destination[2..]);
        return 2 + byteCount;
    }

    private static bool TryGetInteger(JsonElement element, out long value)
    {
        if (element.TryGetInt64(out value))
            return true;

        if (element.TryGetDouble(out var d) && double.IsInteger(d) && d is >= long.MinValue and <= long.MaxValue)
        {
            value = Convert.ToInt64(d);
            return true;
        }

        value = 0;
        return false;
    }

    private static bool TryReadArray(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 0;
        if (source.Length < 1 + 4)
            return false;

        var count = BinaryPrimitives.ReadUInt32LittleEndian(source[1..]);
        bytesRead = 5;
        var array = new JsonArray();
        for (var i = 0; i < count; i++)
        {
            if (!TryReadNode(source[bytesRead..], out var item, out var itemBytes))
                return false;

            array.Add(item);
            bytesRead += itemBytes;
        }

        node = array;
        return true;
    }

    private static bool TryReadBool(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        node = JsonValue.Create(source[1] is not 0);
        bytesRead = 2;
        return true;
    }

    private static bool TryReadNode(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 0;
        if (source.IsEmpty)
            return false;

        return source[0] switch
        {
            ValueKind.Null => TryReadNull(out node, out bytesRead),
            ValueKind.Bool => TryReadBool(source, out node, out bytesRead),
            ValueKind.String => TryReadString(source, out node, out bytesRead),
            ValueKind.Int64 => TryReadInt64(source, out node, out bytesRead),
            ValueKind.Double => TryReadDouble(source, out node, out bytesRead),
            ValueKind.Decimal => TryReadDecimal(source, out node, out bytesRead),
            ValueKind.Object => TryReadObject(source, out node, out bytesRead),
            ValueKind.Array => TryReadArray(source, out node, out bytesRead),
            _ => false,
        };
    }

    private static bool TryReadInt64(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 0;
        if (source.Length < 1 + 8)
            return false;

        node = JsonValue.Create(BinaryPrimitives.ReadInt64LittleEndian(source[1..]));
        bytesRead = 1 + 8;
        return true;
    }

    private static bool TryReadDouble(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 0;
        if (source.Length < 1 + 8)
            return false;

        node = JsonValue.Create(BinaryPrimitives.ReadDoubleLittleEndian(source[1..]));
        bytesRead = 1 + 8;
        return true;
    }

    private static bool TryReadDecimal(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 0;
        if (!TryReadUtf8Prefixed(source[1..], out var decimalText, out var decimalBytesRead))
            return false;

        if (!decimal.TryParse(decimalText, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalValue))
            return false;

        node = JsonValue.Create(decimalValue);
        bytesRead = 1 + decimalBytesRead;
        return true;
    }

    private static bool TryReadNull(out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 1;
        return true;
    }

    private static bool TryReadObject(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 0;
        if (source.Length < 1 + 2)
            return false;

        var count = BinaryPrimitives.ReadUInt16LittleEndian(source[1..]);
        bytesRead = 3;
        var obj = new JsonObject();
        for (var i = 0; i < count; i++)
        {
            if (!TryReadUtf8Prefixed(source[bytesRead..], out var name, out var nameBytes))
                return false;

            bytesRead += nameBytes;
            if (!TryReadNode(source[bytesRead..], out var value, out var valueBytes))
                return false;

            bytesRead += valueBytes;
            obj[name] = value;
        }

        node = obj;
        return true;
    }

    private static bool TryReadString(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
    {
        node = null;
        bytesRead = 0;
        if (!TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
            return false;

        node = JsonValue.Create(Encoding.UTF8.GetString(stringBytes));
        bytesRead = 1 + stringBytesRead;
        return true;
    }

    private static bool TryReadUtf32Prefixed(ReadOnlySpan<byte> source, out ReadOnlySpan<byte> bytes, out int bytesRead)
    {
        bytes = default;
        bytesRead = 0;
        if (source.Length < 4)
            return false;

        var length = BinaryPrimitives.ReadUInt32LittleEndian(source);
        var lengthInt = int.CreateChecked(length);
        bytesRead = 4 + lengthInt;
        if (source.Length < bytesRead)
            return false;

        bytes = source.Slice(4, lengthInt);
        return true;
    }

    private static bool TryReadUtf8Prefixed(ReadOnlySpan<byte> source, out string text, out int bytesRead)
    {
        text = string.Empty;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
        bytesRead = 2 + length;
        if (source.Length < bytesRead)
            return false;

        text = Encoding.UTF8.GetString(source.Slice(2, length));
        return true;
    }
}
