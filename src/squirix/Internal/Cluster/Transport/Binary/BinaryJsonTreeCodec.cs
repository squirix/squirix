using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;

namespace Squirix.Internal.Cluster.Transport.Binary;

/// <summary>Encodes and decodes <see cref="JsonElement" /> trees without persisting UTF-8 JSON blobs.</summary>
internal static class BinaryJsonTreeCodec
{
    private const int InitialJsonBufferSize = 256;
    private const int MaxUtf16StringLength = ushort.MaxValue;

    private const int NumberEncodedLength = 1 + sizeof(long);

    public static int ComputeEncodedLength(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => 1,
        JsonValueKind.True or JsonValueKind.False => 2,
        JsonValueKind.String => 1 + 4 + Encoding.UTF8.GetByteCount(element.GetString() ?? string.Empty),
        JsonValueKind.Number => ComputeNumberEncodedLength(element),
        JsonValueKind.Object => ComputeObjectLength(element),
        JsonValueKind.Array => ComputeArrayLength(element),
        _ => throw new InvalidDataException("Unsupported JSON value kind for binary tree encoding."),
    };

    public static bool TryRead(ReadOnlySpan<byte> source, out JsonElement element)
    {
        element = default;
        var buffer = new ArrayBufferWriter<byte>(InitialJsonBufferSize);
        var writer = new Utf8JsonWriter(buffer);
        if (!TryWriteJsonValue(source, ref writer, out _))
            return false;

        FlushJsonWriter(writer);
        element = JsonSerializer.Deserialize(buffer.WrittenSpan, BinaryJsonTreeJsonContext.Default.JsonElement);
        return true;
    }

    public static int Write(JsonElement element, Span<byte> destination)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Null or JsonValueKind.Undefined => WriteNull(destination),
            JsonValueKind.True or JsonValueKind.False => WriteBool(element.GetBoolean(), destination),
            JsonValueKind.String => WriteString(element.GetString() ?? string.Empty, destination),
            JsonValueKind.Number => WriteNumber(element, destination),
            JsonValueKind.Object => WriteObject(element, destination),
            JsonValueKind.Array => WriteArray(element, destination),
            _ => throw new InvalidDataException("Unsupported JSON value kind for binary tree encoding."),
        };
    }

    private static int ComputeArrayLength(ref Utf8JsonReader reader)
    {
        var length = 1 + 4;
        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.EndArray)
                break;

            length += ComputeEncodedLength(ref reader);
        }

        return length;
    }

    private static int ComputeArrayLength(JsonElement element)
    {
        var length = 1 + 4;
        foreach (var item in element.EnumerateArray())
            length += ComputeEncodedLength(item);

        return length;
    }

    private static int ComputeEncodedLength(ref Utf8JsonReader reader)
    {
        return reader.TokenType switch
        {
            JsonTokenType.Null => 1,
            JsonTokenType.True or JsonTokenType.False => 2,
            JsonTokenType.String => 1 + 4 + reader.ValueSpan.Length,
            JsonTokenType.Number => NumberEncodedLength,
            JsonTokenType.StartObject => ComputeObjectLength(ref reader),
            JsonTokenType.StartArray => ComputeArrayLength(ref reader),
            _ => throw new InvalidDataException("Unsupported JSON token for binary tree encoding."),
        };
    }

    private static int ComputeNumberEncodedLength(JsonElement element)
    {
        if (TryGetInteger(element, out _) || !element.TryGetDecimal(out var decimalValue))
            return NumberEncodedLength;

        var text = decimalValue.ToString(CultureInfo.InvariantCulture);
        return 1 + 2 + Encoding.UTF8.GetByteCount(text);
    }

    private static int ComputeObjectLength(ref Utf8JsonReader reader)
    {
        var length = 1 + 2;
        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.EndObject)
                break;

            if (reader.TokenType is not JsonTokenType.PropertyName)
                throw new InvalidDataException("Expected JSON property name for binary tree encoding.");

            if (reader.ValueSpan.Length > MaxUtf16StringLength)
                throw new InvalidDataException("Object property name exceeds maximum encoded length.");

            length += 2 + reader.ValueSpan.Length;
            if (!reader.Read())
                throw new InvalidDataException("Truncated JSON object for binary tree encoding.");

            length += ComputeEncodedLength(ref reader);
        }

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

    private static void FlushJsonWriter(Utf8JsonWriter writer)
    {
#pragma warning disable MA0045
        writer.Flush();
#pragma warning restore MA0045
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

    private static bool TryReadUtf8PrefixedSpan(ReadOnlySpan<byte> source, out ReadOnlySpan<byte> text, out int bytesRead)
    {
        text = default;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
        bytesRead = 2 + length;
        if (source.Length < bytesRead)
            return false;

        text = source.Slice(2, length);
        return true;
    }

    private static bool TryWriteJsonArray(ReadOnlySpan<byte> source, ref Utf8JsonWriter writer, out int bytesRead)
    {
        bytesRead = 0;
        if (source.Length < 1 + 4)
            return false;

        var count = BinaryPrimitives.ReadUInt32LittleEndian(source[1..]);
        bytesRead = 5;
        writer.WriteStartArray();
        for (var i = 0; i < count; i++)
        {
            if (!TryWriteJsonValue(source[bytesRead..], ref writer, out var itemBytes))
            {
                writer.WriteEndArray();
                return false;
            }

            bytesRead += itemBytes;
        }

        writer.WriteEndArray();
        return true;
    }

    private static bool TryWriteJsonDecimal(ReadOnlySpan<byte> source, ref Utf8JsonWriter writer, out int bytesRead)
    {
        bytesRead = 0;
        if (!TryReadUtf8Prefixed(source[1..], out var decimalText, out var decimalBytesRead))
            return false;

        if (!decimal.TryParse(decimalText, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalValue))
            return false;

        writer.WriteNumberValue(decimalValue);
        bytesRead = 1 + decimalBytesRead;
        return true;
    }

    private static bool TryWriteJsonObject(ReadOnlySpan<byte> source, ref Utf8JsonWriter writer, out int bytesRead)
    {
        bytesRead = 0;
        if (source.Length < 1 + 2)
            return false;

        var count = BinaryPrimitives.ReadUInt16LittleEndian(source[1..]);
        bytesRead = 3;
        writer.WriteStartObject();
        for (var i = 0; i < count; i++)
        {
            if (!TryReadUtf8PrefixedSpan(source[bytesRead..], out var name, out var nameBytes))
            {
                writer.WriteEndObject();
                return false;
            }

            bytesRead += nameBytes;
            writer.WritePropertyName(name);
            if (!TryWriteJsonValue(source[bytesRead..], ref writer, out var valueBytes))
            {
                writer.WriteEndObject();
                return false;
            }

            bytesRead += valueBytes;
        }

        writer.WriteEndObject();
        return true;
    }

    private static bool TryWriteJsonValue(ReadOnlySpan<byte> source, ref Utf8JsonWriter writer, out int bytesRead)
    {
        bytesRead = 0;
        if (source.IsEmpty)
            return false;

        switch (source[0])
        {
            case ValueKind.Null:
                writer.WriteNullValue();
                bytesRead = 1;
                return true;

            case ValueKind.Bool:
                if (source.Length < 2)
                    return false;

                writer.WriteBooleanValue(source[1] is not 0);
                bytesRead = 2;
                return true;

            case ValueKind.String:
                if (!TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
                    return false;

                writer.WriteStringValue(stringBytes);
                bytesRead = 1 + stringBytesRead;
                return true;

            case ValueKind.Int64:
                if (source.Length < 1 + 8)
                    return false;

                writer.WriteNumberValue(BinaryPrimitives.ReadInt64LittleEndian(source[1..]));
                bytesRead = 1 + 8;
                return true;

            case ValueKind.Double:
                if (source.Length < 1 + 8)
                    return false;

                writer.WriteNumberValue(BinaryPrimitives.ReadDoubleLittleEndian(source[1..]));
                bytesRead = 1 + 8;
                return true;

            case ValueKind.Decimal:
                return TryWriteJsonDecimal(source, ref writer, out bytesRead);

            case ValueKind.Object:
                return TryWriteJsonObject(source, ref writer, out bytesRead);

            case ValueKind.Array:
                return TryWriteJsonArray(source, ref writer, out bytesRead);

            default:
                return false;
        }
    }

    private static int WriteArray(ref Utf8JsonReader reader, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = ValueKind.Array;
        var countOffset = offset;
        offset += 4;
        uint itemCount = 0;
        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.EndArray)
                break;

            offset += WriteFromReader(ref reader, destination[offset..]);
            itemCount++;
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[countOffset..], itemCount);
        return offset;
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

    private static int WriteDecimalNumber(decimal decimalValue, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = ValueKind.Decimal;
        offset += WriteUtf8Prefixed(decimalValue.ToString(CultureInfo.InvariantCulture), destination[offset..]);
        return offset;
    }

    private static int WriteFromReader(ref Utf8JsonReader reader, Span<byte> destination)
    {
        return reader.TokenType switch
        {
            JsonTokenType.Null => WriteNull(destination),
            JsonTokenType.True => WriteBool(true, destination),
            JsonTokenType.False => WriteBool(false, destination),
            JsonTokenType.String => WriteUtf8String(reader.ValueSpan, destination),
            JsonTokenType.Number => WriteNumber(ref reader, destination),
            JsonTokenType.StartObject => WriteObject(ref reader, destination),
            JsonTokenType.StartArray => WriteArray(ref reader, destination),
            _ => throw new InvalidDataException("Unsupported JSON token for binary tree encoding."),
        };
    }

    private static int WriteNull(Span<byte> destination)
    {
        destination[0] = ValueKind.Null;
        return 1;
    }

    private static int WriteNumber(ref Utf8JsonReader reader, Span<byte> destination)
    {
        if (reader.TryGetInt64(out var integer))
        {
            destination[0] = ValueKind.Int64;
            BinaryPrimitives.WriteInt64LittleEndian(destination[1..], integer);
        }
        else
        {
            destination[0] = ValueKind.Double;
            BinaryPrimitives.WriteDoubleLittleEndian(destination[1..], reader.GetDouble());
        }

        return NumberEncodedLength;
    }

    private static int WriteNumber(JsonElement element, Span<byte> destination)
    {
        if (TryGetInteger(element, out var integer))
        {
            destination[0] = ValueKind.Int64;
            BinaryPrimitives.WriteInt64LittleEndian(destination[1..], integer);
            return NumberEncodedLength;
        }

        if (element.TryGetDecimal(out var decimalValue))
            return WriteDecimalNumber(decimalValue, destination);

        destination[0] = ValueKind.Double;
        BinaryPrimitives.WriteDoubleLittleEndian(destination[1..], element.GetDouble());
        return NumberEncodedLength;
    }

    private static int WriteObject(ref Utf8JsonReader reader, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = ValueKind.Object;
        var countOffset = offset;
        offset += 2;
        ushort propertyCount = 0;
        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.EndObject)
                break;

            if (reader.TokenType is not JsonTokenType.PropertyName)
                throw new InvalidDataException("Expected JSON property name for binary tree encoding.");

            offset += WriteUtf8PrefixedFromSpan(reader.ValueSpan, destination[offset..]);
            if (!reader.Read())
                throw new InvalidDataException("Truncated JSON object for binary tree encoding.");

            offset += WriteFromReader(ref reader, destination[offset..]);
            propertyCount++;
        }

        BinaryPrimitives.WriteUInt16LittleEndian(destination[countOffset..], propertyCount);
        return offset;
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

    private static int WriteUtf8PrefixedFromSpan(ReadOnlySpan<byte> utf8, Span<byte> destination)
    {
        if (utf8.Length > MaxUtf16StringLength)
            throw new InvalidDataException("Object property name exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(utf8.Length));
        utf8.CopyTo(destination[2..]);
        return 2 + utf8.Length;
    }

    private static int WriteUtf8String(ReadOnlySpan<byte> utf8, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = ValueKind.String;
        BinaryPrimitives.WriteUInt32LittleEndian(destination[offset..], uint.CreateTruncating(utf8.Length));
        offset += 4;
        utf8.CopyTo(destination[offset..]);
        return offset + utf8.Length;
    }
}
