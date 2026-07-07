using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Read helpers for <see cref="JsonTreeCodec" />.</summary>
internal static class JsonTreeReadCodec
{
    internal static bool TryReadNode(ReadOnlySpan<byte> source, out JsonNode? node, out int bytesRead)
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
