using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Text.Json;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Write helpers for <see cref="JsonTreeCodec" />.</summary>
internal static class JsonTreeWriteCodec
{
    internal static int Write(JsonElement element, Span<byte> destination)
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
        if (byteCount > JsonTreeCodec.MaxUtf16StringLength)
            throw new InvalidDataException("Object property name exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, destination[2..]);
        return 2 + byteCount;
    }
}
