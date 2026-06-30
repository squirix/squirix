using System;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Squirix.Serialization;

namespace Squirix.Internal.Cluster.Transport.Binary;

/// <summary>Binary cache value encoding for gRPC wire payloads.</summary>
internal static class CacheValueWireCodec
{
    private const int MaxUtf16StringLength = ushort.MaxValue;

    internal static byte[] EncodeWireValueToOwned(object? value, ISquirixSerializer serializer)
    {
        if (value is null or bool or string or byte[] or sbyte or byte or short or ushort or int or uint or long or float or double or decimal or JsonElement)
        {
            var length = ComputeValueLength(value);
            return WireBufferEx.EncodeToOwned(length, value, static (state, destination) => _ = WriteValue(state, destination));
        }

        var typeInfo = WireSerializerEx.GetTypeInfo(serializer, value.GetType());
        return BinaryJsonTreeMetadataCodec.EncodeToOwned(value, typeInfo);
    }

    internal static bool TryReadWireValue<T>(ReadOnlySpan<byte> source, ISquirixSerializer serializer, out T? value)
    {
        value = default;
        if (source.IsEmpty)
            return false;

        if (source[0] is ValueKind.Object or ValueKind.Array)
        {
            var typeInfo = WireSerializerEx.GetTypeInfo(serializer, typeof(T));
            return BinaryJsonTreeMetadataCodec.TryRead(source, typeInfo, out value);
        }

        if (!TryReadValue(source, out var decoded))
            return false;

        if (TryCoerceTo(decoded, out value))
            return true;

        var leafTypeInfo = WireSerializerEx.GetTypeInfo(serializer, typeof(T));
        return BinaryJsonTreeMetadataCodec.TryRead(source, leafTypeInfo, out value);
    }

    internal static bool TryReadWireValueUntyped(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (!TryReadValue(source, out var decoded))
            return false;

        value = decoded;
        return true;
    }

    private static int ComputeValueLength(object? value) => value switch
    {
        null => 1,
        bool => 2,
        string s => 1 + 4 + Encoding.UTF8.GetByteCount(s),
        byte[] bytes => 1 + 4 + bytes.Length,
        sbyte or byte or short or ushort or int or uint or long => 1 + 8,
        float or double => 1 + 8,
        decimal m => 1 + 2 + Encoding.UTF8.GetByteCount(m.ToString(CultureInfo.InvariantCulture)),
        JsonElement je => BinaryJsonTreeCodec.ComputeEncodedLength(je),
        _ => throw new InvalidOperationException("Value must be normalized before encoding."),
    };

    private static TTarget Reinterpret<TTarget, TValue>(TValue value)
        where TValue : struct => Unsafe.As<TValue, TTarget>(ref value);

    private static bool TryCoerceTo<T>(object? value, out T? result)
    {
        switch (value)
        {
            case null:
                result = default;
                return true;

            case T ok:
                result = ok;
                return true;

            case JsonElement je when typeof(T) == typeof(JsonElement):
                result = Reinterpret<T, JsonElement>(je);
                return true;

            case long l when typeof(T) == typeof(int):
                result = Reinterpret<T, int>(int.CreateChecked(l));
                return true;

            case long l when typeof(T) == typeof(long):
                result = Reinterpret<T, long>(l);
                return true;

            case double d when typeof(T) == typeof(float):
                result = Reinterpret<T, float>(Convert.ToSingle(d));
                return true;

            case double d when typeof(T) == typeof(double):
                result = Reinterpret<T, double>(d);
                return true;

            default:
                result = default;
                return false;
        }
    }

    private static bool TryReadBoolValue(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (source.Length < 2)
            return false;

        value = source[1] is not 0;
        return true;
    }

    private static bool TryReadBytesValue(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (!TryReadUtf32Prefixed(source[1..], out var rawBytes))
            return false;

#pragma warning disable ZA0302
        var bytes = new byte[rawBytes.Length];
#pragma warning restore ZA0302
        rawBytes.CopyTo(bytes);
        value = bytes;
        return true;
    }

    private static bool TryReadDecimalValue(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (!TryReadUtf8Prefixed(source[1..], out var decimalText))
            return false;

        if (!decimal.TryParse(decimalText, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalValue))
            return false;

        value = decimalValue;
        return true;
    }

    private static bool TryReadDoubleValue(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (source.Length < 1 + 8)
            return false;

        value = BinaryPrimitives.ReadDoubleLittleEndian(source[1..]);
        return true;
    }

    private static bool TryReadInt64Value(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (source.Length < 1 + 8)
            return false;

        value = BinaryPrimitives.ReadInt64LittleEndian(source[1..]);
        return true;
    }

    private static bool TryReadJsonTreeValue(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (!BinaryJsonTreeCodec.TryRead(source, out var element))
            return false;

        value = element;
        return true;
    }

    private static bool TryReadNullValue(out object? value)
    {
        value = null;
        return true;
    }

    private static bool TryReadStringValue(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (!TryReadUtf32Prefixed(source[1..], out var stringBytes))
            return false;

        value = Encoding.UTF8.GetString(stringBytes);
        return true;
    }

    private static bool TryReadUtf32Prefixed(ReadOnlySpan<byte> source, out ReadOnlySpan<byte> bytes)
    {
        bytes = default;
        if (source.Length < 4)
            return false;

        var length = BinaryPrimitives.ReadUInt32LittleEndian(source);
        var lengthInt = int.CreateChecked(length);
        var bytesRead = 4 + lengthInt;
        if (source.Length < bytesRead)
            return false;

        bytes = source.Slice(4, lengthInt);
        return true;
    }

    private static bool TryReadUtf8Prefixed(ReadOnlySpan<byte> source, out string text)
    {
        text = string.Empty;
        if (source.Length < 2)
            return false;

        var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
        var bytesRead = 2 + length;
        if (source.Length < bytesRead)
            return false;

        text = Encoding.UTF8.GetString(source.Slice(2, length));
        return true;
    }

    private static bool TryReadValue(ReadOnlySpan<byte> source, out object? value)
    {
        value = null;
        if (source.IsEmpty)
            return false;

        return source[0] switch
        {
            ValueKind.Null => TryReadNullValue(out value),
            ValueKind.Bool => TryReadBoolValue(source, out value),
            ValueKind.String => TryReadStringValue(source, out value),
            ValueKind.Bytes => TryReadBytesValue(source, out value),
            ValueKind.Int64 => TryReadInt64Value(source, out value),
            ValueKind.Double => TryReadDoubleValue(source, out value),
            ValueKind.Decimal => TryReadDecimalValue(source, out value),
            ValueKind.Object or ValueKind.Array => TryReadJsonTreeValue(source, out value),
            _ => false,
        };
    }

    private static int WriteUtf32Prefixed(ReadOnlySpan<byte> bytes, Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(destination, uint.CreateTruncating(bytes.Length));
        bytes.CopyTo(destination[4..]);
        return 4 + bytes.Length;
    }

    private static int WriteUtf32PrefixedString(string text, Span<byte> destination)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        BinaryPrimitives.WriteUInt32LittleEndian(destination, uint.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, destination[4..]);
        return 4 + byteCount;
    }

    private static int WriteUtf8Prefixed(string text, Span<byte> destination)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        if (byteCount > MaxUtf16StringLength)
            throw new InvalidDataException("Snapshot string exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, destination[2..]);
        return 2 + byteCount;
    }

    private static int WriteValue(object? value, Span<byte> destination)
    {
        var offset = 0;
        switch (value)
        {
            case null:
                destination[offset++] = ValueKind.Null;
                break;

            case bool b:
                destination[offset++] = ValueKind.Bool;
                if (b)
                    destination[offset++] = 1;
                else
                    destination[offset++] = 0;
                break;

            case string s:
                destination[offset++] = ValueKind.String;
                offset += WriteUtf32PrefixedString(s, destination[offset..]);
                break;

            case byte[] bytes:
                destination[offset++] = ValueKind.Bytes;
                offset += WriteUtf32Prefixed(bytes, destination[offset..]);
                break;

            case sbyte or byte or short or ushort or int or uint or long:
                destination[offset++] = ValueKind.Int64;
                BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], Convert.ToInt64(value, CultureInfo.InvariantCulture));
                offset += 8;
                break;

            case float or double:
                destination[offset++] = ValueKind.Double;
                BinaryPrimitives.WriteDoubleLittleEndian(destination[offset..], Convert.ToDouble(value, CultureInfo.InvariantCulture));
                offset += 8;
                break;

            case decimal m:
                destination[offset++] = ValueKind.Decimal;
                offset += WriteUtf8Prefixed(m.ToString(CultureInfo.InvariantCulture), destination[offset..]);
                break;

            case JsonElement je:
                offset += BinaryJsonTreeCodec.Write(je, destination);
                break;

            default:
                throw new InvalidOperationException("Value must be normalized before encoding.");
        }

        return offset;
    }
}
