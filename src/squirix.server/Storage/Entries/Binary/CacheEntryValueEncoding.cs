using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Squirix.Server.Serialization;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Value encoding helpers for <see cref="CacheEntryCodec" />.</summary>
internal static class CacheEntryValueEncoding
{
    internal static int ComputeLength(object? value) => value switch
    {
        null => 1,
        bool => 2,
        string s => 1 + 4 + Encoding.UTF8.GetByteCount(s),
        byte[] bytes => 1 + 4 + bytes.Length,
        sbyte or byte or short or ushort or int or uint or long => 1 + 8,
        float or double => 1 + 8,
        decimal m => 1 + 2 + Encoding.UTF8.GetByteCount(m.ToString(CultureInfo.InvariantCulture)),
        JsonElement je => JsonTreeCodec.ComputeEncodedLength(je),
        _ => JsonTreeCodec.ComputeEncodedLength(ServerSerializationProvider.Instance.SerializeToElement(value)),
    };

    internal static bool TryCoerceTo<T>(object? value, out T? result)
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

    internal static bool TryRead(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.IsEmpty)
            return false;

        return source[0] switch
        {
            ValueKind.Null => TryReadNullValue(out value, out bytesRead),
            ValueKind.Bool => TryReadBoolValue(source, out value, out bytesRead),
            ValueKind.String => TryReadStringValue(source, out value, out bytesRead),
            ValueKind.Bytes => TryReadBytesValue(source, out value, out bytesRead),
            ValueKind.Int64 => TryReadInt64Value(source, out value, out bytesRead),
            ValueKind.Double => TryReadDoubleValue(source, out value, out bytesRead),
            ValueKind.Decimal => TryReadDecimalValue(source, out value, out bytesRead),
            ValueKind.Object or ValueKind.Array => TryReadJsonTreeValue(source, out value, out bytesRead),
            _ => false,
        };
    }

    internal static int Write(object? value, Span<byte> destination) => value switch
    {
        null => WriteNull(destination),
        bool b => WriteBool(b, destination),
        string s => WriteString(s, destination),
        byte[] bytes => WriteBytes(bytes, destination),
        sbyte or byte or short or ushort or int or uint or long => WriteInt64(value, destination),
        float or double => WriteDouble(value, destination),
        decimal m => WriteDecimal(m, destination),
        JsonElement je => JsonTreeCodec.Write(je, destination),
        _ => WriteSerializedObject(value, destination),
    };

    private static int WriteBool(bool value, Span<byte> destination)
    {
        destination[0] = ValueKind.Bool;
        if (value)
            destination[1] = 1;
        else
            destination[1] = 0;

        return 2;
    }

    private static int WriteBytes(byte[] bytes, Span<byte> destination)
    {
        destination[0] = ValueKind.Bytes;
        return 1 + CacheEntryTagEncoding.WriteUtf32Prefixed(bytes, destination[1..]);
    }

    private static int WriteDecimal(decimal value, Span<byte> destination)
    {
        destination[0] = ValueKind.Decimal;
        return 1 + CacheEntryTagEncoding.WriteUtf8Prefixed(value.ToString(CultureInfo.InvariantCulture), destination[1..]);
    }

    private static int WriteDouble(object value, Span<byte> destination)
    {
        destination[0] = ValueKind.Double;
        BinaryPrimitives.WriteDoubleLittleEndian(destination[1..], Convert.ToDouble(value, CultureInfo.InvariantCulture));
        return 1 + 8;
    }

    private static int WriteInt64(object value, Span<byte> destination)
    {
        destination[0] = ValueKind.Int64;
        BinaryPrimitives.WriteInt64LittleEndian(destination[1..], Convert.ToInt64(value, CultureInfo.InvariantCulture));
        return 1 + 8;
    }

    private static int WriteNull(Span<byte> destination)
    {
        destination[0] = ValueKind.Null;
        return 1;
    }

    private static int WriteSerializedObject(object value, Span<byte> destination) =>
        JsonTreeCodec.Write(ServerSerializationProvider.Instance.SerializeToElement(value), destination);

    private static int WriteString(string value, Span<byte> destination)
    {
        destination[0] = ValueKind.String;
        return 1 + CacheEntryTagEncoding.WriteUtf32PrefixedString(value, destination[1..]);
    }

    private static TTarget Reinterpret<TTarget, TValue>(TValue value)
        where TValue : struct => Unsafe.As<TValue, TTarget>(ref value);

    private static bool TryReadBoolValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        value = source[1] is not 0;
        bytesRead = 2;
        return true;
    }

    private static bool TryReadBytesValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!CacheEntryTagEncoding.TryReadUtf32Prefixed(source[1..], out var rawBytes, out var rawBytesRead))
            return false;

        // ZA0302: this array IS the decoded user value; its lifetime is owned by the cache
        // and returned to callers as byte[], so it cannot be rented from ArrayPool.
#pragma warning disable ZA0302
        var bytes = new byte[rawBytes.Length];
#pragma warning restore ZA0302
        rawBytes.CopyTo(bytes);
        value = bytes;
        bytesRead = 1 + rawBytesRead;
        return true;
    }

    private static bool TryReadDecimalValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!CacheEntryTagEncoding.TryReadUtf8Prefixed(source[1..], out var decimalText, out var decimalBytesRead))
            return false;

        if (!decimal.TryParse(decimalText, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalValue))
            return false;

        value = decimalValue;
        bytesRead = 1 + decimalBytesRead;
        return true;
    }

    private static bool TryReadDoubleValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.Length < 1 + 8)
            return false;

        value = BinaryPrimitives.ReadDoubleLittleEndian(source[1..]);
        bytesRead = 1 + 8;
        return true;
    }

    private static bool TryReadInt64Value(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.Length < 1 + 8)
            return false;

        value = BinaryPrimitives.ReadInt64LittleEndian(source[1..]);
        bytesRead = 1 + 8;
        return true;
    }

    private static bool TryReadJsonTreeValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!JsonTreeCodec.TryRead(source, out var element, out bytesRead))
            return false;

        value = element;
        return true;
    }

    private static bool TryReadNullValue(out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 1;
        return true;
    }

    private static bool TryReadStringValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!CacheEntryTagEncoding.TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
            return false;

        value = Encoding.UTF8.GetString(stringBytes);
        bytesRead = 1 + stringBytesRead;
        return true;
    }
}
