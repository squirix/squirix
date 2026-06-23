using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Squirix.Server.Serialization;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Binary cache-entry encoding for journal and snapshot payloads.</summary>
internal static class CacheEntryCodec
{
    private const int MaxUtf16StringLength = ushort.MaxValue;

    /// <summary>
    /// Returns a value already in a directly-encodable form: primitives, strings, byte arrays and
    /// <see cref="JsonElement" /> pass through unchanged, while any other object is serialized to a
    /// <see cref="JsonElement" /> exactly once. Callers normalize before the
    /// <see cref="ComputeEncodedLength" /> / <see cref="Write" /> pair so an arbitrary object is not
    /// re-serialized on every length and write pass.
    /// </summary>
    /// <param name="value">The raw cache value.</param>
    /// <returns>The same value when directly encodable; otherwise its <see cref="JsonElement" /> form.</returns>
    public static object? NormalizeValue(object? value) => value switch
    {
        null or bool or string or byte[] or sbyte or byte or short or ushort or int or uint or long or float or double or decimal or JsonElement => value,
        _ => SerializationProvider.Instance.SerializeToElement(value),
    };

    public static int ComputeEncodedLength(CacheEntry<object?> entry)
    {
        var length = 1 + 1 + 8;
        length += ComputeTagsLength(entry.Tags);
        length += ComputeValueLength(entry.Value);
        if (entry.ExpiresUtc is not null)
            length += 8;

        if (entry.Expiration is not null)
            length += 8;

        return length;
    }

    public static void Write(CacheEntry<object?> entry, Span<byte> destination)
    {
        if (destination.Length < ComputeEncodedLength(entry))
            throw new ArgumentException("Destination span is too small for the encoded cache entry.", nameof(destination));

        var offset = 0;
        if (entry.ExpiresUtc is { } expiresUtc)
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], new DateTimeOffset(expiresUtc.ToUniversalTime()).ToUnixTimeMilliseconds());
            offset += 8;
        }
        else
        {
            destination[offset++] = 0;
        }

        if (entry.Expiration is { } expiration)
        {
            destination[offset++] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], expiration.Ticks);
            offset += 8;
        }
        else
        {
            destination[offset++] = 0;
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], entry.Version);
        offset += 8;
        offset += WriteTags(entry.Tags, destination[offset..]);
        _ = WriteValue(entry.Value, destination[offset..]);
    }

    public static bool TryMapEntry<T>(CacheEntry<object?> entry, out CacheEntry<T>? mapped)
    {
        if (!TryCoerceTo<T>(entry.Value, out var typedValue))
        {
            mapped = null;
            return false;
        }

        mapped = new CacheEntry<T>
        {
            Value = typedValue,
            ExpiresUtc = entry.ExpiresUtc,
            Expiration = entry.Expiration,
            Version = entry.Version,
            Tags = entry.Tags,
        };
        return true;
    }

    public static bool TryRead<T>(ReadOnlySpan<byte> source, out CacheEntry<T>? entry, out int bytesRead)
    {
        entry = null;
        bytesRead = 0;
        if (source.Length < 1 + 1 + 8)
            return false;

        var offset = 0;
        DateTime? expiresUtc = null;
        if (source[offset++] is not 0)
        {
            if (source.Length < offset + 8)
                return false;

            expiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(source[offset..])).UtcDateTime;
            offset += 8;
        }

        TimeSpan? expiration = null;
        if (source[offset++] is not 0)
        {
            if (source.Length < offset + 8)
                return false;

            expiration = TimeSpan.FromTicks(BinaryPrimitives.ReadInt64LittleEndian(source[offset..]));
            offset += 8;
        }

        if (source.Length < offset + 8)
            return false;

        var version = BinaryPrimitives.ReadInt64LittleEndian(source[offset..]);
        offset += 8;
        if (!TryReadTags(source[offset..], out var tags, out var tagsBytes))
            return false;

        offset += tagsBytes;
        if (!TryReadValue(source[offset..], out var value, out var valueBytes))
            return false;

        offset += valueBytes;
        if (!TryCoerceTo<T>(value, out var typedValue))
            return false;

        entry = new CacheEntry<T>
        {
            Value = typedValue,
            ExpiresUtc = expiresUtc,
            Expiration = expiration,
            Version = version,
            Tags = tags,
        };
        bytesRead = offset;
        return true;
    }

    private static int ComputeTagsLength(FrozenDictionary<string, string>? tags)
    {
        if (tags is null || tags.Count is 0)
            return 2;

        var length = 2;
        foreach (var (key, value) in tags)
        {
            var keyBytes = Encoding.UTF8.GetByteCount(key);
            var valueBytes = Encoding.UTF8.GetByteCount(value);
            if (keyBytes > MaxUtf16StringLength || valueBytes > MaxUtf16StringLength)
                throw new InvalidDataException("Snapshot tag key or value exceeds maximum encoded length.");

            length += 2 + 2 + keyBytes + valueBytes;
        }

        return length;
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
        _ => BinaryJsonTreeCodec.ComputeEncodedLength(SerializationProvider.Instance.SerializeToElement(value)),
    };

    private static int WriteTags(FrozenDictionary<string, string>? tags, Span<byte> destination)
    {
        if (tags is null || tags.Count is 0)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(destination, 0);
            return 2;
        }

        BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(tags.Count));
        var offset = 2;
        foreach (var (key, value) in tags)
        {
            offset += WriteUtf8Prefixed(key, destination[offset..]);
            offset += WriteUtf8Prefixed(value, destination[offset..]);
        }

        return offset;
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
                offset += BinaryJsonTreeCodec.Write(SerializationProvider.Instance.SerializeToElement(value), destination);
                break;
        }

        return offset;
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

    private static bool TryReadTags(ReadOnlySpan<byte> source, out FrozenDictionary<string, string>? tags, out int bytesRead)
    {
        tags = null;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        var count = BinaryPrimitives.ReadUInt16LittleEndian(source);
        bytesRead = 2;
        if (count is 0)
            return true;

        var dict = new Dictionary<string, string>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            if (!TryReadUtf8Prefixed(source[bytesRead..], out var key, out var keyBytes))
                return false;

            bytesRead += keyBytes;
            if (!TryReadUtf8Prefixed(source[bytesRead..], out var value, out var valueBytes))
                return false;

            bytesRead += valueBytes;
            dict[key] = value;
        }

        tags = dict.ToFrozenDictionary(StringComparer.Ordinal);
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

    private static bool TryReadValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
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

    private static bool TryReadNullValue(out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 1;
        return true;
    }

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

    private static bool TryReadStringValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
            return false;

        value = Encoding.UTF8.GetString(stringBytes);
        bytesRead = 1 + stringBytesRead;
        return true;
    }

    private static bool TryReadBytesValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!TryReadUtf32Prefixed(source[1..], out var rawBytes, out var rawBytesRead))
            return false;

        var bytes = new byte[rawBytes.Length];
        rawBytes.CopyTo(bytes);
        value = bytes;
        bytesRead = 1 + rawBytesRead;
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

    private static bool TryReadDecimalValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!TryReadUtf8Prefixed(source[1..], out var decimalText, out var decimalBytesRead))
            return false;

        if (!decimal.TryParse(decimalText, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalValue))
            return false;

        value = decimalValue;
        bytesRead = 1 + decimalBytesRead;
        return true;
    }

    private static bool TryReadJsonTreeValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!BinaryJsonTreeCodec.TryRead(source, out var element, out bytesRead))
            return false;

        value = element;
        return true;
    }

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
}
