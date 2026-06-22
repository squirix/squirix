using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Squirix.Server.Serialization;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Binary cache-entry encoding mirroring discriminated JSON semantics.</summary>
[SuppressMessage("Design", "MA0181:Do not use cast", Justification = "Binary framing writes tagged union discriminators.")]
[SuppressMessage("Design", "MA0051:Method is too long", Justification = "Value decoder mirrors a single tagged switch over all value kinds.")]
internal static class CacheEntryCodec
{
    private const int MaxUtf16StringLength = ushort.MaxValue;

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

    private static int ComputeValueLength(object? value) => 1 + value switch
    {
        null => 0,
        bool => 1,
        string s => 4 + Encoding.UTF8.GetByteCount(s),
        byte[] bytes => 4 + bytes.Length,
        sbyte or byte or short or ushort or int or uint or long => 8,
        float or double => 8,
        decimal m => 2 + Encoding.UTF8.GetByteCount(m.ToString(CultureInfo.InvariantCulture)),
        StoredJsonPayload sjp => 4 + sjp.Utf8Memory.Length,
        JsonElement je => 4 + Encoding.UTF8.GetByteCount(je.GetRawText()),
        _ => 4 + SerializationProvider.Instance.SerializeToUtf8Bytes(value).Length,
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
                destination[offset++] = (byte)ValueKind.Null;
                break;

            case bool b:
                destination[offset++] = (byte)ValueKind.Bool;
                destination[offset++] = (byte)(b ? 1 : 0);
                break;

            case string s:
                destination[offset++] = (byte)ValueKind.String;
                offset += WriteUtf32Prefixed(Encoding.UTF8.GetBytes(s), destination[offset..]);
                break;

            case byte[] bytes:
                destination[offset++] = (byte)ValueKind.Bytes;
                offset += WriteUtf32Prefixed(bytes, destination[offset..]);
                break;

            case sbyte or byte or short or ushort or int or uint or long:
                destination[offset++] = (byte)ValueKind.Int64;
                BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], Convert.ToInt64(value, CultureInfo.InvariantCulture));
                offset += 8;
                break;

            case float or double:
                destination[offset++] = (byte)ValueKind.Double;
                BinaryPrimitives.WriteDoubleLittleEndian(destination[offset..], Convert.ToDouble(value, CultureInfo.InvariantCulture));
                offset += 8;
                break;

            case decimal m:
                destination[offset++] = (byte)ValueKind.Decimal;
                offset += WriteUtf8Prefixed(m.ToString(CultureInfo.InvariantCulture), destination[offset..]);
                break;

            case StoredJsonPayload sjp:
                destination[offset++] = (byte)ValueKind.JsonBlob;
                offset += WriteUtf32Prefixed(sjp.Utf8Memory.Span, destination[offset..]);
                break;

            case JsonElement je:
                destination[offset++] = (byte)ValueKind.JsonBlob;
                offset += WriteUtf32Prefixed(Encoding.UTF8.GetBytes(je.GetRawText()), destination[offset..]);
                break;

            default:
                destination[offset++] = (byte)ValueKind.JsonBlob;
                offset += WriteUtf32Prefixed(SerializationProvider.Instance.SerializeToUtf8Bytes(value), destination[offset..]);
                break;
        }

        return offset;
    }

    private static int WriteUtf32Prefixed(ReadOnlySpan<byte> bytes, Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(destination, (uint)bytes.Length);
        bytes.CopyTo(destination[4..]);
        return 4 + bytes.Length;
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
        bytesRead = 4 + (int)length;
        if (source.Length < bytesRead)
            return false;

        bytes = source.Slice(4, (int)length);
        return true;
    }

    private static bool TryReadValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.IsEmpty)
            return false;

        var kind = (ValueKind)source[0];
        bytesRead = 1;
        switch (kind)
        {
            case ValueKind.Null:
                return true;

            case ValueKind.Bool:
                if (source.Length < 2)
                    return false;

                value = source[1] is not 0;
                bytesRead = 2;
                return true;

            case ValueKind.String:
                if (!TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
                    return false;

                value = Encoding.UTF8.GetString(stringBytes);
                bytesRead = 1 + stringBytesRead;
                return true;

            case ValueKind.Bytes:
                if (!TryReadUtf32Prefixed(source[1..], out var rawBytes, out var rawBytesRead))
                    return false;

                value = rawBytes.ToArray();
                bytesRead = 1 + rawBytesRead;
                return true;

            case ValueKind.Int64:
                if (source.Length < 1 + 8)
                    return false;

                value = BinaryPrimitives.ReadInt64LittleEndian(source[1..]);
                bytesRead = 1 + 8;
                return true;

            case ValueKind.Double:
                if (source.Length < 1 + 8)
                    return false;

                value = BinaryPrimitives.ReadDoubleLittleEndian(source[1..]);
                bytesRead = 1 + 8;
                return true;

            case ValueKind.Decimal:
                if (!TryReadUtf8Prefixed(source[1..], out var decimalText, out var decimalBytesRead))
                    return false;

                if (!decimal.TryParse(decimalText, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalValue))
                    return false;

                value = decimalValue;
                bytesRead = 1 + decimalBytesRead;
                return true;

            case ValueKind.JsonBlob:
                if (!TryReadUtf32Prefixed(source[1..], out var jsonBytes, out var jsonBytesRead))
                    return false;

                value = new StoredJsonPayload(jsonBytes);
                bytesRead = 1 + jsonBytesRead;
                return true;

            default:
                return false;
        }
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

            case StoredJsonPayload sjp when typeof(T) == typeof(JsonElement):
            {
                using var doc = JsonDocument.Parse(sjp.Utf8Memory);
                var element = doc.RootElement.Clone();
                result = Reinterpret<T, JsonElement>(element);
                return true;
            }

            case JsonElement je when typeof(T) == typeof(JsonElement):
                result = Reinterpret<T, JsonElement>(je);
                return true;

            case long l when typeof(T) == typeof(int):
                result = (T)(object)checked((int)l);
                return true;

            case long l when typeof(T) == typeof(long):
                result = (T)(object)l;
                return true;

            case double d when typeof(T) == typeof(float):
                result = (T)(object)(float)d;
                return true;

            case double d when typeof(T) == typeof(double):
                result = (T)(object)d;
                return true;

            default:
                result = default;
                return false;
        }
    }
}
