using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Single-pass cache-entry encoding helpers.</summary>
internal static partial class CacheEntryCodec
{
    private const int InitialEncodeCapacity = 512;

    internal static byte[] EncodeToOwned(CacheEntry<object?> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var buffer = new ArrayBufferWriter<byte>(InitialEncodeCapacity);
        AppendEntry(entry, buffer);
#pragma warning disable ZA0302
        var owned = new byte[buffer.WrittenCount];
#pragma warning restore ZA0302
        buffer.WrittenSpan.CopyTo(owned);
        return owned;
    }

    private static void AppendEntry(CacheEntry<object?> entry, ArrayBufferWriter<byte> buffer)
    {
        if (entry.ExpiresUtc is { } expiresUtc)
        {
            var span = buffer.GetSpan(1 + 8);
            span[0] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(span[1..], new DateTimeOffset(expiresUtc.ToUniversalTime()).ToUnixTimeMilliseconds());
            buffer.Advance(1 + 8);
        }
        else
        {
            var span = buffer.GetSpan(1);
            span[0] = 0;
            buffer.Advance(1);
        }

        if (entry.Expiration is { } expiration)
        {
            var span = buffer.GetSpan(1 + 8);
            span[0] = 1;
            BinaryPrimitives.WriteInt64LittleEndian(span[1..], expiration.Ticks);
            buffer.Advance(1 + 8);
        }
        else
        {
            var span = buffer.GetSpan(1);
            span[0] = 0;
            buffer.Advance(1);
        }

        var versionSpan = buffer.GetSpan(8);
        BinaryPrimitives.WriteInt64LittleEndian(versionSpan, entry.Version);
        buffer.Advance(8);
        AppendTags(entry.Tags, buffer);
        if (entry.WireValuePayload is { } wireValuePayload)
        {
            var span = buffer.GetSpan(wireValuePayload.Length);
            wireValuePayload.AsSpan().CopyTo(span);
            buffer.Advance(wireValuePayload.Length);
            return;
        }

        AppendValue(entry.Value, buffer);
    }

    private static void AppendTags(FrozenDictionary<string, string>? tags, ArrayBufferWriter<byte> buffer)
    {
        if (tags is null || tags.Count is 0)
        {
            var span = buffer.GetSpan(2);
            BinaryPrimitives.WriteUInt16LittleEndian(span, 0);
            buffer.Advance(2);
            return;
        }

        var header = buffer.GetSpan(2);
        BinaryPrimitives.WriteUInt16LittleEndian(header, ushort.CreateTruncating(tags.Count));
        buffer.Advance(2);
        foreach (var (key, value) in tags)
        {
            AppendUtf8Prefixed(key, buffer);
            AppendUtf8Prefixed(value, buffer);
        }
    }

    private static void AppendUtf8Prefixed(string text, ArrayBufferWriter<byte> buffer)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        if (byteCount > MaxUtf16StringLength)
            throw new InvalidDataException("Snapshot string exceeds maximum encoded length.");

        var span = buffer.GetSpan(2 + byteCount);
        BinaryPrimitives.WriteUInt16LittleEndian(span, ushort.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, span[2..]);
        buffer.Advance(2 + byteCount);
    }

    private static void AppendUtf32Prefixed(ReadOnlySpan<byte> bytes, ArrayBufferWriter<byte> buffer)
    {
        var span = buffer.GetSpan(4 + bytes.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(span, uint.CreateTruncating(bytes.Length));
        bytes.CopyTo(span[4..]);
        buffer.Advance(4 + bytes.Length);
    }

    private static void AppendUtf32PrefixedString(string text, ArrayBufferWriter<byte> buffer)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        var span = buffer.GetSpan(4 + byteCount);
        BinaryPrimitives.WriteUInt32LittleEndian(span, uint.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, span[4..]);
        buffer.Advance(4 + byteCount);
    }

    private static void AppendValue(object? value, ArrayBufferWriter<byte> buffer)
    {
        switch (value)
        {
            case null:
                AppendNullValue(buffer);
                break;

            case bool b:
                AppendBoolValue(b, buffer);
                break;

            case string s:
                AppendStringValue(s, buffer);
                break;

            case byte[] bytes:
                AppendBytesValue(bytes, buffer);
                break;

            case sbyte or byte or short or ushort or int or uint or long:
                AppendInt64Value(value, buffer);
                break;

            case float or double:
                AppendDoubleValue(value, buffer);
                break;

            case decimal m:
                AppendDecimalValue(m, buffer);
                break;

            case JsonElement je:
                AppendJsonElementValue(je, buffer);
                break;

            default:
                BinaryJsonTreeMetadataCodec.Append(value, WireSerializerEx.GetTypeInfo(value.GetType()), buffer);
                break;
        }
    }

    private static void AppendNullValue(ArrayBufferWriter<byte> buffer)
    {
        var span = buffer.GetSpan(1);
        span[0] = ValueKind.Null;
        buffer.Advance(1);
    }

    private static void AppendBoolValue(bool value, ArrayBufferWriter<byte> buffer)
    {
        var span = buffer.GetSpan(2);
        span[0] = ValueKind.Bool;
        if (value)
            span[1] = 1;
        else
            span[1] = 0;
        buffer.Advance(2);
    }

    private static void AppendStringValue(string value, ArrayBufferWriter<byte> buffer)
    {
        var kindSpan = buffer.GetSpan(1);
        kindSpan[0] = ValueKind.String;
        buffer.Advance(1);
        AppendUtf32PrefixedString(value, buffer);
    }

    private static void AppendBytesValue(byte[] value, ArrayBufferWriter<byte> buffer)
    {
        var kindSpan = buffer.GetSpan(1);
        kindSpan[0] = ValueKind.Bytes;
        buffer.Advance(1);
        AppendUtf32Prefixed(value, buffer);
    }

    private static void AppendInt64Value(object value, ArrayBufferWriter<byte> buffer)
    {
        var span = buffer.GetSpan(1 + 8);
        span[0] = ValueKind.Int64;
        BinaryPrimitives.WriteInt64LittleEndian(span[1..], Convert.ToInt64(value, CultureInfo.InvariantCulture));
        buffer.Advance(1 + 8);
    }

    private static void AppendDoubleValue(object value, ArrayBufferWriter<byte> buffer)
    {
        var span = buffer.GetSpan(1 + 8);
        span[0] = ValueKind.Double;
        BinaryPrimitives.WriteDoubleLittleEndian(span[1..], Convert.ToDouble(value, CultureInfo.InvariantCulture));
        buffer.Advance(1 + 8);
    }

    private static void AppendDecimalValue(decimal value, ArrayBufferWriter<byte> buffer)
    {
        var kindSpan = buffer.GetSpan(1);
        kindSpan[0] = ValueKind.Decimal;
        buffer.Advance(1);
        AppendUtf8Prefixed(value.ToString(CultureInfo.InvariantCulture), buffer);
    }

    private static void AppendJsonElementValue(JsonElement value, ArrayBufferWriter<byte> buffer)
    {
        var jsonLength = BinaryJsonTreeCodec.ComputeEncodedLength(value);
        var jsonSpan = buffer.GetSpan(jsonLength);
        _ = BinaryJsonTreeCodec.Write(value, jsonSpan);
        buffer.Advance(jsonLength);
    }
}
