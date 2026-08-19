using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Codecs;

/// <summary>Binary cache-entry encoding for journal and snapshot payloads.</summary>
internal static class CacheEntryCodec
{
    private const byte False = 0;
    private const int MaxUtf16StringLength = ushort.MaxValue;
    private const byte True = 1;

    internal static int ComputeEncodedLength(NodeCacheEntry<object?> entry)
    {
        var length = 1 + 1 + 8;
        length += TagEncoding.ComputeLength(entry.Tags);
        length += CacheEntryValueEncoding.ComputeLength(entry.Value);
        if (entry.ExpiresUtc != null)
            length += 8;

        if (entry.Expiration != null)
            length += 8;

        return length;
    }

    internal static bool TryMapEntry<T>(NodeCacheEntry<object?> entry, out NodeCacheEntry<T>? mapped)
    {
        if (!CacheEntryValueEncoding.TryCoerceTo<T>(entry.Value, out var typedValue))
        {
            mapped = null;
            return false;
        }

        mapped = new NodeCacheEntry<T>(typedValue, entry.Version, entry.ExpiresUtc, entry.Expiration, entry.Tags);
        return true;
    }

    internal static bool TryRead<T>(ReadOnlySpan<byte> source, out NodeCacheEntry<T>? entry, out int bytesRead)
    {
        entry = null;
        bytesRead = 0;
        if (!TryReadEnvelope(source, out var envelope))
            return false;

        if (!CacheEntryValueEncoding.TryCoerceTo<T>(envelope.Value, out var typedValue))
            return false;

        // bytesRead reports the full envelope consumed so callers can advance snapshot/journal cursors.
        entry = CreateEntry(typedValue, in envelope);
        bytesRead = envelope.BytesRead;
        return true;
    }

    internal static void Write(NodeCacheEntry<object?> entry, Span<byte> destination)
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
        offset += TagEncoding.WriteTag(entry.Tags, destination[offset..]);
        _ = CacheEntryValueEncoding.WriteInternal(entry.Value, destination[offset..]);
    }

    private static NodeCacheEntry<T> CreateEntry<T>(T? typedValue, in ReadEnvelope e) => new(typedValue, e.Version, e.ExpiresUtc, e.Expiration, e.Tags);

    private static bool TryReadEnvelope(ReadOnlySpan<byte> source, out ReadEnvelope envelope)
    {
        envelope = default;
        if (source.Length < 1 + 1 + 8)
            return false;

        var offset = 0;
        if (!TryReadExpirationFields(source, ref offset, out var expiresUtc, out var expiration))
            return false;

        if (!TryReadVersion(source, ref offset, out var version))
            return false;

        if (!TryReadTagsAndValue(source, ref offset, out var tags, out var value))
            return false;

        envelope = new ReadEnvelope(expiresUtc, expiration, version, tags, value, offset);
        return true;
    }

    private static bool TryReadExpirationFields(ReadOnlySpan<byte> source, ref int offset, out DateTime? expiresUtc, out TimeSpan? expiration)
    {
        expiresUtc = null;
        expiration = null;

        // Entry envelope: optional expires/expiration flags, fixed version, tags, then typed value payload.
        if (source[offset++] != 0)
        {
            if (source.Length < offset + 8)
                return false;

            expiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(source[offset..])).UtcDateTime;
            offset += 8;
        }

        if (source[offset++] == 0)
            return true;
        if (source.Length < offset + 8)
            return false;

        expiration = TimeSpan.FromTicks(BinaryPrimitives.ReadInt64LittleEndian(source[offset..]));
        offset += 8;

        return true;
    }

    private static bool TryReadTagsAndValue(ReadOnlySpan<byte> source, ref int offset, out FrozenDictionary<string, string>? tags, out object? value)
    {
        tags = null;
        value = null;

        // Tags and value are length-prefixed sections; both must parse before coercion to T.
        if (!TagEncoding.TryReadTag(source[offset..], out tags, out var tagsBytes))
            return false;

        offset += tagsBytes;
        if (!CacheEntryValueEncoding.TryReadInternal(source[offset..], out value, out var valueBytes))
            return false;

        offset += valueBytes;
        return true;
    }

    private static bool TryReadVersion(ReadOnlySpan<byte> source, ref int offset, out long version)
    {
        version = 0;
        if (source.Length < offset + 8)
            return false;

        version = BinaryPrimitives.ReadInt64LittleEndian(source[offset..]);
        offset += 8;
        return true;
    }

    [Immutable]
    private readonly record struct ReadEnvelope
    {
        internal ReadEnvelope(DateTime? expiresUtc, TimeSpan? expiration, long version, FrozenDictionary<string, string>? tags, object? value, int bytesRead)
        {
            ExpiresUtc = expiresUtc;
            Expiration = expiration;
            Version = version;
            Tags = tags;
            Value = value;
            BytesRead = bytesRead;
        }

        internal int BytesRead { get; }

        internal TimeSpan? Expiration { get; }

        internal DateTime? ExpiresUtc { get; }

        internal FrozenDictionary<string, string>? Tags { get; }

        internal object? Value { get; }

        internal long Version { get; }
    }

    /// <summary>Value encoding helpers for <see cref="CacheEntryCodec" />.</summary>
    private static class CacheEntryValueEncoding
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
            JsonElement je => JsonTreeCodec.ComputeEncodedLengthInternal(je),
            _ => JsonTreeCodec.ComputeEncodedLengthInternal(SerializerProvider.Instance.SerializeToElement(value)),
        };

        internal static bool TryCoerceTo<T>(object? value, out T? result)
        {
            if (value == null)
            {
                result = default;
                return true;
            }

            if (value is not T ok)
                return TryCoerceNumericOrJson(value, out result);
            result = ok;
            return true;
        }

        internal static bool TryReadInternal(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
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

        internal static int WriteInternal(object? value, Span<byte> destination) => value switch
        {
            null => WriteNull(destination),
            bool b => WriteBool(b, destination),
            string s => WriteString(s, destination),
            byte[] bytes => WriteBytes(bytes, destination),
            sbyte or byte or short or ushort or int or uint or long => WriteInt64(value, destination),
            float or double => WriteDouble(value, destination),
            decimal m => WriteDecimal(m, destination),
            JsonElement je => JsonTreeCodec.WriteInternal(je, destination),
            _ => WriteSerializedObject(value, destination),
        };

        private static TTarget Reinterpret<TTarget, TValue>(TValue value)
            where TValue : struct => Unsafe.As<TValue, TTarget>(ref value);

        private static bool TryCoerceFloatingPoint<T>(object value, out T? result)
        {
            if (value is not double number)
            {
                result = default;
                return false;
            }

            if (typeof(T) == typeof(double))
            {
                result = Reinterpret<T, double>(number);
                return true;
            }

            result = Reinterpret<T, float>(Convert.ToSingle(number));
            return true;
        }

        private static bool TryCoerceInteger<T>(object value, out T? result)
        {
            if (value is not long number)
            {
                result = default;
                return false;
            }

            if (typeof(T) == typeof(long))
            {
                result = Reinterpret<T, long>(number);
                return true;
            }

            result = Reinterpret<T, int>(int.CreateChecked(number));
            return true;
        }

        private static bool TryCoerceJsonElement<T>(object value, out T? result)
        {
            if (value is JsonElement element)
            {
                result = Reinterpret<T, JsonElement>(element);
                return true;
            }

            result = default;
            return false;
        }

        private static bool TryCoerceNumericOrJson<T>(object value, out T? result)
        {
            if (typeof(T) == typeof(JsonElement))
                return TryCoerceJsonElement(value, out result);

            if (typeof(T) == typeof(int) || typeof(T) == typeof(long))
                return TryCoerceInteger(value, out result);

            if (typeof(T) == typeof(float) || typeof(T) == typeof(double))
                return TryCoerceFloatingPoint(value, out result);

            result = default;
            return false;
        }

        private static bool TryReadBoolValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (source.Length < 2)
                return false;

            value = source[1] != 0;
            bytesRead = 2;
            return true;
        }

        private static bool TryReadBytesValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (!TagEncoding.TryReadUtf32Prefixed(source[1..], out var rawBytes, out var rawBytesRead))
                return false;

            value = BufferEx.CopyToOwned(rawBytes);
            bytesRead = 1 + rawBytesRead;
            return true;
        }

        private static bool TryReadDecimalValue(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (!TagEncoding.TryReadUtf8Prefixed(source[1..], out var decimalText, out var decimalBytesRead))
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
            if (!JsonTreeCodec.TryReadInternal(source, out var element, out bytesRead))
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
            if (!TagEncoding.TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
                return false;

            value = Encoding.UTF8.GetString(stringBytes);
            bytesRead = 1 + stringBytesRead;
            return true;
        }

        private static int WriteBool(bool value, Span<byte> destination)
        {
            destination[0] = ValueKind.Bool;
            destination[1] = value ? True : False;

            return 2;
        }

        private static int WriteBytes(byte[] bytes, Span<byte> destination)
        {
            destination[0] = ValueKind.Bytes;
            return 1 + TagEncoding.WriteUtf32Prefixed(bytes, destination[1..]);
        }

        private static int WriteDecimal(decimal value, Span<byte> destination)
        {
            destination[0] = ValueKind.Decimal;
            return 1 + TagEncoding.WriteUtf8Prefixed(value.ToString(CultureInfo.InvariantCulture), destination[1..]);
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
            JsonTreeCodec.WriteInternal(SerializerProvider.Instance.SerializeToElement(value), destination);

        private static int WriteString(string value, Span<byte> destination)
        {
            destination[0] = ValueKind.String;
            return 1 + TagEncoding.WriteUtf32PrefixedString(value, destination[1..]);
        }
    }

    /// <summary>Encodes and decodes <see cref="JsonElement" /> trees without persisting UTF-8 JSON blobs.</summary>
    private static class JsonTreeCodec
    {
        internal static int ComputeEncodedLengthInternal(JsonElement element) => element.ValueKind switch
        {
            JsonValueKind.Null or JsonValueKind.Undefined => 1,
            JsonValueKind.True or JsonValueKind.False => 2,
            JsonValueKind.String => 1 + 4 + Encoding.UTF8.GetByteCount(element.GetString() ?? string.Empty),
            JsonValueKind.Number => 1 + 8,
            JsonValueKind.Object => ComputeObjectLength(element),
            JsonValueKind.Array => ComputeArrayLength(element),
            _ => throw new InvalidDataException("Unsupported JSON value kind for binary tree encoding."),
        };

        internal static bool TryReadInternal(ReadOnlySpan<byte> source, out JsonElement element, out int bytesRead)
        {
            element = default;
            bytesRead = 0;
            if (!JsonTreeReadCodec.TryReadNode(source, out var node, out bytesRead))
                return false;

            element = node == null ? default : JsonSerializer.SerializeToElement(node, JsonTreeJsonContext.Default.JsonNode);
            return true;
        }

        internal static int WriteInternal(JsonElement element, Span<byte> destination) => JsonTreeWriteCodec.WriteCore(element, destination);

        private static int ComputeArrayLength(JsonElement element)
        {
            var length = 1 + 4;
            foreach (var item in element.EnumerateArray())
                length += ComputeEncodedLengthInternal(item);

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

                length += 2 + nameBytes + ComputeEncodedLengthInternal(property.Value);
            }

            return length;
        }

        private static class JsonTreeReadCodec
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

                node = JsonValue.Create(source[1] != 0);
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
                if (length > uint.CreateTruncating(source.Length - 4))
                    return false;

                var lengthInt = Convert.ToInt32(length);
                bytesRead = 4 + lengthInt;
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

        private static class JsonTreeWriteCodec
        {
            internal static int WriteCore(JsonElement element, Span<byte> destination)
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
                    offset += WriteCore(item, destination[offset..]);

                return offset;
            }

            private static int WriteBool(bool value, Span<byte> destination)
            {
                destination[0] = ValueKind.Bool;
                destination[1] = value ? True : False;
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
                    offset += WriteCore(property.Value, destination[offset..]);
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
        }
    }

    /// <summary>Tag encoding helpers for <see cref="CacheEntryCodec" />.</summary>
    private static class TagEncoding
    {
        internal static int ComputeLength(FrozenDictionary<string, string>? tags)
        {
            if (tags == null || tags.Count == 0)
                return 2;

            if (tags.Count > ushort.MaxValue)
                throw new InvalidDataException($"Tag count {tags.Count} exceeds the maximum of {ushort.MaxValue}.");

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

        internal static bool TryReadTag(ReadOnlySpan<byte> source, out FrozenDictionary<string, string>? tags, out int bytesRead)
        {
            tags = null;
            bytesRead = 0;
            if (source.Length < 2)
                return false;

            var count = BinaryPrimitives.ReadUInt16LittleEndian(source);
            bytesRead = 2;
            if (count == 0)
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

        internal static bool TryReadUtf32Prefixed(ReadOnlySpan<byte> source, out ReadOnlySpan<byte> bytes, out int bytesRead)
        {
            bytes = default;
            bytesRead = 0;
            if (source.Length < 4)
                return false;

            var length = BinaryPrimitives.ReadUInt32LittleEndian(source);
            if (length > uint.CreateTruncating(source.Length - 4))
                return false;

            var lengthInt = Convert.ToInt32(length);
            bytesRead = 4 + lengthInt;
            bytes = source.Slice(4, lengthInt);
            return true;
        }

        internal static bool TryReadUtf8Prefixed(ReadOnlySpan<byte> source, out string text, out int bytesRead)
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

        internal static int WriteTag(FrozenDictionary<string, string>? tags, Span<byte> destination)
        {
            if (tags == null || tags.Count == 0)
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

        internal static int WriteUtf32Prefixed(ReadOnlySpan<byte> bytes, Span<byte> destination)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(destination, uint.CreateTruncating(bytes.Length));
            bytes.CopyTo(destination[4..]);
            return 4 + bytes.Length;
        }

        internal static int WriteUtf32PrefixedString(string text, Span<byte> destination)
        {
            var byteCount = Encoding.UTF8.GetByteCount(text);
            BinaryPrimitives.WriteUInt32LittleEndian(destination, uint.CreateTruncating(byteCount));
            _ = Encoding.UTF8.GetBytes(text, destination[4..]);
            return 4 + byteCount;
        }

        internal static int WriteUtf8Prefixed(string text, Span<byte> destination)
        {
            var byteCount = Encoding.UTF8.GetByteCount(text);
            if (byteCount > MaxUtf16StringLength)
                throw new InvalidDataException("Snapshot string exceeds maximum encoded length.");

            BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(byteCount));
            _ = Encoding.UTF8.GetBytes(text, destination[2..]);
            return 2 + byteCount;
        }
    }

    /// <summary>Tagged cache-value kinds in binary snapshot/journal payloads.</summary>
    private static class ValueKind
    {
        /// <summary>JSON array encoded as a recursive binary tree.</summary>
        internal const byte Array = 8;

        /// <summary>Boolean value.</summary>
        internal const byte Bool = 1;

        /// <summary>Raw byte array value.</summary>
        internal const byte Bytes = 3;

        /// <summary>Decimal serialized as invariant text.</summary>
        internal const byte Decimal = 6;

        /// <summary>IEEE double value.</summary>
        internal const byte Double = 5;

        /// <summary>64-bit integer value.</summary>
        internal const byte Int64 = 4;

        /// <summary>Null value.</summary>
        internal const byte Null = 0;

        /// <summary>JSON object encoded as a recursive binary tree.</summary>
        internal const byte Object = 7;

        /// <summary>UTF-8 string value.</summary>
        internal const byte String = 2;
    }
}
