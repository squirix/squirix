using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Squirix.Server.Core;
using Squirix.Server.Runtime;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Binary cache-entry encoding for journal and snapshot payloads.</summary>
internal static class CacheEntryCodec
{
    internal const int MaxUtf16StringLength = ushort.MaxValue;

    /// <summary>
    /// Returns a value already in a directly-encodable form: primitives, strings, byte arrays and
    /// <see cref="JsonElement" /> pass through unchanged, while any other object is serialized to a
    /// <see cref="JsonElement" /> exactly once. Callers normalize before the
    /// <see cref="ComputeEncodedLength" /> / <see cref="Write" /> pair so an arbitrary object is not
    /// re-serialized on every length and write pass.
    /// </summary>
    /// <param name="value">The raw cache value.</param>
    /// <returns>The same value when directly encodable; otherwise its <see cref="JsonElement" /> form.</returns>
    internal static object? NormalizeValue(object? value) => value switch
    {
        null or bool or string or byte[] or sbyte or byte or short or ushort or int or uint or long or float or double or decimal or JsonElement => value,
        _ => SerializationProvider.Instance.SerializeToElement(value),
    };

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
        offset += CacheEntryTagEncoding.Write(entry.Tags, destination[offset..]);
        _ = CacheEntryValueEncoding.WriteInternal(entry.Value, destination[offset..]);
    }

    internal static int ComputeEncodedLength(NodeCacheEntry<object?> entry)
    {
        var length = 1 + 1 + 8;
        length += CacheEntryTagEncoding.ComputeLength(entry.Tags);
        length += CacheEntryValueEncoding.ComputeLength(entry.Value);
        if (entry.ExpiresUtc is not null)
            length += 8;

        if (entry.Expiration is not null)
            length += 8;

        return length;
    }

    private static NodeCacheEntry<T> CreateEntry<T>(T? typedValue, in ReadEnvelope envelope) =>
        new(typedValue, envelope.Version, envelope.ExpiresUtc, envelope.Expiration, envelope.Tags);

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
        if (source[offset++] is not 0)
        {
            if (source.Length < offset + 8)
                return false;

            expiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(BinaryPrimitives.ReadInt64LittleEndian(source[offset..])).UtcDateTime;
            offset += 8;
        }

        if (source[offset++] is 0)
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
        if (!CacheEntryTagEncoding.TryRead(source[offset..], out tags, out var tagsBytes))
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

        internal DateTime? ExpiresUtc { get; }

        internal TimeSpan? Expiration { get; }

        internal long Version { get; }

        internal FrozenDictionary<string, string>? Tags { get; }

        internal object? Value { get; }

        internal int BytesRead { get; }
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
            _ => JsonTreeCodec.ComputeEncodedLengthInternal(SerializationProvider.Instance.SerializeToElement(value)),
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
            JsonTreeCodec.WriteInternal(SerializationProvider.Instance.SerializeToElement(value), destination);

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
            if (!CacheEntryTagEncoding.TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
                return false;

            value = Encoding.UTF8.GetString(stringBytes);
            bytesRead = 1 + stringBytesRead;
            return true;
        }
    }

    /// <summary>Encodes and decodes <see cref="JsonElement" /> trees without persisting UTF-8 JSON blobs.</summary>
    private static class JsonTreeCodec
    {
        internal static bool TryReadInternal(ReadOnlySpan<byte> source, out JsonElement element, out int bytesRead)
        {
            element = default;
            bytesRead = 0;
            if (!JsonTreeReadCodec.TryReadNode(source, out var node, out bytesRead))
                return false;

            element = node is null ? default : JsonSerializer.SerializeToElement(node, JsonTreeJsonContext.Default.JsonNode);
            return true;
        }

        internal static int WriteInternal(JsonElement element, Span<byte> destination) => JsonTreeWriteCodec.WriteCore(element, destination);

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
    }
}
