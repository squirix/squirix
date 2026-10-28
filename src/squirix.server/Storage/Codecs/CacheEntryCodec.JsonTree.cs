using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Codecs;

internal static partial class CacheEntryCodec
{
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

        /// <summary>Decodes a tree straight into UTF-8 JSON and parses that once into a <see cref="JsonElement" />.</summary>
        /// <param name="source">The encoded tree.</param>
        /// <param name="element">The decoded element; <see langword="default" /> for a null root.</param>
        /// <param name="bytesRead">The number of bytes the tree spans.</param>
        /// <returns><see langword="true" /> when the tree decodes.</returns>
        /// <remarks>Repeated property names are kept as they were encoded.</remarks>
        internal static bool TryReadInternal(ReadOnlySpan<byte> source, out JsonElement element, out int bytesRead)
        {
            element = default;
            bytesRead = 0;
            if (source.IsEmpty)
                return false;

            if (source[0] == ValueKind.Null)
            {
                bytesRead = 1;
                return true;
            }

            using var buffer = new PooledJsonBuffer(source.Length);

            // Sync flush: the writer writes to memory; async Utf8JsonWriter disposal would allocate a state machine on every decoding.
#pragma warning disable MA0045
            using (var writer = new Utf8JsonWriter(buffer))
            {
                if (!JsonTreeReadCodec.TryWriteNode(source, writer, out bytesRead))
                    return false;

                writer.Flush();
            }
#pragma warning restore MA0045

            var reader = new Utf8JsonReader(buffer.WrittenSpan);
            element = JsonElement.ParseValue(ref reader);
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
            /// <summary>The nesting depth a JSON reader accepts by default; the decoded value is parsed with that reader.</summary>
            private const int MaxDepth = 64;

            internal static bool TryWriteNode(ReadOnlySpan<byte> source, Utf8JsonWriter writer, out int bytesRead)
            {
                bytesRead = 0;
                return !source.IsEmpty && source[0] switch
                {
                    ValueKind.Null => WriteNull(writer, out bytesRead),
                    ValueKind.Bool => TryWriteBool(source, writer, out bytesRead),
                    ValueKind.String => TryWriteString(source, writer, out bytesRead),
                    ValueKind.Int64 => TryWriteInt64(source, writer, out bytesRead),
                    ValueKind.Double => TryWriteDouble(source, writer, out bytesRead),
                    ValueKind.Decimal => TryWriteDecimal(source, writer, out bytesRead),
                    ValueKind.Object => TryWriteObject(source, writer, out bytesRead),
                    ValueKind.Array => TryWriteArray(source, writer, out bytesRead),
                    _ => false,
                };
            }

            private static bool TryWriteArray(ReadOnlySpan<byte> source, Utf8JsonWriter writer, out int bytesRead)
            {
                bytesRead = 0;
                if (source.Length < 1 + 4)
                    return false;

                var count = BinaryPrimitives.ReadUInt32LittleEndian(source[1..]);
                bytesRead = 5;
                if (IsTooDeep(writer))
                    return false;

                writer.WriteStartArray();
                for (var i = 0; i < count; i++)
                {
                    if (!TryWriteNode(source[bytesRead..], writer, out var itemBytes))
                        return false;

                    bytesRead += itemBytes;
                }

                writer.WriteEndArray();
                return true;
            }

            private static bool TryWriteBool(ReadOnlySpan<byte> source, Utf8JsonWriter writer, out int bytesRead)
            {
                bytesRead = 0;
                if (source.Length < 2)
                    return false;

                writer.WriteBooleanValue(source[1] != 0);
                bytesRead = 2;
                return true;
            }

            private static bool TryWriteDecimal(ReadOnlySpan<byte> source, Utf8JsonWriter writer, out int bytesRead)
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

            private static bool TryWriteDouble(ReadOnlySpan<byte> source, Utf8JsonWriter writer, out int bytesRead)
            {
                bytesRead = 0;
                if (source.Length < 1 + 8)
                    return false;

                writer.WriteNumberValue(BinaryPrimitives.ReadDoubleLittleEndian(source[1..]));
                bytesRead = 1 + 8;
                return true;
            }

            private static bool TryWriteInt64(ReadOnlySpan<byte> source, Utf8JsonWriter writer, out int bytesRead)
            {
                bytesRead = 0;
                if (source.Length < 1 + 8)
                    return false;

                writer.WriteNumberValue(BinaryPrimitives.ReadInt64LittleEndian(source[1..]));
                bytesRead = 1 + 8;
                return true;
            }

            private static bool TryWriteObject(ReadOnlySpan<byte> source, Utf8JsonWriter writer, out int bytesRead)
            {
                bytesRead = 0;
                if (source.Length < 1 + 2)
                    return false;

                var count = BinaryPrimitives.ReadUInt16LittleEndian(source[1..]);
                bytesRead = 3;
                if (IsTooDeep(writer))
                    return false;

                writer.WriteStartObject();
                for (var i = 0; i < count; i++)
                {
                    if (!TryReadUtf8PrefixedBytes(source[bytesRead..], out var name, out var nameBytes))
                        return false;

                    // Bytes that are not valid UTF-8 decode with replacement characters, as a string conversion does.
                    if (Utf8.IsValid(name))
                        writer.WritePropertyName(name);
                    else
                        writer.WritePropertyName(Encoding.UTF8.GetString(name));

                    bytesRead += nameBytes;
                    if (!TryWriteNode(source[bytesRead..], writer, out var valueBytes))
                        return false;

                    bytesRead += valueBytes;
                }

                writer.WriteEndObject();
                return true;
            }

            private static bool TryWriteString(ReadOnlySpan<byte> source, Utf8JsonWriter writer, out int bytesRead)
            {
                bytesRead = 0;
                if (!TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
                    return false;

                // Bytes that are not valid UTF-8 decode with replacement characters, as a string conversion does.
                if (Utf8.IsValid(stringBytes))
                    writer.WriteStringValue(stringBytes);
                else
                    writer.WriteStringValue(Encoding.UTF8.GetString(stringBytes));

                bytesRead = 1 + stringBytesRead;
                return true;
            }

            /// <summary>Detects a container that would be nested deeper than a JSON reader accepts by default, before it is written.</summary>
            /// <param name="writer">The writer about to open a container.</param>
            /// <returns><see langword="true" /> when opening another container would exceed <see cref="MaxDepth" /> levels.</returns>
            private static bool IsTooDeep(Utf8JsonWriter writer) => writer.CurrentDepth >= MaxDepth;

            private static bool WriteNull(Utf8JsonWriter writer, out int bytesRead)
            {
                writer.WriteNullValue();
                bytesRead = 1;
                return true;
            }

            private static bool TryReadUtf32Prefixed(ReadOnlySpan<byte> source, out ReadOnlySpan<byte> bytes, out int bytesRead)
            {
                bytes = default;
                bytesRead = 0;
                if (source.Length < 4)
                    return false;

                // A length past int.MaxValue reads as negative and fails the check.
                var length = int.CreateTruncating(BinaryPrimitives.ReadUInt32LittleEndian(source));
                if (!WireBounds.Fits(length, 4, source.Length))
                    return false;

                bytesRead = 4 + length;
                bytes = source.Slice(4, length);
                return true;
            }

            private static bool TryReadUtf8Prefixed(ReadOnlySpan<byte> source, out string text, out int bytesRead)
            {
                text = string.Empty;
                if (!TryReadUtf8PrefixedBytes(source, out var bytes, out bytesRead))
                    return false;

                text = Encoding.UTF8.GetString(bytes);
                return true;
            }

            private static bool TryReadUtf8PrefixedBytes(ReadOnlySpan<byte> source, out ReadOnlySpan<byte> bytes, out int bytesRead)
            {
                bytes = default;
                bytesRead = 0;
                if (source.Length < 2)
                    return false;

                var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
                bytesRead = 2 + length;
                if (!WireBounds.Fits(length, 2, source.Length))
                    return false;

                bytes = source.Slice(2, length);
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
                foreach (var unused in element.EnumerateObject())
                    propertyCount++;

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

        /// <summary>A growable byte buffer rented from the shared pool for one decode; disposing it returns the array.</summary>
        [Mutable]
        private sealed class PooledJsonBuffer : IBufferWriter<byte>, IDisposable
        {
            private byte[] _array;
            private int _written;

            internal PooledJsonBuffer(int initialCapacity)
            {
                _array = ArrayPool<byte>.Shared.Rent(Math.Max(256, initialCapacity * 2));
            }

            internal ReadOnlySpan<byte> WrittenSpan => _array.AsSpan(0, _written);

            public void Advance(int count) => _written += count;

            public void Dispose()
            {
                ArrayPool<byte>.Shared.ReturnCleared(_array);
                _array = [];
            }

            public Memory<byte> GetMemory(int sizeHint = 0)
            {
                Ensure(sizeHint);
                return _array.AsMemory(_written);
            }

            public Span<byte> GetSpan(int sizeHint = 0)
            {
                Ensure(sizeHint);
                return _array.AsSpan(_written);
            }

            private void Ensure(int sizeHint)
            {
                var needed = _written + Math.Max(sizeHint, 1);
                if (needed <= _array.Length)
                    return;

                var grown = ArrayPool<byte>.Shared.Rent(Math.Max(needed, _array.Length * 2));
                _array.AsSpan(0, _written).CopyTo(grown);
                ArrayPool<byte>.Shared.ReturnCleared(_array);
                _array = grown;
            }
        }
    }
}
