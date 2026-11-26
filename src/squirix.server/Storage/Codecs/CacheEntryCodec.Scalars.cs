using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Codecs;

internal static partial class CacheEntryCodec
{
    /// <summary>Value encoding helpers for <see cref="CacheEntryCodec" />.</summary>
    private static class CacheEntryValueEncoding
    {
        internal static int ComputeLength(object? value) => value switch
        {
            StoredValue stored => stored.Kind switch
            {
                StoredValue.ValueKind.Null => 1,
                StoredValue.ValueKind.Boolean => 2,
                StoredValue.ValueKind.Integer or StoredValue.ValueKind.Double => 9,
                _ => 5 + stored.Bytes.Length,
            },
            null => 1,
            bool => 2,
            StoredText text => 5 + text.Bytes.Length,
            string s => 1 + 4 + Encoding.UTF8.GetByteCount(s),
            byte[] bytes => 1 + 4 + bytes.Length,
            sbyte or byte or short or ushort or int or uint or long => 1 + 8,
            float or double => 1 + 8,
            _ => ComputeDecimalOrStructuredLength(value),
        };

        internal static bool TryCoerceTo<T>(object? value, out T? result)
        {
            if (typeof(T) == typeof(StoredValue))
            {
                if (value is decimal)
                {
                    result = default;
                    return false;
                }

                var stored = value is StoredValue existing ? existing : StoredValue.FromDecoded(value);
                result = Unsafe.As<StoredValue, T>(ref stored);
                return true;
            }

            if (value == null)
            {
                result = default;
                return true;
            }

            if (value is StoredText text && typeof(T) != typeof(object))
                value = text.Decode();
            if (value is not T ok)
                return TryCoerceNumericOrJson(value, out result);
            result = ok;
            return true;
        }

        internal static bool TryReadInternal<T>(ReadOnlySpan<byte> source, bool materialize, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            return !source.IsEmpty && source[0] switch
            {
                ValueKind.Null => CacheEntryValueReadCodec.TryReadNullValue(out value, out bytesRead),
                ValueKind.Bool => CacheEntryValueReadCodec.TryReadBoolValue(source, materialize, out value, out bytesRead),
                ValueKind.String => CacheEntryValueReadCodec.TryReadStringValue<T>(source, materialize, out value, out bytesRead),
                ValueKind.Bytes => CacheEntryValueReadCodec.TryReadBytesValue(source, materialize, out value, out bytesRead),
                ValueKind.Int64 => CacheEntryValueReadCodec.TryReadInt64Value(source, materialize, out value, out bytesRead),
                ValueKind.Double => CacheEntryValueReadCodec.TryReadDoubleValue(source, materialize, out value, out bytesRead),
                ValueKind.Decimal => CacheEntryValueReadCodec.TryReadDecimalValue(source, materialize, out value, out bytesRead),
                ValueKind.Json => CacheEntryValueReadCodec.TryReadJsonValue(source, materialize, out value, out bytesRead),
                _ => false,
            };
        }

        internal static int WriteInternal(object? value, Span<byte> destination) => value switch
        {
            StoredValue stored => WriteStored(in stored, destination),
            null => CacheEntryValueWriteCodec.WriteNull(destination),
            bool b => CacheEntryValueWriteCodec.WriteBool(b, destination),
            StoredText text => CacheEntryValueWriteCodec.WritePayload(ValueKind.String, text.Bytes, destination),
            string s => CacheEntryValueWriteCodec.WriteString(s, destination),
            byte[] bytes => CacheEntryValueWriteCodec.WritePayload(ValueKind.Bytes, bytes, destination),
            sbyte or byte or short or ushort or int or uint or long => CacheEntryValueWriteCodec.WriteInt64(value, destination),
            float or double => CacheEntryValueWriteCodec.WriteDouble(value, destination),
            _ => WriteDecimalOrStructuredValue(value, destination),
        };

        private static int WriteStored(in StoredValue value, Span<byte> destination) => value.Kind switch
        {
            StoredValue.ValueKind.Null => CacheEntryValueWriteCodec.WriteNull(destination),
            StoredValue.ValueKind.Boolean => CacheEntryValueWriteCodec.WriteBool(value.Boolean, destination),
            StoredValue.ValueKind.Integer => CacheEntryValueWriteCodec.WriteBits(ValueKind.Int64, value.Integer, destination),
            StoredValue.ValueKind.Double => CacheEntryValueWriteCodec.WriteBits(ValueKind.Double, value.Integer, destination),
            StoredValue.ValueKind.Text => CacheEntryValueWriteCodec.WritePayload(ValueKind.String, value.Bytes, destination),
            StoredValue.ValueKind.Binary => CacheEntryValueWriteCodec.WritePayload(ValueKind.Bytes, value.Bytes, destination),
            StoredValue.ValueKind.Json => CacheEntryValueWriteCodec.WritePayload(ValueKind.Json, value.Bytes, destination),
            _ => throw new InvalidOperationException("Invalid stored cache value."),
        };

        private static int ComputeDecimalOrStructuredLength(object value) => value switch
        {
            decimal m => 1 + 2 + Encoding.UTF8.GetByteCount(m.ToString(CultureInfo.InvariantCulture)),
            JsonUtf8Value json => 1 + 4 + json.Length,
            _ => ComputeLength(NodeCacheEntry<object?>.NormalizeValue(value)),
        };

        private static int WriteDecimalOrStructuredValue(object value, Span<byte> destination) => value switch
        {
            decimal m => CacheEntryValueWriteCodec.WriteDecimal(m, destination),
            JsonUtf8Value json => CacheEntryValueWriteCodec.WritePayload(ValueKind.Json, json.Bytes.Span, destination),
            _ => WriteInternal(NodeCacheEntry<object?>.NormalizeValue(value), destination),
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

            if (number is < int.MinValue or > int.MaxValue)
            {
                result = default;
                return false;
            }

            result = Reinterpret<T, int>(int.CreateTruncating(number));
            return true;
        }

        private static bool TryCoerceJsonElement<T>(object value, out T? result)
        {
            if (value is JsonElement element)
            {
                result = Reinterpret<T, JsonElement>(element);
                return true;
            }

            if (value is JsonUtf8Value json)
            {
                result = Reinterpret<T, JsonElement>(json.ToJsonElement());
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
    }

    /// <summary>Read-side value decoding helpers for <see cref="CacheEntryCodec" />.</summary>
    private static class CacheEntryValueReadCodec
    {
        internal static bool TryReadBoolValue(ReadOnlySpan<byte> source, bool materialize, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (source.Length < 2)
                return false;

            if (materialize)
                value = source[1] != 0;
            bytesRead = 2;
            return true;
        }

        internal static bool TryReadBytesValue(ReadOnlySpan<byte> source, bool materialize, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (!TagEncoding.TryReadUtf32Prefixed(source[1..], out var rawBytes, out var rawBytesRead))
                return false;

            if (materialize)
                value = BufferEx.CopyToOwned(rawBytes);
            bytesRead = 1 + rawBytesRead;
            return true;
        }

        internal static bool TryReadDecimalValue(ReadOnlySpan<byte> source, bool materialize, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (!TagEncoding.TryReadUtf8Prefixed(source[1..], true, out var decimalText, out var decimalBytesRead))
                return false;

            if (!decimal.TryParse(decimalText, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalValue))
                return false;

            if (materialize)
                value = decimalValue;
            bytesRead = 1 + decimalBytesRead;
            return true;
        }

        internal static bool TryReadDoubleValue(ReadOnlySpan<byte> source, bool materialize, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (source.Length < 1 + 8)
                return false;

            if (materialize)
                value = BinaryPrimitives.ReadDoubleLittleEndian(source[1..]);
            bytesRead = 1 + 8;
            return true;
        }

        internal static bool TryReadInt64Value(ReadOnlySpan<byte> source, bool materialize, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (source.Length < 1 + 8)
                return false;

            if (materialize)
                value = BinaryPrimitives.ReadInt64LittleEndian(source[1..]);
            bytesRead = 1 + 8;
            return true;
        }

        internal static bool TryReadJsonValue(ReadOnlySpan<byte> source, bool materialize, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (!TagEncoding.TryReadUtf32Prefixed(source[1..], out var json, out var jsonBytesRead))
                return false;

            // The source is a pooled or windowed buffer that is overwritten once the caller moves on, so the value copies the bytes.
            if (materialize)
            {
                if (!JsonUtf8Value.TryCreateCopy(json, out var jsonValue))
                    return false;

                value = jsonValue;
            }
            else if (!JsonUtf8Validator.IsValid(json))
            {
                return false;
            }

            bytesRead = 1 + jsonBytesRead;
            return true;
        }

        internal static bool TryReadNullValue(out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 1;
            return true;
        }

        internal static bool TryReadStringValue<T>(ReadOnlySpan<byte> source, bool materialize, out object? value, out int bytesRead)
        {
            value = null;
            bytesRead = 0;
            if (!TagEncoding.TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
                return false;

            if (materialize)
            {
                value = (typeof(T) == typeof(object) || typeof(T) == typeof(StoredValue)) && System.Text.Unicode.Utf8.IsValid(stringBytes)
                    ? new StoredText(Squirix.Transport.Wire.Codec.WireReader.CopyBytes(stringBytes))
                    : Encoding.UTF8.GetString(stringBytes);
            }

            bytesRead = 1 + stringBytesRead;
            return true;
        }
    }

    /// <summary>Write-side value encoding helpers for <see cref="CacheEntryCodec" />.</summary>
    private static class CacheEntryValueWriteCodec
    {
        internal static int WriteBool(bool value, Span<byte> destination)
        {
            destination[0] = ValueKind.Bool;
            destination[1] = value ? True : False;

            return 2;
        }

        internal static int WriteBits(byte kind, long bits, Span<byte> destination)
        {
            destination[0] = kind;
            BinaryPrimitives.WriteInt64LittleEndian(destination[1..], bits);
            return 1 + sizeof(long);
        }

        internal static int WritePayload(byte kind, ReadOnlySpan<byte> bytes, Span<byte> destination)
        {
            destination[0] = kind;
            return 1 + TagEncoding.WriteUtf32Prefixed(bytes, destination[1..]);
        }

        internal static int WriteDecimal(decimal value, Span<byte> destination)
        {
            destination[0] = ValueKind.Decimal;
            return 1 + TagEncoding.WriteUtf8Prefixed(value.ToString(CultureInfo.InvariantCulture), destination[1..]);
        }

        internal static int WriteDouble(object value, Span<byte> destination)
        {
            destination[0] = ValueKind.Double;
            BinaryPrimitives.WriteDoubleLittleEndian(destination[1..], Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return 1 + 8;
        }

        internal static int WriteInt64(object value, Span<byte> destination)
        {
            destination[0] = ValueKind.Int64;
            BinaryPrimitives.WriteInt64LittleEndian(destination[1..], Convert.ToInt64(value, CultureInfo.InvariantCulture));
            return 1 + 8;
        }

        internal static int WriteNull(Span<byte> destination)
        {
            destination[0] = ValueKind.Null;
            return 1;
        }

        internal static int WriteString(string value, Span<byte> destination)
        {
            destination[0] = ValueKind.String;
            return 1 + TagEncoding.WriteUtf32PrefixedString(value, destination[1..]);
        }
    }

    /// <summary>Tagged cache-value kinds in binary snapshot/journal payloads.</summary>
    private static class ValueKind
    {
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

        /// <summary>One JSON value kept as the UTF-8 text the client sent, after a u32 length.</summary>
        internal const byte Json = 7;

        /// <summary>Null value.</summary>
        internal const byte Null = 0;

        /// <summary>UTF-8 string value.</summary>
        internal const byte String = 2;
    }
}
