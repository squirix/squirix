using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Codecs;

internal static partial class CacheEntryCodec
{
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

            // A length past int.MaxValue reads as negative and fails the check.
            var length = int.CreateTruncating(BinaryPrimitives.ReadUInt32LittleEndian(source));
            if (!WireBounds.Fits(length, 4, source.Length))
                return false;

            bytesRead = 4 + length;
            bytes = source.Slice(4, length);
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
            if (!WireBounds.Fits(length, 2, source.Length))
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
}
