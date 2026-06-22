using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Squirix.Server.Core;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

/// <summary>Encodes and decodes binary snapshot files; on-disk layout is documented in docs/snapshot-format.md.</summary>
[SuppressMessage("Design", "MA0181:Do not use cast", Justification = "Binary framing writes integral record headers.")]
[SuppressMessage("Design", "MA0191:Do not use the null-forgiving operator", Justification = "Out parameters are assigned before returning true.")]
internal static class Codec
{
    internal const byte Version = 1;

    internal const int FileHeaderSize = 5;

    internal const int FileFooterSize = 4;

    internal const int RecordHeaderSize = 5;

    private const int RecordFooterSize = 4;

    internal enum RecordKind : byte
    {
        /// <summary>Cache entry record.</summary>
        Entry = 1,

        /// <summary>Idempotency record.</summary>
        Idempotency = 2,
    }

    private static ReadOnlySpan<byte> Magic => "SQSS"u8;

    public static int ComputeEntryBodyLength(CacheKey key, CacheEntry<object?> entry)
    {
        var namespaceBytes = Encoding.UTF8.GetByteCount(key.Namespace);
        var keyBytes = Encoding.UTF8.GetByteCount(key.Key);
        if (namespaceBytes > ushort.MaxValue || keyBytes > ushort.MaxValue)
            throw new InvalidDataException("Snapshot key or namespace exceeds maximum encoded length.");

        return 2 + namespaceBytes + 2 + keyBytes + CacheEntryCodec.ComputeEncodedLength(entry);
    }

    public static int ComputeRecordLength(int bodyLength) => RecordHeaderSize + bodyLength + RecordFooterSize;

    public static void WriteEntryBody(CacheKey key, CacheEntry<object?> entry, Span<byte> destination)
    {
        var required = ComputeEntryBodyLength(key, entry);
        if (destination.Length < required)
            throw new ArgumentException("Destination span is too small for the encoded entry body.", nameof(destination));

        var offset = 0;
        offset += WriteUtf8Prefixed(key.Namespace, destination[offset..]);
        offset += WriteUtf8Prefixed(key.Key, destination[offset..]);
        CacheEntryCodec.Write(entry, destination[offset..]);
    }

    public static void WriteRecord(Span<byte> destination, RecordKind kind, ReadOnlySpan<byte> body)
    {
        if (destination.Length < ComputeRecordLength(body.Length))
            throw new ArgumentException("Destination span is too small for the encoded record.", nameof(destination));

        destination[0] = (byte)kind;
        BinaryPrimitives.WriteUInt32LittleEndian(destination[1..], (uint)body.Length);
        body.CopyTo(destination[RecordHeaderSize..]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[(RecordHeaderSize + body.Length)..], Crc32C.Compute(body));
    }

    public static void WriteFileHeader(Span<byte> destination)
    {
        if (destination.Length < FileHeaderSize)
            throw new ArgumentException("Destination span is too small for the file header.", nameof(destination));

        Magic.CopyTo(destination);
        destination[Magic.Length] = Version;
    }

    public static void ValidateFileHeader(ReadOnlySpan<byte> source)
    {
        if (source.Length < FileHeaderSize)
            throw new InvalidDataException("Binary snapshot header is truncated.");

        if (!source[..Magic.Length].SequenceEqual(Magic))
            throw new InvalidDataException("Binary snapshot magic is invalid.");

        if (source[Magic.Length] is not Version)
            throw new InvalidDataException($"Unsupported binary snapshot version: {source[Magic.Length]}.");
    }

    public static bool TryReadRecord(ReadOnlySpan<byte> source, out RecordKind kind, out ReadOnlySpan<byte> body, out int bytesRead)
    {
        kind = default;
        body = default;
        bytesRead = 0;
        if (source.Length < RecordHeaderSize + RecordFooterSize)
            return false;

        kind = (RecordKind)source[0];
        var bodyLength = BinaryPrimitives.ReadUInt32LittleEndian(source[1..]);
        var total = RecordHeaderSize + (int)bodyLength + RecordFooterSize;
        if (source.Length < total)
            return false;

        body = source.Slice(RecordHeaderSize, (int)bodyLength);
        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(source[(RecordHeaderSize + (int)bodyLength)..]);
        if (Crc32C.Compute(body) != expectedCrc)
            throw new InvalidDataException("Binary snapshot record CRC mismatch.");

        bytesRead = total;
        return true;
    }

    public static void ValidateFileFooter(ReadOnlySpan<byte> fileBytes, ReadOnlySpan<byte> crcPayload) =>
        ValidateFileFooter(fileBytes, Crc32C.Compute(crcPayload));

    public static void ValidateFileFooter(ReadOnlySpan<byte> fileBytes, uint crc)
    {
        if (fileBytes.Length < FileHeaderSize + FileFooterSize)
            throw new InvalidDataException("Binary snapshot file is truncated.");

        var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(fileBytes[^FileFooterSize..]);
        if (crc != expectedCrc)
            throw new InvalidDataException("Binary snapshot file CRC mismatch.");
    }

    public static bool TryReadEntryBody(ReadOnlySpan<byte> body, out CacheKey key, out CacheEntry<object?>? entry)
    {
        key = default;
        entry = null;
        if (!TryReadUtf8Prefixed(body, out var cacheNamespace, out var namespaceBytes))
            return false;

        if (!TryReadUtf8Prefixed(body[namespaceBytes..], out var cacheKey, out var keyBytes))
            return false;

        if (!CacheEntryCodec.TryRead<object?>(body[(namespaceBytes + keyBytes)..], out var parsedEntry, out _))
            return false;

        key = new CacheKey(PersistedCacheNamespace.Normalize(cacheNamespace), cacheKey);
        entry = parsedEntry;
        return true;
    }

    private static int WriteUtf8Prefixed(string text, Span<byte> destination)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        if (byteCount > ushort.MaxValue)
            throw new InvalidDataException("Snapshot string exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, destination[2..]);
        return 2 + byteCount;
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
