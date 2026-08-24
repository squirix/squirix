using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

/// <summary>Shared binary snapshot file encode/write helpers.</summary>
internal static class SnapshotFileEncoder
{
    internal static (long TotalFileSize, int MaxRecordLength) ComputeWriteMetrics(
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords)
    {
        long total = SnapshotCodec.FileHeaderSize + SnapshotCodec.FileFooterSize;
        var maxRecordLength = 0;
        for (var i = 0; i < items.Count; i++)
        {
            var (key, entry) = items[i];
            var recordLength = SnapshotCodec.ComputeRecordLength(SnapshotCodec.ComputeEntryBodyLength(key, entry));
            total += recordLength;
            if (recordLength > maxRecordLength)
                maxRecordLength = recordLength;
        }

        for (var i = 0; i < idempotencyRecords.Count; i++)
        {
            var record = idempotencyRecords[i];
            var recordLength = SnapshotCodec.ComputeRecordLength(IdempotencyCodec.ComputeEncodedLength(record));
            total += recordLength;
            if (recordLength > maxRecordLength)
                maxRecordLength = recordLength;
        }

        if (total > int.MaxValue)
            throw new InvalidDataException("Binary snapshot file exceeds maximum encoded length.");

        return (total, maxRecordLength);
    }

    internal static async Task WriteFileAsync(
        SafeFileHandle destination,
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        byte[] encodeBuffer,
        long totalFileSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        RandomAccess.SetLength(destination, totalFileSize);

        long offset = 0;
        var crc = Crc32C.Append(Crc32C.InitialValue, SnapshotCodec.Version);
        WriteFileHeader(destination, ref offset);

        for (var i = 0; i < items.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (key, entry) = items[i];
            var recordLength = WriteEntryRecord(encodeBuffer, key, entry);
            crc = await WriteRecordAndUpdateCrcAsync(destination, encodeBuffer, recordLength, offset, crc, cancellationToken).ConfigureAwait(false);
            offset += recordLength;
        }

        for (var i = 0; i < idempotencyRecords.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = idempotencyRecords[i];
            var recordLength = WriteIdempotencyRecord(encodeBuffer, record);
            crc = await WriteRecordAndUpdateCrcAsync(destination, encodeBuffer, recordLength, offset, crc, cancellationToken).ConfigureAwait(false);
            offset += recordLength;
        }

        cancellationToken.ThrowIfCancellationRequested();
        WriteFileFooter(destination, offset, crc);
    }

    private static void WriteFileHeader(SafeFileHandle destination, ref long offset)
    {
        Span<byte> header = stackalloc byte[SnapshotCodec.FileHeaderSize];
        SnapshotCodec.WriteFileHeader(header);
        RandomAccess.Write(destination, header, offset);
        offset += header.Length;
    }

    private static void WriteFileFooter(SafeFileHandle destination, long offset, uint crc)
    {
        Span<byte> footer = stackalloc byte[SnapshotCodec.FileFooterSize];
        BinaryPrimitives.WriteUInt32LittleEndian(footer, Crc32C.Finalize(crc));
        RandomAccess.Write(destination, footer, offset);
    }

    private static int WriteEntryRecord(byte[] encodeBuffer, CacheKey key, NodeCacheEntry<object?> entry)
    {
        var bodyLength = SnapshotCodec.ComputeEntryBodyLength(key, entry);
        var recordLength = SnapshotCodec.ComputeRecordLength(bodyLength);
        var body = encodeBuffer.AsSpan(SnapshotCodec.RecordHeaderSize, bodyLength);
        SnapshotCodec.WriteEntryBody(key, entry, body);
        SnapshotCodec.WriteRecord(encodeBuffer.AsSpan(0, recordLength), RecordKind.Entry, body);
        return recordLength;
    }

    private static int WriteIdempotencyRecord(byte[] encodeBuffer, PersistedIdempotencyRecord record)
    {
        var bodyLength = IdempotencyCodec.ComputeEncodedLength(record);
        var recordLength = SnapshotCodec.ComputeRecordLength(bodyLength);
        var body = encodeBuffer.AsSpan(SnapshotCodec.RecordHeaderSize, bodyLength);
        IdempotencyCodec.Write(record, body);
        SnapshotCodec.WriteRecord(encodeBuffer.AsSpan(0, recordLength), RecordKind.Idempotency, body);
        return recordLength;
    }

    private static async Task<uint> WriteRecordAndUpdateCrcAsync(SafeFileHandle destination, byte[] encodeBuffer, int recordLength, long offset, uint crc, CancellationToken cancellationToken)
    {
        var recordBytes = encodeBuffer.AsMemory(0, recordLength);
        crc = Crc32C.Append(crc, recordBytes.Span);
        await RandomAccess.WriteAsync(destination, recordBytes, offset, cancellationToken).ConfigureAwait(false);
        return crc;
    }
}
