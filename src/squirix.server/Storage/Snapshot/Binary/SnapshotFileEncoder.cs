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
    /// <summary>Target size of one batched file write; a larger record gets a buffer of its own size.</summary>
    internal const int WriteBatchSize = 1024 * 1024;

    /// <summary>Measures the snapshot file and records every record body length so the writer does not measure them again.</summary>
    /// <param name="items">The cache entries, written first.</param>
    /// <param name="idempotencyRecords">The idempotency records, written after the entries.</param>
    /// <param name="bodyLengths">Receives one body length per entry and then per idempotency record; needs at least their combined count.</param>
    /// <returns>The total file size and the longest encoded record.</returns>
    internal static (long TotalFileSize, int MaxRecordLength) ComputeWriteMetrics(
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        int[] bodyLengths)
    {
        long total = SnapshotCodec.FileHeaderSize + SnapshotCodec.FileFooterSize;
        var maxRecordLength = 0;
        for (var i = 0; i < items.Count; i++)
        {
            var (key, entry) = items[i];
            var bodyLength = SnapshotCodec.ComputeEntryBodyLength(key, entry);
            bodyLengths[i] = bodyLength;
            var recordLength = SnapshotCodec.ComputeRecordLength(bodyLength);
            total += recordLength;
            if (recordLength > maxRecordLength)
                maxRecordLength = recordLength;
        }

        for (var i = 0; i < idempotencyRecords.Count; i++)
        {
            var bodyLength = IdempotencyCodec.ComputeEncodedLength(idempotencyRecords[i]);
            bodyLengths[items.Count + i] = bodyLength;
            var recordLength = SnapshotCodec.ComputeRecordLength(bodyLength);
            total += recordLength;
            if (recordLength > maxRecordLength)
                maxRecordLength = recordLength;
        }

        return (total, maxRecordLength);
    }

    /// <summary>Writes the whole snapshot file, encoding records into <paramref name="writeBuffer" /> and writing it whenever the next record would not fit.</summary>
    /// <param name="destination">The file handle.</param>
    /// <param name="items">The cache entries.</param>
    /// <param name="idempotencyRecords">The idempotency records.</param>
    /// <param name="bodyLengths">The body lengths recorded by <see cref="ComputeWriteMetrics" />.</param>
    /// <param name="writeBuffer">A buffer at least as long as the longest record.</param>
    /// <param name="totalFileSize">The file size reported by <see cref="ComputeWriteMetrics" />.</param>
    /// <param name="cancellationToken">The cancellation token, checked before every record.</param>
    /// <returns>A task that completes when the footer is written.</returns>
    internal static async Task WriteFileAsync(
        SafeFileHandle destination,
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        int[] bodyLengths,
        byte[] writeBuffer,
        long totalFileSize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        RandomAccess.SetLength(destination, totalFileSize);

        long offset = 0;
        var crc = Crc32C.Append(Crc32C.InitialValue, SnapshotCodec.Version);
        WriteFileHeader(destination, ref offset);

        var pending = 0;
        for (var i = 0; i < items.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var recordLength = SnapshotCodec.ComputeRecordLength(bodyLengths[i]);
            if (recordLength > writeBuffer.Length - pending)
            {
                crc = await FlushBatchAsync(destination, writeBuffer, pending, offset, crc, cancellationToken).ConfigureAwait(false);
                offset += pending;
                pending = 0;
            }

            var (key, entry) = items[i];
            EncodeEntryRecord(writeBuffer.AsSpan(pending, recordLength), key, entry);
            pending += recordLength;
        }

        for (var i = 0; i < idempotencyRecords.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var recordLength = SnapshotCodec.ComputeRecordLength(bodyLengths[items.Count + i]);
            if (recordLength > writeBuffer.Length - pending)
            {
                crc = await FlushBatchAsync(destination, writeBuffer, pending, offset, crc, cancellationToken).ConfigureAwait(false);
                offset += pending;
                pending = 0;
            }

            EncodeIdempotencyRecord(writeBuffer.AsSpan(pending, recordLength), idempotencyRecords[i]);
            pending += recordLength;
        }

        cancellationToken.ThrowIfCancellationRequested();
        crc = await FlushBatchAsync(destination, writeBuffer, pending, offset, crc, cancellationToken).ConfigureAwait(false);
        offset += pending;
        WriteFileFooter(destination, offset, crc);
    }

    private static void EncodeEntryRecord(Span<byte> record, CacheKey key, NodeCacheEntry<object?> entry)
    {
        SnapshotCodec.WriteEntryBody(key, entry, record[SnapshotCodec.RecordHeaderSize..^SnapshotCodec.RecordFooterSize]);
        SnapshotCodec.SealRecord(record, RecordKind.Entry);
    }

    private static void EncodeIdempotencyRecord(Span<byte> record, PersistedIdempotencyRecord idempotencyRecord)
    {
        IdempotencyCodec.Write(idempotencyRecord, record[SnapshotCodec.RecordHeaderSize..^SnapshotCodec.RecordFooterSize]);
        SnapshotCodec.SealRecord(record, RecordKind.Idempotency);
    }

    private static async Task<uint> FlushBatchAsync(SafeFileHandle destination, byte[] writeBuffer, int count, long offset, uint crc, CancellationToken cancellationToken)
    {
        if (count == 0)
            return crc;

        var batch = writeBuffer.AsMemory(0, count);
        crc = Crc32C.Append(crc, batch.Span);
        await RandomAccess.WriteAsync(destination, batch, offset, cancellationToken).ConfigureAwait(false);
        return crc;
    }

    private static void WriteFileFooter(SafeFileHandle destination, long offset, uint crc)
    {
        Span<byte> footer = stackalloc byte[SnapshotCodec.FileFooterSize];
        BinaryPrimitives.WriteUInt32LittleEndian(footer, Crc32C.Finalize(crc));
        RandomAccess.Write(destination, footer, offset);
    }

    private static void WriteFileHeader(SafeFileHandle destination, ref long offset)
    {
        Span<byte> header = stackalloc byte[SnapshotCodec.FileHeaderSize];
        SnapshotCodec.WriteFileHeader(header);
        RandomAccess.Write(destination, header, offset);
        offset += header.Length;
    }
}
