using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

/// <summary>Shared binary snapshot file encode/write helpers.</summary>
[SuppressMessage("Design", "MA0182:Avoid unused internal types", Justification = "Used by Binary.SnapshotWriter and snapshot breakdown benchmarks.")]
internal static class SnapshotFileEncoder
{
    public static (long TotalFileSize, int MaxRecordLength) ComputeWriteMetrics(
        IReadOnlyList<(CacheKey Key, CacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords)
    {
        long total = Codec.FileHeaderSize + Codec.FileFooterSize;
        var maxRecordLength = 0;
        for (var i = 0; i < items.Count; i++)
        {
            var (key, entry) = items[i];
            var recordLength = Codec.ComputeRecordLength(Codec.ComputeEntryBodyLength(key, entry));
            total += recordLength;
            if (recordLength > maxRecordLength)
                maxRecordLength = recordLength;
        }

        for (var i = 0; i < idempotencyRecords.Count; i++)
        {
            var record = idempotencyRecords[i];
            var recordLength = Codec.ComputeRecordLength(IdempotencyCodec.ComputeEncodedLength(record));
            total += recordLength;
            if (recordLength > maxRecordLength)
                maxRecordLength = recordLength;
        }

        if (total > int.MaxValue)
            throw new InvalidDataException("Binary snapshot file exceeds maximum encoded length.");

        return (total, maxRecordLength);
    }

    public static async Task WriteFileAsync(
        FileStream destination,
        IReadOnlyList<(CacheKey Key, CacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        byte[] encodeBuffer,
        long totalFileSize,
        CancellationToken cancellationToken)
    {
        destination.SetLength(totalFileSize);
        destination.Position = 0;

        WriteFileHeader(destination);

        var crc = Crc32C.Append(Crc32C.InitialValue, [Codec.Version]);
        for (var i = 0; i < items.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (key, entry) = items[i];
            var recordLength = WriteEntryRecord(encodeBuffer, key, entry);
            crc = await WriteRecordAndUpdateCrcAsync(destination, encodeBuffer, recordLength, crc, cancellationToken).ConfigureAwait(false);
        }

        for (var i = 0; i < idempotencyRecords.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = idempotencyRecords[i];
            var recordLength = WriteIdempotencyRecord(encodeBuffer, record);
            crc = await WriteRecordAndUpdateCrcAsync(destination, encodeBuffer, recordLength, crc, cancellationToken).ConfigureAwait(false);
        }

        WriteFileFooter(destination, crc);
        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void WriteFileHeader(FileStream destination)
    {
        Span<byte> header = stackalloc byte[Codec.FileHeaderSize];
        Codec.WriteFileHeader(header);
        destination.Write(header);
    }

    private static void WriteFileFooter(FileStream destination, uint crc)
    {
        Span<byte> footer = stackalloc byte[Codec.FileFooterSize];
        BinaryPrimitives.WriteUInt32LittleEndian(footer, Crc32C.Finalize(crc));
        destination.Write(footer);
    }

    private static int WriteEntryRecord(byte[] encodeBuffer, CacheKey key, CacheEntry<object?> entry)
    {
        var bodyLength = Codec.ComputeEntryBodyLength(key, entry);
        var recordLength = Codec.ComputeRecordLength(bodyLength);
        var body = encodeBuffer.AsSpan(Codec.RecordHeaderSize, bodyLength);
        Codec.WriteEntryBody(key, entry, body);
        Codec.WriteRecord(encodeBuffer.AsSpan(0, recordLength), Codec.RecordKind.Entry, body);
        return recordLength;
    }

    private static int WriteIdempotencyRecord(byte[] encodeBuffer, PersistedIdempotencyRecord record)
    {
        var bodyLength = IdempotencyCodec.ComputeEncodedLength(record);
        var recordLength = Codec.ComputeRecordLength(bodyLength);
        var body = encodeBuffer.AsSpan(Codec.RecordHeaderSize, bodyLength);
        IdempotencyCodec.Write(record, body);
        Codec.WriteRecord(encodeBuffer.AsSpan(0, recordLength), Codec.RecordKind.Idempotency, body);
        return recordLength;
    }

    private static async Task<uint> WriteRecordAndUpdateCrcAsync(FileStream destination, byte[] encodeBuffer, int recordLength, uint crc, CancellationToken cancellationToken)
    {
        var recordBytes = encodeBuffer.AsMemory(0, recordLength);
        crc = Crc32C.Append(crc, recordBytes.Span);
        await destination.WriteAsync(recordBytes, cancellationToken).ConfigureAwait(false);
        return crc;
    }
}
