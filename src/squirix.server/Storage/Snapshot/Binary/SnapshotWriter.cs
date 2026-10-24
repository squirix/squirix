using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

[Immutable]
internal sealed class SnapshotWriter : ISnapshotWriter
{
    private readonly string _dataDir;
    private readonly IStorageFileOperations _fileOperations;

    internal SnapshotWriter(string dataDir)
        : this(dataDir, new FileOperations())
    {
    }

    internal SnapshotWriter(string dataDir, IStorageFileOperations fileOperations)
    {
        _dataDir = dataDir;
        _fileOperations = fileOperations;
    }

    public int AllocateIndex(int publishedIndex)
    {
        var highest = publishedIndex;
        foreach (var path in Directory.GetFiles(_dataDir, $"{FilePrefixes.Snapshot}*{FileExtensions.Snapshot}"))
            highest = Math.Max(highest, SnapshotFileName.ParseIndex(Path.GetFileName(path)));

        return highest + 1;
    }

    public async ValueTask<string> WriteAsync(
        int index,
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        var tmp = PathEx.Combine(_dataDir, $"{FilePrefixes.Snapshot}{InvariantDigitStrings.FormatD6(index)}{FileExtensions.Temp}");
        var (totalFileSize, maxRecordLength) = SnapshotFileEncoder.ComputeWriteMetrics(items, idempotencyRecords);
        var encodeBuffer = ArrayPool<byte>.Shared.Rent(maxRecordLength);
        try
        {
            await WriteSnapshotTempFileAsync(tmp, items, idempotencyRecords, encodeBuffer, totalFileSize, cancellationToken).ConfigureAwait(false);
            var snap = SnapshotFileName.BuildPath(_dataDir, index);
            return _fileOperations.PublishSnapshot(tmp, snap) ? snap : throw new IOException("Failed to publish snapshot.");
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(encodeBuffer);
            _ = FileEx.TryDeleteFile(tmp);
        }
    }

    private static async Task WriteSnapshotTempFileAsync(
        string tmp,
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        byte[] encodeBuffer,
        long totalFileSize,
        CancellationToken cancellationToken)
    {
        using var handle = FileEx.OpenPrivate(tmp, FileMode.Create, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, SnapshotDurability.GetTempFileOptions());
        await SnapshotFileEncoder.WriteFileAsync(handle, items, idempotencyRecords, encodeBuffer, totalFileSize, cancellationToken).ConfigureAwait(false);
        SnapshotDurability.Flush(handle);
    }
}
