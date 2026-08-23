using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

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

    public async ValueTask<string> WriteAsync(
        int index,
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        var tmp = PathEx.Combine(_dataDir, $"{FilePrefixes.Snapshot}{InvariantDigitStrings.FormatD6(index)}.tmp");
        var (totalFileSize, maxRecordLength) = SnapshotFileEncoder.ComputeWriteMetrics(items, idempotencyRecords);
        var encodeBuffer = ArrayPool<byte>.Shared.Rent(maxRecordLength);
        try
        {
            await WriteSnapshotTempFileAsync(tmp, items, idempotencyRecords, encodeBuffer, totalFileSize, cancellationToken).ConfigureAwait(false);
            var snap = PathEx.Combine(_dataDir, $"{FilePrefixes.Snapshot}{InvariantDigitStrings.FormatD6(index)}{FileExtensions.Snapshot}");
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
        var fs = new FileStream(tmp, FileMode.Create, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 64 * 1024, SnapshotDurability.GetTempFileOptions());
        await using (fs.ConfigureAwait(false))
        {
            await SnapshotFileEncoder.WriteFileAsync(fs, items, idempotencyRecords, encodeBuffer, totalFileSize, cancellationToken).ConfigureAwait(false);
            SnapshotDurability.FlushIfNeeded(fs.SafeFileHandle);
        }
    }
}
