using System;
using System.Collections.Generic;
using System.Globalization;
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
    private byte[] _encodeBuffer = new byte[4096];

    internal SnapshotWriter(string dataDir)
        : this(dataDir, new FileOperations())
    {
    }

    internal SnapshotWriter(string dataDir, IStorageFileOperations fileOperations)
    {
        _dataDir = dataDir;
        _fileOperations = fileOperations;
    }

    public async Task<string> WriteAsync(
        int index,
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        var tmp = PathEx.Combine(_dataDir, $"{FilePrefixes.Snapshot}{index.ToString(FilePrefixes.SegmentIndexFormat, CultureInfo.InvariantCulture)}.tmp");
        try
        {
            await WriteSnapshotTempFileAsync(tmp, items, idempotencyRecords, cancellationToken).ConfigureAwait(false);
            var snap = PathEx.Combine(_dataDir, $"{FilePrefixes.Snapshot}{index.ToString(FilePrefixes.SegmentIndexFormat, CultureInfo.InvariantCulture)}{FileExtensions.Snapshot}");
            return _fileOperations.PublishSnapshot(tmp, snap) ? snap : throw new IOException($"Failed to publish snapshot to '{snap}'.");
        }
        finally
        {
            _ = FileEx.TryDeleteFile(tmp);
        }
    }

    private void EnsureEncodeBufferCapacity(int maxRecordLength)
    {
        if (_encodeBuffer.Length >= maxRecordLength)
            return;

        _encodeBuffer = new byte[Math.Max(maxRecordLength, _encodeBuffer.Length * 2)];
    }

    private async Task WriteSnapshotTempFileAsync(
        string tmp,
        IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        IReadOnlyList<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        var (totalFileSize, maxRecordLength) = SnapshotFileEncoder.ComputeWriteMetrics(items, idempotencyRecords);
        EnsureEncodeBufferCapacity(maxRecordLength);

        var fs = new FileStream(tmp, FileMode.Create, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 64 * 1024, SnapshotDurability.GetTempFileOptions());
        await using (fs.ConfigureAwait(false))
        {
            await SnapshotFileEncoder.WriteFileAsync(fs, items, idempotencyRecords, _encodeBuffer, totalFileSize, cancellationToken).ConfigureAwait(false);
            SnapshotDurability.FlushIfNeeded(fs.SafeFileHandle);
        }
    }
}
