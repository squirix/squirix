using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

internal sealed class SnapshotWriter : ISnapshotWriter
{
    private readonly string _dataDir;
    private readonly IStorageFileOperations _fileOperations;
    private byte[] _encodeBuffer = new byte[4096];

    public SnapshotWriter(string dataDir)
        : this(dataDir, new StorageFileOperations())
    {
    }

    internal SnapshotWriter(string dataDir, IStorageFileOperations fileOperations)
    {
        _dataDir = dataDir;
        _fileOperations = fileOperations;
    }

    public async Task<string> WriteAsync(
        int index,
        IEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> items,
        IEnumerable<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        var tmp = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Snapshot}{index.ToString("000000", CultureInfo.InvariantCulture)}.tmp");
        try
        {
            await WriteSnapshotTempFileAsync(tmp, items, idempotencyRecords, cancellationToken).ConfigureAwait(false);
            var snap = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Snapshot}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Snapshot}");
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
        IEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> items,
        IEnumerable<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        var itemList = Materialization.Items(items);
        var idempotencyList = Materialization.IdempotencyRecords(idempotencyRecords);
        var (totalFileSize, maxRecordLength) = SnapshotFileEncoder.ComputeWriteMetrics(itemList, idempotencyList);
        EnsureEncodeBufferCapacity(maxRecordLength);

        var fs = new FileStream(tmp, FileMode.Create, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 64 * 1024, SnapshotDurability.GetTempFileOptions());
        await using (fs.ConfigureAwait(false))
        {
            await SnapshotFileEncoder.WriteFileAsync(fs, itemList, idempotencyList, _encodeBuffer, totalFileSize, cancellationToken).ConfigureAwait(false);
            SnapshotDurability.FlushIfNeeded(fs.SafeFileHandle);
        }
    }

    private static class Materialization
    {
        public static List<PersistedIdempotencyRecord> IdempotencyRecords(IEnumerable<PersistedIdempotencyRecord> records)
        {
            if (records is List<PersistedIdempotencyRecord> list)
                return list;

            if (records is IReadOnlyList<PersistedIdempotencyRecord> readOnlyList)
            {
                var materialized = new List<PersistedIdempotencyRecord>(readOnlyList.Count);
                for (var i = 0; i < readOnlyList.Count; i++)
                    materialized.Add(readOnlyList[i]);

                return materialized;
            }

            var result = new List<PersistedIdempotencyRecord>();
            foreach (var record in records)
                result.Add(record);

            return result;
        }

        public static List<(CacheKey Key, CacheEntry<object?> Entry)> Items(IEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> items)
        {
            if (items is List<(CacheKey Key, CacheEntry<object?> Entry)> list)
                return list;

            if (items is IReadOnlyList<(CacheKey Key, CacheEntry<object?> Entry)> readOnlyList)
            {
                var materialized = new List<(CacheKey Key, CacheEntry<object?> Entry)>(readOnlyList.Count);
                for (var i = 0; i < readOnlyList.Count; i++)
                    materialized.Add(readOnlyList[i]);

                return materialized;
            }

            var result = new List<(CacheKey Key, CacheEntry<object?> Entry)>();
            foreach (var item in items)
                result.Add(item);

            return result;
        }
    }
}
