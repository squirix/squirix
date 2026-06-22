using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Threading;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Hosts warmed binary snapshot items for write-path breakdown benchmarks.</summary>
[SuppressMessage("AsyncUsage", "MA0045:Use await instead of GetResult()", Justification = "Benchmark breakdown APIs run synchronously without a synchronization context.")]
[SuppressMessage("Usage", "VSTHRD002:Avoid problematic synchronous waits", Justification = "Benchmark breakdown APIs run synchronously without a synchronization context.")]
internal sealed class SnapshotWriteBreakdownSession : IDisposable
{
    private readonly byte[] _encodeBuffer;
    private readonly TempDirectory _dataDir;
    private readonly List<(CacheKey Key, CacheEntry<object?> Entry)> _items;
    private readonly ManifestStore _manifestStore;
    private readonly SnapshotWriter _writer;
    private int _nextFileIndex = 10_000;
    private int _nextSnapshotIndex = 1;

    private SnapshotWriteBreakdownSession(
        TempDirectory dataDir,
        List<(CacheKey Key, CacheEntry<object?> Entry)> items,
        byte[] encodeBuffer,
        SnapshotWriter writer,
        ManifestStore manifestStore)
    {
        _dataDir = dataDir;
        _items = items;
        _encodeBuffer = encodeBuffer;
        _writer = writer;
        _manifestStore = manifestStore;
    }

    /// <summary>Creates a warmed binary snapshot breakdown session.</summary>
    /// <param name="entryCount">Number of synthetic entries.</param>
    /// <returns>A session ready for breakdown benchmarks.</returns>
    public static SnapshotWriteBreakdownSession Create(int entryCount)
    {
        var dataDir = new TempDirectory("snapshot-breakdown");
        var items = new List<(CacheKey Key, CacheEntry<object?> Entry)>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            object? value = (i % 3) switch
            {
                0 => $"value-{i}",
                1 => i + 0L,
                _ => i * 1.5d,
            };
            items.Add((CacheKey.Default($"key-{i}"), new CacheEntry<object?> { Value = value, Version = 1 }));
        }

        var (_, maxRecordLength) = SnapshotFileEncoder.ComputeWriteMetrics(items, []);
        var writer = new SnapshotWriter(dataDir);
        var retention = ManifestBenchmarkSupport.ResolveRetentionCount();
        var manifestStore = new ManifestStore(
            new PersistenceOptions
            {
                DataDir = dataDir.Path,
                ManifestRetentionCount = retention,
                SnapshotRetentionCount = retention,
            });
        manifestStore.PublishRollBlocking(1, 1);
        return new SnapshotWriteBreakdownSession(dataDir, items, new byte[maxRecordLength], writer, manifestStore);
    }

    /// <summary>Encodes all entry records into the reusable buffer (no I/O).</summary>
    /// <returns>Total encoded record bytes.</returns>
    public int EncodeAllEntries()
    {
        var total = 0;
        foreach (var (key, entry) in _items)
            total += SnapshotFileEncoder.WriteEntryRecord(_encodeBuffer, key, entry);

        return total;
    }

    /// <summary>Writes a complete binary snapshot temp file and flushes it to disk.</summary>
    public void WriteTempFileOnly()
    {
        var path = BuildTempPath(_nextFileIndex++);
        using var fs = new FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 64 * 1024, SnapshotDurability.GetTempFileOptions());
        var (totalFileSize, _) = SnapshotFileEncoder.ComputeWriteMetrics(_items, []);
        WriteFileBlocking(fs, totalFileSize);
    }

    /// <summary>Runs the production binary snapshot publish path.</summary>
    public void PublishSnapshot() => _ = _writer.WriteAsync(1, _items, [], CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Writes a snapshot manifest update matching the coordinator publish slice (no snapshot file I/O).</summary>
    public void WriteManifestOnly()
    {
        var snapshotIndex = _nextSnapshotIndex++;
        var snapshotPath = BuildSnapshotPath(snapshotIndex);
        File.WriteAllBytes(snapshotPath, []);

        var previous = _manifestStore.ReadCurrentOrDefaultBlocking();
        var updated = new ManifestState
        {
            Format = previous.Format is 0 ? 1 : previous.Format,
            CurrentJournal = previous.CurrentJournal,
            NextSequence = previous.NextSequence + 1,
            LastSnapshot = new ManifestState.SnapshotRef
            {
                Index = snapshotIndex,
                Path = snapshotPath,
                CreatedUtc = DateTime.UtcNow,
                LastAppliedSequence = previous.NextSequence,
                ReplayFromJournalSegment = previous.CurrentJournal,
            },
        };

        _manifestStore.WriteAsync(updated, CancellationToken.None).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _manifestStore.Dispose();
        _dataDir.Dispose();
    }

    private void WriteFileBlocking(FileStream destination, long totalFileSize) =>
        SnapshotFileEncoder.WriteFileAsync(destination, _items, [], _encodeBuffer, totalFileSize, CancellationToken.None).GetAwaiter().GetResult();

    private string BuildTempPath(int index) => PathEx.Combine(_dataDir.Path, $"{StorageFilePrefixes.Snapshot}{index.ToString("000000", CultureInfo.InvariantCulture)}.tmp");

    private string BuildSnapshotPath(int index) => PathEx.Combine(
        _dataDir.Path,
        $"{StorageFilePrefixes.Snapshot}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.BinarySnapshot}");
}
