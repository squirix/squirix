using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Isolates binary snapshot write costs: temp-file write and full publish.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class SnapshotWriteBreakdownBenchmarks
{
    private int _operationsPerInvoke;
    private Session? _session;

    /// <summary>Disposes the breakdown session and temporary data directory.</summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _session?.Dispose();
        _session = null;
    }

    /// <summary>Creates a warmed binary snapshot breakdown session.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _operationsPerInvoke = SnapshotBenchmarkSupport.ResolveOperationsPerInvoke(2);
        _session = await Session.CreateAsync(SnapshotBenchmarkSupport.ResolveEntryCount()).ConfigureAwait(false);
    }

    /// <summary>Manifest store update after snapshot (encode + durable manifest file + pointer; no snapshot file I/O).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public async Task ManifestWriteOnlyAsync()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            await session.WriteManifestOnlyAsync().ConfigureAwait(false);
    }

    /// <summary>Full binary snapshot publish path (tmp write + rename).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark(Baseline = true)]
    public async Task PublishSnapshotAsync()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            await session.PublishSnapshotAsync().ConfigureAwait(false);
    }

    /// <summary>Writes a complete temp snapshot file and flushes it to disk (no publish rename).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public async Task WriteTempFileOnlyAsync()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            await session.WriteTempFileOnlyAsync().ConfigureAwait(false);
    }

    /// <summary>Hosts warmed binary snapshot items for write-path breakdown benchmarks.</summary>
    private sealed class Session : IDisposable
    {
        private readonly TempDirectory _dataDir;
        private readonly byte[] _encodeBuffer;
        private readonly List<(CacheKey Key, NodeCacheEntry<object?> Entry)> _items;
        private readonly Ledger _manifestStore;
        private readonly SnapshotWriter _writer;
        private int _nextFileIndex = 10_000;
        private int _nextSnapshotIndex = 1;

        private Session(TempDirectory dataDir, List<(CacheKey Key, NodeCacheEntry<object?> Entry)> items, byte[] encodeBuffer, SnapshotWriter writer, Ledger manifestStore)
        {
            _dataDir = dataDir;
            _items = items;
            _encodeBuffer = encodeBuffer;
            _writer = writer;
            _manifestStore = manifestStore;
        }

        public void Dispose()
        {
            _manifestStore.Dispose();
            _dataDir.Dispose();
        }

        /// <summary>Creates a warmed binary snapshot breakdown session.</summary>
        /// <param name="entryCount">Number of synthetic entries.</param>
        /// <returns>A session ready for breakdown benchmarks.</returns>
        internal static async Task<Session> CreateAsync(int entryCount)
        {
            var dataDir = new TempDirectory("snapshot-breakdown");
            var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(entryCount);
            for (var i = 0; i < entryCount; i++)
            {
                object? value = (i % 3) switch
                {
                    0 => $"value-{NodeInvariantIndexStrings.Format(i)}",
                    1 => i,
                    _ => i * 1.5d,
                };
                items.Add((CacheKey.Default($"key-{NodeInvariantIndexStrings.Format(i)}"), new NodeCacheEntry<object?> { Value = value, Version = 1 }));
            }

            var (_, maxRecordLength) = SnapshotFileEncoder.ComputeWriteMetrics(items, []);
            var writer = new SnapshotWriter(dataDir);
            var retention = ManifestBenchmarkSupport.ResolveRetentionCount();
            var options = new PersistenceOptions
            {
                DataDir = dataDir.Path,
                ManifestRetentionCount = retention,
                SnapshotRetentionCount = retention,
            };
            var manifestStore = new Ledger(options);
            var warmup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            manifestStore.EnqueueRoll(1, 1, warmup.SetResult, warmup.SetException);
            await warmup.Task.ConfigureAwait(false);
            return new Session(dataDir, items, new byte[maxRecordLength], writer, manifestStore);
        }

        /// <summary>Runs the production binary snapshot publish path.</summary>
        internal async Task PublishSnapshotAsync() => _ = await _writer.WriteAsync(1, _items, [], CancellationToken.None).ConfigureAwait(false);

        /// <summary>Writes a snapshot manifest update matching the coordinator publish slice (no snapshot file I/O).</summary>
        internal async Task WriteManifestOnlyAsync()
        {
            var snapshotIndex = _nextSnapshotIndex++;
            var snapshotPath = BuildSnapshotPath(snapshotIndex);

            var previous = await _manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None).ConfigureAwait(false);
            var updated = new State
            {
                Format = previous.Format == 0 ? 1 : previous.Format,
                CurrentJournal = previous.CurrentJournal,
                NextSequence = previous.NextSequence + 1,
                LastSnapshot = new SnapshotRef
                {
                    Index = snapshotIndex,
                    Path = snapshotPath,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = previous.NextSequence,
                    ReplayFromJournalSegment = previous.CurrentJournal,
                },
            };

            await _manifestStore.WriteAsync(updated, CancellationToken.None).ConfigureAwait(false);
        }

        /// <summary>Writes a complete binary snapshot temp file and flushes it to disk.</summary>
        internal async Task WriteTempFileOnlyAsync()
        {
            var path = BuildTempPath(_nextFileIndex++);
            using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, SnapshotDurability.GetTempFileOptions());
            var (totalFileSize, _) = SnapshotFileEncoder.ComputeWriteMetrics(_items, []);
            await SnapshotFileEncoder.WriteFileAsync(handle, _items, [], _encodeBuffer, totalFileSize, CancellationToken.None).ConfigureAwait(false);
        }

        private string BuildSnapshotPath(int index) =>
            PathEx.Combine(_dataDir.Path, $"{FilePrefixes.Snapshot}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Snapshot}");

        private string BuildTempPath(int index) => PathEx.Combine(_dataDir.Path, $"{FilePrefixes.Snapshot}{NodeInvariantIndexStrings.FormatD6(index)}.tmp");
    }
}
