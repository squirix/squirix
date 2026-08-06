using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.Benchmarks;

/// <summary>Durable ordered append and committed-prefix recovery benchmarks for the replica-group follower log.</summary>
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "BenchmarkDotNet [Params] properties require public setters.")]
[SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable", Justification = "BenchmarkDotNet lifecycle manages disposal via GlobalCleanup.")]
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class FollowerAppendBenchmarks
{
    private const int OperationsPerInvoke = 1_000;
    private const string GroupId = "grp-bench";

    private byte[] _payload = [];
    private TempDirectory? _dir;
    private FollowerLog? _log;
    private ulong _nextIndex;

    /// <summary>Gets or sets the payload size in bytes.</summary>
    [Params(256, 4096)]
    public int PayloadBytes { get; set; }

    /// <summary>Appends a single ordered entry and awaits durability.</summary>
    /// <returns>A task that completes when the appending finishes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the log was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task AppendSingleOrderedEntryAsync()
    {
        var log = _log ?? throw new InvalidOperationException("Benchmark log was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await AppendEntryAsync(log).ConfigureAwait(false);
    }

    /// <summary>Appends a batch of ordered entries and awaits durability.</summary>
    /// <returns>A task that completes when the batch finishes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the log was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task AppendBatchOrderedEntriesAsync()
    {
        var log = _log ?? throw new InvalidOperationException("Benchmark log was not initialized.");
        const int batchSize = 32;
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            var entries = new FollowerLogEntry[batchSize];
            for (var j = 0; j < batchSize; j++)
                entries[j] = new FollowerLogEntry(++_nextIndex, 1UL, _payload);

            var request = new FollowerLogAppendRequest("leader-1", 1UL, _nextIndex - batchSize, 1UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>(entries));
            _ = await log.AppendAsync(request, CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Acknowledges an identical duplicate entry idempotently.</summary>
    /// <returns>A task that completes when the duplicate appending finishes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the log was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task AcknowledgeIdenticalDuplicateAsync()
    {
        var log = _log ?? throw new InvalidOperationException("Benchmark log was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            var entry = new FollowerLogEntry(1UL, 1UL, _payload);
            var request = new FollowerLogAppendRequest("leader-1", 1UL, 0UL, 1UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>([entry]));
            _ = await log.AppendAsync(request, CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Recovers the committed prefix by reopening the log from disk.</summary>
    /// <returns>A task that completes when recovery finishes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the log or directory was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task RecoverCommittedPrefixAsync()
    {
        var dir = _dir ?? throw new InvalidOperationException("Benchmark directory was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            if (_log is not null)
                await _log.DisposeAsync().ConfigureAwait(false);

            _log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
            await _log.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Disposes the follower log and temp directory.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_log is not null)
            await _log.DisposeAsync().ConfigureAwait(false);
        _log = null;
        _dir?.Dispose();
        _dir = null;
    }

    /// <summary>Creates the follower log, payload, and a committed prefix for recovery.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dir = new TempDirectory("squirix-follower-append-bench");
        _payload = new byte[PayloadBytes];
        Array.Fill(_payload, Convert.ToByte('x'));
        _nextIndex = 0;

        _log = new FollowerLog(_dir, GroupId, GroupComposition.Create(GroupId));
        await _log.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        for (var i = 0; i < 1_000; i++)
            _ = await AppendEntryAsync(_log).ConfigureAwait(false);
        _ = await _log.AdvanceCommitAsync(_nextIndex, CancellationToken.None).ConfigureAwait(false);
    }

    private Task<FollowerLogAppendResult> AppendEntryAsync(FollowerLog log)
    {
        var entry = new FollowerLogEntry(++_nextIndex, 1UL, _payload);
        var request = new FollowerLogAppendRequest("leader-1", 1UL, _nextIndex - 1, 1UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>([entry]));
        return log.AppendAsync(request, CancellationToken.None);
    }
}
