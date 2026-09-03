using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures replica-group snapshot creation, installation, and journal compaction.</summary>
[SuppressMessage("ReSharper", "UnusedMember.Global", Justification = "BenchmarkDotNet discovers benchmark methods by reflection.")]
[SuppressMessage("Design", "CA1001", Justification = "BenchmarkDotNet lifecycle manages disposable fields via global cleanup.")]
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5, invocationCount: 1)]
public class ReplicaSnapshotBenchmarks
{
    private const string GroupId = "grp-snapshot-bench";
    private const ulong SnapshotIndex = 256UL;

    private TempDirectory? _sourceDirectory;
    private TempDirectory? _targetDirectory;
    private FollowerLog? _source;
    private FollowerLog? _target;
    private GroupSnapshot _snapshot;

    /// <summary>Writes a committed replica snapshot.</summary>
    /// <returns>A task that completes after the snapshot is durably published.</returns>
    /// <exception cref="InvalidOperationException">Thrown when benchmark setup did not initialize the source log.</exception>
    [Benchmark]
    public async Task WriteReplicaSnapshotAsync()
    {
        var source = _source ?? throw new InvalidOperationException("Benchmark source log was not initialized.");
        _snapshot = await source.CreateSnapshotAsync(SnapshotIndex, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Validates the published replica snapshot.</summary>
    /// <returns>A task that completes after validation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup is incomplete or the snapshot is invalid.</exception>
    [Benchmark]
    public async Task ValidateReplicaSnapshotAsync()
    {
        var sourceDirectory = _sourceDirectory ?? throw new InvalidOperationException("Benchmark source directory was not initialized.");
        var store = new GroupSnapshotStore(sourceDirectory.Path, GroupId);
        _ = await store.ReadPublishedAsync(CancellationToken.None).ConfigureAwait(false) ?? throw new InvalidOperationException("Published snapshot was not found.");
    }

    /// <summary>Installs the prepared snapshot into a replica log.</summary>
    /// <returns>A task that completes after installation is durable.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup is incomplete or installation is refused.</exception>
    [Benchmark]
    public async Task InstallReplicaSnapshotAsync()
    {
        var target = _target ?? throw new InvalidOperationException("Benchmark target log was not initialized.");
        var result = await target.InstallSnapshotAsync(_snapshot, CancellationToken.None).ConfigureAwait(false);
        if (!result.Success)
            throw new InvalidOperationException($"Snapshot install was refused: {result.Refusal}.");
    }

    /// <summary>Restores the snapshot's committed idempotency outcomes into a replica log.</summary>
    /// <exception cref="InvalidOperationException">Thrown when benchmark setup did not initialize the target log.</exception>
    [Benchmark]
    public void RestoreIdempotencyRecords()
    {
        var target = _target ?? throw new InvalidOperationException("Benchmark target log was not initialized.");
        target.Idempotency.RestoreFromSnapshot(_snapshot.CommittedOutcomes);
    }

    /// <summary>Compacts the source journal while retaining the published snapshot.</summary>
    /// <returns>A task that completes after compaction is durable.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup is incomplete or compaction is refused.</exception>
    [Benchmark]
    public async Task CompactAsync()
    {
        var source = _source ?? throw new InvalidOperationException("Benchmark source log was not initialized.");
        var result = await source.CompactAsync(CancellationToken.None).ConfigureAwait(false);
        if (!result.Success)
            throw new InvalidOperationException("Snapshot compaction was not performed.");
    }

    /// <summary>Creates the source and target logs with a committed prefix.</summary>
    /// <returns>A task that completes after setup.</returns>
    /// <exception cref="IOException">Thrown when the temporary benchmark storage cannot be initialized.</exception>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _sourceDirectory = new TempDirectory("squirix-replica-snapshot-bench-source");
        _targetDirectory = new TempDirectory("squirix-replica-snapshot-bench-target");
        var composition = GroupComposition.Create(GroupId);
        _source = new FollowerLog(_sourceDirectory.Path, GroupId, composition);
        _target = new FollowerLog(_targetDirectory.Path, GroupId, composition);
        await _source.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        await _target.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        await SeedSourceLogAsync(_source, true).ConfigureAwait(false);
    }

    /// <summary>Rebuilds the source log before each compaction iteration so every run compacts a fully populated journal.</summary>
    [IterationSetup(Target = nameof(CompactAsync))]
    [SuppressMessage("Usage", "VSTHRD002", Justification = "BenchmarkDotNet requires IterationSetup to be synchronous; the awaited work runs without a synchronization context, so blocking is safe.")]
    public void CompactIterationSetup() => RebuildSourceLogAsync().GetAwaiter().GetResult();

    /// <summary>Rebuilds the source log before each install iteration so every run installs into a fresh replica.</summary>
    [IterationSetup(Target = nameof(InstallReplicaSnapshotAsync))]
    [SuppressMessage("Usage", "VSTHRD002", Justification = "BenchmarkDotNet requires IterationSetup to be synchronous; the awaited work runs without a synchronization context, so blocking is safe.")]
    public void InstallIterationSetup() => RebuildTargetLogAsync().GetAwaiter().GetResult();

    /// <summary>Rebuilds the target log before each restore iteration so every run restores into a fresh replica with an empty idempotency map.</summary>
    [IterationSetup(Target = nameof(RestoreIdempotencyRecords))]
    [SuppressMessage("Usage", "VSTHRD002", Justification = "BenchmarkDotNet requires IterationSetup to be synchronous; the awaited work runs without a synchronization context, so blocking is safe.")]
    public void RestoreIdempotencyIterationSetup() => RebuildTargetLogAsync().GetAwaiter().GetResult();

    /// <summary>Rebuilds the source log before each write iteration without a published snapshot, so every run measures the first publish path.</summary>
    [IterationSetup(Target = nameof(WriteReplicaSnapshotAsync))]
    [SuppressMessage("Usage", "VSTHRD002", Justification = "BenchmarkDotNet requires IterationSetup to be synchronous; the awaited work runs without a synchronization context, so blocking is safe.")]
    public void WriteIterationSetup() => RebuildSourceLogAsync(false).GetAwaiter().GetResult();

    /// <summary>Disposes benchmark logs and temporary directories.</summary>
    /// <returns>A task that completes after cleanup.</returns>
    /// <exception cref="IOException">Thrown when benchmark storage cleanup fails.</exception>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_source != null)
            await _source.DisposeAsync().ConfigureAwait(false);
        if (_target != null)
            await _target.DisposeAsync().ConfigureAwait(false);
        _sourceDirectory?.Dispose();
        _targetDirectory?.Dispose();
    }

    private async Task RebuildSourceLogAsync(bool publishSnapshot = true)
    {
        if (_source != null)
            await _source.DisposeAsync().ConfigureAwait(false);

        _sourceDirectory?.Dispose();
        _sourceDirectory = new TempDirectory("squirix-replica-snapshot-bench-source");

        _source = new FollowerLog(_sourceDirectory.Path, GroupId, GroupComposition.Create(GroupId));
        await _source.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        await SeedSourceLogAsync(_source, publishSnapshot).ConfigureAwait(false);
    }

    private async Task RebuildTargetLogAsync()
    {
        if (_target != null)
            await _target.DisposeAsync().ConfigureAwait(false);

        _targetDirectory?.Dispose();
        _targetDirectory = new TempDirectory("squirix-replica-snapshot-bench-target");

        _target = new FollowerLog(_targetDirectory.Path, GroupId, GroupComposition.Create(GroupId));
        await _target.OpenAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Seeds a freshly opened source log with a committed prefix, one resolved idempotency outcome per journal index, and optionally a published snapshot.</summary>
    /// <param name="source">The opened source log to populate.</param>
    /// <param name="publishSnapshot">When <see langword="false" />, no snapshot is published and <see cref="_snapshot" /> keeps its previous value.</param>
    /// <returns>A task that completes after the seed is durable.</returns>
    /// <exception cref="InvalidOperationException">Thrown when an append or commit is refused during seeding.</exception>
    private async Task SeedSourceLogAsync(FollowerLog source, bool publishSnapshot)
    {
        var payload = Encoding.UTF8.GetBytes("snapshot-benchmark-payload");
        for (var index = 1UL; index <= SnapshotIndex; index++)
        {
            var entry = new FollowerLogEntry(index, 1UL, payload);
            var request = new FollowerLogAppendRequest("leader", 1UL, index - 1UL, 1UL, 0UL, new ReadOnlyMemory<FollowerLogEntry>([entry]));
            var appendResult = await source.AppendAsync(request, CancellationToken.None).ConfigureAwait(false);
            if (!appendResult.Success)
                throw new InvalidOperationException($"Benchmark append failed at index {index}: refusal={appendResult.RefusalCode}.");
        }

        var commitResult = await source.AdvanceCommitAsync(SnapshotIndex, CancellationToken.None).ConfigureAwait(false);
        if (!commitResult.Success)
            throw new InvalidOperationException($"Benchmark commit failed: refusal={commitResult.RefusalCode}.");

        // CompactAsync refuses a snapshot whose included boundary differs from the applied watermark, so the seeded
        // log must be fully applied before the snapshot is created and measured.
        var appliedResult = await source.AdvanceAppliedAsync(SnapshotIndex, CancellationToken.None).ConfigureAwait(false);
        if (!appliedResult.Success)
            throw new InvalidOperationException($"Benchmark applied advance failed: refusal={appliedResult.RefusalCode}.");

        for (var index = 1UL; index <= SnapshotIndex; index++)
        {
            var operationId = $"operation-{index}";
            var reserveResult = source.Idempotency.Reserve("benchmark", operationId, new byte[] { 1 }, GroupRecordKind.UserMutation, index, 1UL);
            if (reserveResult != GroupIdempotencyReserveResult.Success)
                throw new InvalidOperationException($"Benchmark idempotency reserve failed at index {index}: {reserveResult}.");
            if (!source.Idempotency.TryResolve("benchmark", operationId, new byte[] { 2 }, index, 1UL))
                throw new InvalidOperationException($"Benchmark idempotency resolve failed at index {index}.");
        }

        if (publishSnapshot)
            _snapshot = await source.CreateSnapshotAsync(SnapshotIndex, CancellationToken.None).ConfigureAwait(false);
    }
}
