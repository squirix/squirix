using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures replica-group compaction (snapshot publish plus journal prefix drop), snapshot installation, and snapshot validation.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5, invocationCount: 1)]
public class ReplicaSnapshotBenchmarks
{
    private const string GroupId = "grp-snapshot-bench";
    private const ulong SnapshotIndex = 256UL;

    private TempDirectory? _dir;
    private TempDirectory? _dir2;
    private GroupSnapshot _snapshot;
    private FollowerLog? _source;
    private FollowerLog? _target;

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
        _dir?.Dispose();
        _dir2?.Dispose();
    }

    /// <summary>
    /// Compacts the committed, applied prefix the way the node's compaction step does: the snapshot is built and durably
    /// published, then the journal prefix it covers is dropped.
    /// </summary>
    /// <returns>A task that completes after compaction is durable.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup is incomplete or compaction is refused.</exception>
    [Benchmark]
    public async Task CompactThroughAsync()
    {
        var source = ThrowHelper.Required(_source, "Benchmark source log was not initialized.");
        var outcome = await source.CompactThroughAsync(SnapshotIndex, CancellationToken.None).ConfigureAwait(false);
        if (outcome != GroupCompactionOutcome.Compacted)
            throw new InvalidOperationException($"Compaction was not performed: {outcome}.");
    }

    /// <summary>Rebuilds the source log before each compaction iteration so every run snapshots and compacts a fully populated journal.</summary>
    [IterationSetup(Target = nameof(CompactThroughAsync))]
    public void CompactIterationSetup() => RebuildSourceLogAsync().GetAwaiter().GetResult();

    /// <summary>Rebuilds the source log before each install iteration so every run installs into a fresh replica.</summary>
    [IterationSetup(Target = nameof(InstallReplicaSnapshotAsync))]
    public void InstallIterationSetup() => RebuildTargetLogAsync().GetAwaiter().GetResult();

    /// <summary>Installs the prepared snapshot into a replica log.</summary>
    /// <returns>A task that completes after installation is durable.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup is incomplete or installation is refused.</exception>
    [Benchmark]
    public async Task InstallReplicaSnapshotAsync()
    {
        var target = ThrowHelper.Required(_target, "Benchmark target log was not initialized.");
        var result = await target.InstallSnapshotAsync(_snapshot, 1UL, CancellationToken.None).ConfigureAwait(false);
        if (!result.Success)
            throw new InvalidOperationException($"Snapshot install was refused: {result.Refusal}.");
    }

    /// <summary>Rebuilds the target log before each restore iteration so every run restores into a fresh replica with an empty idempotency map.</summary>
    [IterationSetup(Target = nameof(RestoreIdempotencyRecords))]
    public void RestoreIdempotencyIterationSetup() => RebuildTargetLogAsync().GetAwaiter().GetResult();

    /// <summary>Restores the snapshot's committed idempotency outcomes into a replica log.</summary>
    /// <exception cref="InvalidOperationException">Thrown when benchmark setup did not initialize the target log.</exception>
    [Benchmark]
    public void RestoreIdempotencyRecords()
    {
        var target = ThrowHelper.Required(_target, "Benchmark target log was not initialized.");
        target.Idempotency.RestoreFromSnapshot(_snapshot.CommittedOutcomes, _snapshot.CapturedUtc, []);
    }

    /// <summary>Creates the source and target logs with a committed prefix.</summary>
    /// <returns>A task that completes after setup.</returns>
    /// <exception cref="IOException">Thrown when the temporary benchmark storage cannot be initialized.</exception>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dir = new TempDirectory("squirix-replica-snapshot-bench-source");
        _dir2 = new TempDirectory("squirix-replica-snapshot-bench-target");
        var composition = GroupComposition.Create(GroupId);
        _source = new FollowerLog(_dir, GroupId, composition, NullLogger<FollowerLog>.Instance);
        _target = new FollowerLog(_dir2, GroupId, composition, NullLogger<FollowerLog>.Instance);
        await _source.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        await _target.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        await SeedSourceLogAsync(_source, true).ConfigureAwait(false);
    }

    /// <summary>Validates the published replica snapshot.</summary>
    /// <returns>A task that completes after validation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup is incomplete or the snapshot is invalid.</exception>
    [Benchmark]
    public async Task ValidateReplicaSnapshotAsync()
    {
        var dir = ThrowHelper.Required(_dir, "Benchmark source directory was not initialized.");
        var store = new GroupSnapshotStore(dir, GroupId);
        if (await store.ReadPublishedAsync(CancellationToken.None).ConfigureAwait(false) == null)
            throw new InvalidOperationException("Published snapshot was not found.");
    }

    private async Task RebuildSourceLogAsync()
    {
        if (_source != null)
            await _source.DisposeAsync().ConfigureAwait(false);

        _dir?.Dispose();
        _dir = new TempDirectory("squirix-replica-snapshot-bench-source");

        _source = new FollowerLog(_dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await _source.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        await SeedSourceLogAsync(_source, false).ConfigureAwait(false);
    }

    private async Task RebuildTargetLogAsync()
    {
        if (_target != null)
            await _target.DisposeAsync().ConfigureAwait(false);

        _dir2?.Dispose();
        _dir2 = new TempDirectory("squirix-replica-snapshot-bench-target");

        _target = new FollowerLog(_dir2, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await _target.OpenAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Seeds a freshly opened source log with a committed prefix and one resolved idempotency outcome per journal index, and
    /// optionally compacts it to publish the snapshot the install and validate benchmarks use.
    /// </summary>
    /// <param name="source">The opened source log to populate.</param>
    /// <param name="publishSnapshot">When <see langword="false" />, nothing is compacted and <see cref="_snapshot" /> keeps its previous value.</param>
    /// <returns>A task that completes after the seed is durable.</returns>
    /// <exception cref="InvalidOperationException">Thrown when an append, commit, or compaction is refused during seeding.</exception>
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

        // Compaction only covers a prefix that is both committed and applied, so the seeded log is fully applied.
        var appliedResult = await source.AdvanceAppliedAsync(SnapshotIndex, CancellationToken.None).ConfigureAwait(false);
        if (!appliedResult.Success)
            throw new InvalidOperationException($"Benchmark applied advance failed: refusal={appliedResult.RefusalCode}.");

        for (var index = 1UL; index <= SnapshotIndex; index++)
        {
            var operationId = $"operation-{index}";
            var reserveResult = source.Idempotency.Reserve("benchmark", operationId, [1], GroupRecordKind.UserMutation, index, 1UL);
            if (reserveResult != GroupIdempotencyReserveResult.Success)
                throw new InvalidOperationException($"Benchmark idempotency reserve failed at index {index}: {reserveResult}.");
            if (!source.Idempotency.TryResolve("benchmark", operationId, [2], index, 1UL))
                throw new InvalidOperationException($"Benchmark idempotency resolve failed at index {index}.");
        }

        if (!publishSnapshot)
            return;

        var outcome = await source.CompactThroughAsync(SnapshotIndex, CancellationToken.None).ConfigureAwait(false);
        if (outcome != GroupCompactionOutcome.Compacted)
            throw new InvalidOperationException($"Benchmark compaction failed: {outcome}.");

        var dir = ThrowHelper.Required(_dir, "Benchmark source directory was not initialized.");
        var published = await new GroupSnapshotStore(dir, GroupId).ReadPublishedAsync(CancellationToken.None).ConfigureAwait(false);
        if (published is not { } snapshot)
            throw new InvalidOperationException("Benchmark compaction published no snapshot.");

        _snapshot = snapshot;
    }
}
