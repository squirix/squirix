using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Utils;
using static Squirix.Server.UnitTests.Persistence.Replication.StateInstallLeaders;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Publishes follower-log snapshots through the production compaction path, as the replica compaction step does.</summary>
internal static class FollowerSnapshotScenario
{
    /// <summary>Creates the one-shot fault that fails the first durable flush after it is armed.</summary>
    /// <returns>The fault hooks to pass to the log under test.</returns>
    internal static ArmableFlushFaultHooks CreateCompactionFaults() => new(static () => new IOException("simulated crash after the snapshot was published."));

    /// <summary>Marks the prefix applied, compacts it, and returns the snapshot the compaction published.</summary>
    /// <param name="log">The open log holding committed entries through <paramref name="index" />.</param>
    /// <param name="persistenceRoot">The node data directory the log was created under.</param>
    /// <param name="index">The committed index to compact through.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The published snapshot.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the compaction does not complete or publishes no snapshot.</exception>
    internal static async Task<GroupSnapshot> CompactThroughAsync(FollowerLog log, string persistenceRoot, ulong index, CancellationToken cancellationToken)
    {
        await AdvanceAppliedAsync(log, index, cancellationToken);
        var outcome = await log.CompactThroughAsync(index, cancellationToken);
        var published = outcome == GroupCompactionOutcome.Compacted ? await new GroupSnapshotStore(persistenceRoot, log.GroupId).ReadPublishedAsync(cancellationToken) : null;
        return published ?? ThrowHelper.Throw<GroupSnapshot>(new InvalidOperationException($"Compaction through index {index} ended with '{outcome}' and no published snapshot."));
    }

    /// <summary>Marks the prefix applied and compacts it with the compaction failing after the snapshot is published, so the journal is left uncompacted.</summary>
    /// <param name="log">The open log, created with <paramref name="faults" />, holding committed entries through <paramref name="index" />.</param>
    /// <param name="faults">The fault hooks the log was created with.</param>
    /// <param name="index">The committed index to compact through.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the injected failure surfaced.</returns>
    /// <remarks>The log fails readiness afterwards and must be disposed and reopened, like after a crash.</remarks>
    internal static async Task PublishWithoutCompactionAsync(FollowerLog log, ArmableFlushFaultHooks faults, ulong index, CancellationToken cancellationToken)
    {
        await AdvanceAppliedAsync(log, index, cancellationToken);
        faults.Arm();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactThroughAsync(index, cancellationToken));
    }

    /// <summary>Marks the copy incomplete for a state transfer at the index of a snapshot and completes the transfer with that snapshot.</summary>
    /// <param name="log">The open complete log to transfer into.</param>
    /// <param name="snapshot">The snapshot the transferred state is taken at.</param>
    /// <param name="leaderTerm">The term of the leader that sends the state.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The completion outcome, or the refusal of the begin step.</returns>
    internal static async Task<GroupSnapshotInstallResult> InstallAsync(FollowerLog log, GroupSnapshot snapshot, ulong leaderTerm, CancellationToken cancellationToken)
    {
        var refusal = await BeginAsync(log, snapshot, leaderTerm, cancellationToken);
        return refusal != null ? GroupSnapshotInstallResult.Refused(refusal) : await log.CompleteStateInstallAsync(snapshot, Leader(leaderTerm), cancellationToken);
    }

    /// <summary>Marks the copy incomplete for a state transfer of a snapshot, under the topology the log already holds.</summary>
    /// <param name="log">The open log.</param>
    /// <param name="snapshot">The snapshot the transfer will complete at.</param>
    /// <param name="leaderTerm">The term of the leader that sends the state.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="null" /> once the copy is incomplete; otherwise the refusal.</returns>
    internal static async Task<string?> BeginAsync(FollowerLog log, GroupSnapshot snapshot, ulong leaderTerm, CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken);
        return await log.BeginStateInstallAsync(Leader(leaderTerm), snapshot.LastIncludedIndex, status.TopologyFingerprint, status.ConfigurationGeneration, snapshot, cancellationToken);
    }

    /// <summary>Marks the copy incomplete for a state transfer under the topology the log already holds.</summary>
    /// <param name="log">The open log.</param>
    /// <param name="index">The applied index the state is transferred at.</param>
    /// <param name="leaderTerm">The term of the leader that sends the state.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="null" /> once the copy is incomplete; otherwise the refusal.</returns>
    internal static async Task<string?> BeginAsync(FollowerLog log, ulong index, ulong leaderTerm, CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken);
        var snapshot = new GroupSnapshot(log.GroupId, status.TopologyFingerprint, status.ConfigurationGeneration, 1UL, index, index, [], DateTime.UnixEpoch);
        return await BeginAsync(log, snapshot, leaderTerm, cancellationToken);
    }

    private static async Task AdvanceAppliedAsync(FollowerLog log, ulong index, CancellationToken cancellationToken)
    {
        var applied = await log.AdvanceAppliedAsync(index, cancellationToken);
        if (!applied.Success)
            throw new InvalidOperationException($"Marking index {index} applied was refused: '{applied.RefusalCode}'.");
    }
}
