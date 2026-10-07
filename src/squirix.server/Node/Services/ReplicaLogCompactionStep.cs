using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The checks and the durable sequence of one compaction of a replica group log this node serves.</summary>
/// <remarks>
/// The committer runs <see cref="RunAsync" /> under its commit gate, so no write appends, commits, or applies while the step decides and
/// compacts: the commit index it reads stays the last log index, and the applied index it persists stays the commit index. A follower
/// group runs <see cref="RunFollowerAsync" /> instead, which compacts through the durable applied index and keeps every entry above it.
/// </remarks>
internal static class ReplicaLogCompactionStep
{
    /// <summary>The longest wait for a ready follower whose acknowledgement of the last committed entries is still in flight.</summary>
    /// <remarks>A commit returns after a majority; the remaining followers acknowledge right after it, while the gate is already free.</remarks>
    private static readonly TimeSpan FollowerWait = TimeSpan.FromMilliseconds(250);

    private static readonly TimeSpan FollowerPoll = TimeSpan.FromMilliseconds(5);

    /// <summary>Waits, bounded, until every follower slot is ready and has durably acknowledged <paramref name="commit" />.</summary>
    /// <param name="coordinator">The running commit coordinator recording the follower acknowledgements.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="commit">The commit index every follower must hold.</param>
    /// <param name="clock">Time source bounding the wait.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="null" /> when every follower holds <paramref name="commit" />; otherwise why the step may not go on.</returns>
    internal static async Task<ReplicaLogCompactionOutcome?> AwaitFollowersAsync(
        ReplicaCommitCoordinator coordinator,
        ReplicaEligibility eligibility,
        ulong commit,
        TimeProvider clock,
        CancellationToken cancellationToken)
    {
        var refusal = FollowerRefusal(coordinator, eligibility, commit);

        // Only an acknowledgement in flight is worth waiting for: a slot that is not ready is verified by the readiness service.
        for (var waited = TimeSpan.Zero; refusal == ReplicaLogCompactionOutcome.FollowerBehind && waited < FollowerWait; waited += FollowerPoll)
        {
            await Task.Delay(FollowerPoll, clock, cancellationToken).ConfigureAwait(false);
            refusal = FollowerRefusal(coordinator, eligibility, commit);
        }

        return refusal;
    }

    /// <summary>Compacts the owned log through its commit index when nothing may still need the covered entries.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="coordinator">The running commit coordinator.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="appliedIndex">The committer's in-memory applied index.</param>
    /// <param name="durability">The node cache journal the applies wrote to.</param>
    /// <param name="clock">Time source bounding the wait for followers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The step outcome; only <see cref="ReplicaLogCompactionOutcome.Compacted" /> changes the log.</returns>
    /// <remarks>
    /// The covered entries must be committed and applied, their idempotency outcomes resolved, and durably held by every follower,
    /// because the leader has no snapshot catch-up for a follower that would need them. The cache journal barrier then makes every
    /// applied entry durable before the log persists the applied index and drops the entries.
    /// </remarks>
    internal static async Task<ReplicaLogCompactionOutcome> RunAsync(
        IFollowerLog log,
        ReplicaCommitCoordinator coordinator,
        ReplicaEligibility eligibility,
        ulong appliedIndex,
        IJournalDurabilityCoordinator durability,
        TimeProvider clock,
        CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var commit = status.CommitIndex;
        var refusal = (status.Readiness == FollowerLogReadiness.Ready, status.LastLogIndex == commit, !coordinator.HasPendingApply && appliedIndex == commit) switch
        {
            (false, _, _) => ReplicaLogCompactionOutcome.NotReady,
            (true, false, _) => ReplicaLogCompactionOutcome.UncommittedTail,
            (true, true, false) => ReplicaLogCompactionOutcome.PendingApply,
            (true, true, true) => await AwaitFollowersAsync(coordinator, eligibility, commit, clock, cancellationToken).ConfigureAwait(false),
        };
        if (refusal is { } refused)
            return refused;

        await durability.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        var applied = await log.AdvanceAppliedAsync(commit, cancellationToken).ConfigureAwait(false);
        return applied.Success ? Map(await log.CompactThroughAsync(commit, cancellationToken).ConfigureAwait(false)) : ReplicaLogCompactionOutcome.NotReady;
    }

    /// <summary>Compacts a follower group log through its durable applied index once the log reaches a threshold.</summary>
    /// <param name="log">The follower group log.</param>
    /// <param name="policy">The compaction thresholds.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The step outcome; only <see cref="ReplicaLogCompactionOutcome.Compacted" /> changes the log.</returns>
    /// <remarks>
    /// The group's flush persists the applied index only after the cache journal holds the applied entries durably, and the commit index
    /// may run ahead of it: every entry above the applied index, committed or not, stays in the log for the group's applier.
    /// The step waits as <see cref="ReplicaLogCompactionOutcome.PendingApply" /> while the group's applier has not rebuilt the idempotency
    /// outcomes of the applied entries, which the snapshot would otherwise lose with their frames, and while nothing was applied past the
    /// last snapshot.
    /// </remarks>
    internal static async Task<ReplicaLogCompactionOutcome> RunFollowerAsync(IFollowerLog log, ReplicaLogCompactionPolicy policy, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        var retention = await log.GetRetentionAsync(cancellationToken).ConfigureAwait(false);
        if (!policy.IsReachedBy(in retention))
            return ReplicaLogCompactionOutcome.BelowThreshold;

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        return (status.Readiness == FollowerLogReadiness.Ready, log.Idempotency.OutcomesRebuilt && status.LastAppliedIndex > retention.SnapshotIndex) switch
        {
            (false, _) => ReplicaLogCompactionOutcome.NotReady,
            (true, false) => ReplicaLogCompactionOutcome.PendingApply,
            (true, true) => Map(await log.CompactThroughAsync(status.LastAppliedIndex, cancellationToken).ConfigureAwait(false)),
        };
    }

    /// <summary>Maps the storage compaction outcome to the step outcome.</summary>
    /// <param name="outcome">The storage compaction outcome.</param>
    /// <returns>The step outcome.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The outcome is not a named value.</exception>
    private static ReplicaLogCompactionOutcome Map(GroupCompactionOutcome outcome) => outcome switch
    {
        GroupCompactionOutcome.Compacted => ReplicaLogCompactionOutcome.Compacted,
        GroupCompactionOutcome.NotReady => ReplicaLogCompactionOutcome.NotReady,
        GroupCompactionOutcome.UnresolvedOutcome => ReplicaLogCompactionOutcome.UnresolvedOutcome,
        GroupCompactionOutcome.SnapshotTooLarge => ReplicaLogCompactionOutcome.SnapshotTooLarge,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unsupported group compaction outcome."),
    };

    /// <summary>Returns why a follower slot cannot let the step go on, or <see langword="null" /> when it holds <paramref name="commit" />.</summary>
    /// <param name="coordinator">The running commit coordinator recording the follower acknowledgements.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="commit">The commit index every follower must hold.</param>
    /// <returns>The first refusal among the follower slots, or <see langword="null" />.</returns>
    private static ReplicaLogCompactionOutcome? FollowerRefusal(ReplicaCommitCoordinator coordinator, ReplicaEligibility eligibility, ulong commit)
    {
        for (var i = 1; i < eligibility.ReplicaCount; i++)
        {
            if (!eligibility.CanCountInWriteQuorum(i))
                return ReplicaLogCompactionOutcome.FollowerNotReady;

            if (coordinator.MatchIndexFor(i) < commit)
                return ReplicaLogCompactionOutcome.FollowerBehind;
        }

        return null;
    }
}
