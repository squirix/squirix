using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The checks and the durable sequence of one compaction of a replica group log this node serves.</summary>
/// <remarks>
/// The committer runs <see cref="RunAsync" /> under its commit gate to persist and capture the applied boundary, then prepares the file
/// outside the gate. It reenters the gate for <see cref="FinishAsync" /> and checks follower retention at the captured boundary. A follower
/// group runs <see cref="RunFollowerAsync" /> instead, which compacts through the durable applied index and keeps every entry above it.
/// </remarks>
internal static class ReplicaLogCompactionStep
{
    /// <summary>The slot mask that names no slot.</summary>
    internal const int NoSlots = 0;

    private static readonly TimeSpan FollowerPoll = TimeSpan.FromMilliseconds(5);

    /// <summary>The longest wait for a ready follower whose acknowledgement of the last committed entries is still in flight.</summary>
    /// <remarks>A commit returns after a majority; the remaining followers acknowledge right after it, while the gate is already free.</remarks>
    private static readonly TimeSpan FollowerWait = TimeSpan.FromMilliseconds(250);

    /// <summary>Waits, bounded, until every follower slot is ready and has durably acknowledged <paramref name="commit" />.</summary>
    /// <param name="coordinator">The running commit coordinator recording the follower acknowledgements.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="pastLimit">Whether the log reached its retention limit, past which a slot that is not ready no longer holds it.</param>
    /// <param name="commit">The commit index every follower must hold.</param>
    /// <param name="clock">Time source bounding the wait.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="null" /> when every follower that counts holds <paramref name="commit" />; otherwise why the step may not go on.</returns>
    internal static async Task<ReplicaLogCompactionOutcome?> AwaitFollowersAsync(
        ReplicaCommitCoordinator coordinator,
        ReplicaEligibility eligibility,
        bool pastLimit,
        ulong commit,
        TimeProvider clock,
        CancellationToken cancellationToken)
    {
        var refusal = FollowerRefusal(coordinator, eligibility, pastLimit, commit);

        // Only an acknowledgement in flight is worth waiting for: a slot that is not ready is verified by the readiness service.
        for (var waited = TimeSpan.Zero; refusal == ReplicaLogCompactionOutcome.FollowerBehind && waited < FollowerWait; waited += FollowerPoll)
        {
            await Task.Delay(FollowerPoll, clock, cancellationToken).ConfigureAwait(false);
            refusal = FollowerRefusal(coordinator, eligibility, pastLimit, commit);
        }

        return refusal;
    }

    /// <summary>
    /// Waits as <see cref="AwaitFollowersAsync" /> does and, past the retention limit, names the ready followers that stayed behind when
    /// leaving them behind keeps a write majority.
    /// </summary>
    /// <param name="coordinator">The running commit coordinator recording the follower acknowledgements.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="pastLimit">Whether the log reached its retention limit.</param>
    /// <param name="commit">The commit index every follower must hold.</param>
    /// <param name="clock">Time source bounding the wait.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The refusal, or <see langword="null" /> with the mask of ready follower slots that stayed behind <paramref name="commit" /> and may be
    /// left behind; the mask is empty when none stayed behind or leaving them would cost the write majority.
    /// </returns>
    internal static async Task<(ReplicaLogCompactionOutcome? Refusal, int LeaveBehind)> AdviseFollowersAsync(
        ReplicaCommitCoordinator coordinator,
        ReplicaEligibility eligibility,
        bool pastLimit,
        ulong commit,
        TimeProvider clock,
        CancellationToken cancellationToken)
    {
        var refusal = await AwaitFollowersAsync(coordinator, eligibility, pastLimit, commit, clock, cancellationToken).ConfigureAwait(false);
        if (!pastLimit || refusal != ReplicaLogCompactionOutcome.FollowerBehind)
            return (refusal, NoSlots);

        var behind = BehindFollowers(coordinator, eligibility, commit);
        return KeepsWriteMajority(eligibility, behind) ? (null, behind) : (ReplicaLogCompactionOutcome.FollowerBehind, NoSlots);
    }

    /// <summary>Makes the owned log's applied boundary durable and runs the supplied compaction action at that boundary.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="coordinator">The running commit coordinator.</param>
    /// <param name="slots">
    /// The participation state of the owned group slots, and whether the log reached its retention limit, past which a slot that is not
    /// ready no longer holds it, and the ready follower slots that stayed behind the commit index and may be left behind.
    /// </param>
    /// <param name="appliedIndex">The committer's in-memory applied index.</param>
    /// <param name="durability">The node cache journal the applies wrote to.</param>
    /// <param name="compact">The action to take once the applied boundary is durable.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The mapped result of the action, or a refusal from the checks before it ran.</returns>
    /// <remarks>
    /// The covered entries must be committed and applied, their idempotency outcomes resolved, and durably held by every ready follower.
    /// Below the retention limit every follower must be ready too, so a member that was down catches up from the log; past it, a follower
    /// that is not ready is left behind and repaired by a state transfer when it returns. A ready follower that stayed behind the commit
    /// index is demoted under the gate when the advisory check named it and the slots that remain still form a write majority; the
    /// demotion is all or nothing, and the follower is repaired by catch-up or a state transfer. The cache journal barrier then makes every
    /// applied entry durable before the log persists the applied index and drops the entries.
    /// <para>
    /// The slots are read once, before the compaction, and no slot becomes ready meanwhile: an admission takes the commit gate. A slot can
    /// be taken out of the write quorum meanwhile, which takes no gate; it then already holds every entry through the commit index, and no
    /// entry is appended while the gate is held, so the compaction drops nothing it needs and the outcome stays true for it. A slot this
    /// step demotes may lack entries through the commit index and is left behind on purpose; every slot that stays ready holds them.
    /// The demotion comes after the durability barrier and the applied advance, so a failure of either does not demote. When the
    /// compaction itself fails after a demotion, the follower stays out of the write quorum and is left to the normal repair.
    /// </para>
    /// </remarks>
    internal static async Task<ReplicaLogCompactionOutcome> RunAsync(
        IFollowerLog log,
        ReplicaCommitCoordinator coordinator,
        (ReplicaEligibility Eligibility, bool PastLimit, int LeaveBehind) slots,
        ulong appliedIndex,
        IJournalDurabilityCoordinator durability,
        Func<ulong, CancellationToken, Task<GroupCompactionOutcome>> compact,
        CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        var commit = status.CommitIndex;
        var refusal = (status.Readiness == FollowerLogReadiness.Ready, status.LastLogIndex == commit, !coordinator.HasPendingApply && appliedIndex == commit) switch
        {
            (false, _, _) => ReplicaLogCompactionOutcome.NotReady,
            (true, false, _) => ReplicaLogCompactionOutcome.UncommittedTail,
            (true, true, false) => ReplicaLogCompactionOutcome.PendingApply,
            (true, true, true) => FollowerRefusal(coordinator, slots.Eligibility, slots.PastLimit, commit),
        };
        var demotes = refusal == ReplicaLogCompactionOutcome.FollowerBehind && slots.PastLimit && slots.LeaveBehind != NoSlots &&
                      CanLeaveBehind(slots.Eligibility, slots.LeaveBehind, BehindFollowers(coordinator, slots.Eligibility, commit));
        if (refusal is { } refused && !demotes)
            return refused;

        // The barrier and the applied advance come before any demotion: they are harmless without a compaction, so their failures leave
        // every follower ready. The compaction itself runs after the demotion and can still refuse; that follower is then left to the repair.
        await durability.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        var applied = await log.AdvanceAppliedAsync(commit, cancellationToken).ConfigureAwait(false);
        if (!applied.Success)
            return ReplicaLogCompactionOutcome.NotReady;

        if (demotes && LeaveBehind(coordinator, slots.Eligibility, slots.LeaveBehind, commit) is { } stillRefused)
            return stillRefused;

        var leavesFollowerBehind = HasFollowerNotReady(coordinator, slots.Eligibility);
        var outcome = Map(await compact(commit, cancellationToken).ConfigureAwait(false));
        return outcome == ReplicaLogCompactionOutcome.Compacted && leavesFollowerBehind ? ReplicaLogCompactionOutcome.CompactedPastFollower : outcome;
    }

    /// <summary>Rechecks follower retention at the captured boundary before publishing a prepared compaction.</summary>
    /// <param name="log">The owning group log.</param>
    /// <param name="coordinator">The running commit coordinator.</param>
    /// <param name="eligibility">The current replica slot eligibility.</param>
    /// <param name="pastLimit">Whether the log exceeded its retention limit.</param>
    /// <param name="capture">The captured durable boundary.</param>
    /// <param name="prepared">The verified generation for that boundary.</param>
    /// <param name="cancellationToken">The cancellation token before publication.</param>
    /// <returns>The compaction outcome.</returns>
    internal static async Task<ReplicaLogCompactionOutcome> FinishAsync(
        IFollowerLog log,
        ReplicaCommitCoordinator coordinator,
        ReplicaEligibility eligibility,
        bool pastLimit,
        GroupSnapshotCompaction capture,
        GroupSnapshotCheckpointStore.PreparedSnapshot prepared,
        CancellationToken cancellationToken)
    {
        if (FollowerRefusal(coordinator, eligibility, pastLimit, capture.Snapshot.LastIncludedIndex) is { } refusal)
            return refusal;

        var leavesFollowerBehind = HasFollowerNotReady(coordinator, eligibility);
        var outcome = Map(await log.CompactPreparedAsync(capture, prepared, cancellationToken).ConfigureAwait(false));
        return outcome == ReplicaLogCompactionOutcome.Compacted && leavesFollowerBehind ? ReplicaLogCompactionOutcome.CompactedPastFollower : outcome;
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
    /// outcomes of the applied entries or nothing was applied past the last snapshot. The log gate is released during preparation so
    /// the leader can continue appending to this replica. Publication rechecks the captured boundary and preserves the current tail.
    /// </remarks>
    internal static async Task<ReplicaLogCompactionOutcome> RunFollowerAsync(IFollowerLog log, ReplicaLogCompactionPolicy policy, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        var retention = await log.GetRetentionAsync(cancellationToken).ConfigureAwait(false);
        if (!policy.IsReachedBy(in retention))
            return ReplicaLogCompactionOutcome.BelowThreshold;

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (!IsReadyAndComplete(in status))
            return ReplicaLogCompactionOutcome.NotReady;

        if (!log.Idempotency.OutcomesRebuilt || status.LastAppliedIndex <= retention.SnapshotIndex)
            return ReplicaLogCompactionOutcome.PendingApply;

        var (outcome, captured) = await log.CaptureCompactionAsync(status.LastAppliedIndex, cancellationToken).ConfigureAwait(false);
        if (captured is not { } capture)
            return Map(outcome);

        try
        {
            using var prepared = await log.PrepareCompactionAsync(capture, cancellationToken).ConfigureAwait(false);
            return prepared == null
                ? ReplicaLogCompactionOutcome.NotReady
                : Map(await log.CompactPreparedAsync(capture, prepared, cancellationToken).ConfigureAwait(false));
        }
        catch (GroupSnapshotTooLargeException)
        {
            return ReplicaLogCompactionOutcome.SnapshotTooLarge;
        }
    }

    /// <summary>Demotes the ready followers that stayed behind <paramref name="commit" /> when that is safe, and decides again.</summary>
    /// <param name="coordinator">The running commit coordinator.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="advised">The follower slots the advisory check named.</param>
    /// <param name="commit">The commit index every remaining ready follower must hold.</param>
    /// <returns>
    /// <see cref="ReplicaLogCompactionOutcome.FollowerBehind" /> without a demotion when a slot is behind that the advisory check did not name or
    /// the remaining slots would lose the write majority; otherwise the refusal after the demotion, which is <see langword="null" /> only when
    /// every follower still ready holds <paramref name="commit" />.
    /// </returns>
    private static ReplicaLogCompactionOutcome? LeaveBehind(ReplicaCommitCoordinator coordinator, ReplicaEligibility eligibility, int advised, ulong commit)
    {
        var behind = BehindFollowers(coordinator, eligibility, commit);
        if (!CanLeaveBehind(eligibility, advised, behind))
            return ReplicaLogCompactionOutcome.FollowerBehind;

        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            if ((behind & (1 << i)) != NoSlots)
                coordinator.Pipeline.RecordLaggingReplica(i, commit);
        }

        return FollowerRefusal(coordinator, eligibility, true, commit);
    }

    private static bool CanLeaveBehind(ReplicaEligibility eligibility, int advised, int behind) =>
        (behind & ~advised) == NoSlots && KeepsWriteMajority(eligibility, behind);

    /// <summary>Returns the ready follower slots that have not durably acknowledged <paramref name="commit" />.</summary>
    /// <param name="coordinator">The running commit coordinator recording the follower acknowledgements.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="commit">The commit index.</param>
    /// <returns>The bit mask of slots, one bit per zero-based replica slot.</returns>
    private static int BehindFollowers(ReplicaCommitCoordinator coordinator, ReplicaEligibility eligibility, ulong commit)
    {
        var slots = new ReplicaSlots(coordinator.LeaderReplicaIndex);
        var behind = NoSlots;
        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            if (slots.IsFollower(i) && eligibility.CanCountInWriteQuorum(i) && coordinator.MatchIndexFor(i) < commit)
                behind |= 1 << i;
        }

        return behind;
    }

    /// <summary>Tells whether the slots that count in the write quorum, without <paramref name="excluded" />, still form a write majority.</summary>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="excluded">The bit mask of slots to leave out.</param>
    /// <returns><see langword="true" /> when more than half of the slots remain.</returns>
    private static bool KeepsWriteMajority(ReplicaEligibility eligibility, int excluded)
    {
        var remaining = 0;
        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            if (eligibility.CanCountInWriteQuorum(i) && (excluded & (1 << i)) == NoSlots)
                remaining++;
        }

        return remaining >= (eligibility.ReplicaCount / 2) + 1;
    }

    /// <summary>Returns why a follower slot cannot let the step go on, or <see langword="null" /> when it holds <paramref name="commit" />.</summary>
    /// <param name="coordinator">The running commit coordinator recording the follower acknowledgements.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <param name="pastLimit">Whether a slot that is not ready is skipped because the log reached its retention limit.</param>
    /// <param name="commit">The commit index every follower must hold.</param>
    /// <returns>The first refusal among the follower slots, or <see langword="null" />.</returns>
    private static ReplicaLogCompactionOutcome? FollowerRefusal(ReplicaCommitCoordinator coordinator, ReplicaEligibility eligibility, bool pastLimit, ulong commit)
    {
        var slots = new ReplicaSlots(coordinator.LeaderReplicaIndex);
        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            if (!slots.IsFollower(i))
                continue;

            if (!eligibility.CanCountInWriteQuorum(i))
            {
                if (pastLimit)
                    continue;

                return ReplicaLogCompactionOutcome.FollowerNotReady;
            }

            if (coordinator.MatchIndexFor(i) < commit)
                return ReplicaLogCompactionOutcome.FollowerBehind;
        }

        return null;
    }

    /// <summary>Tells whether a follower slot is not ready, so a compaction past the retention limit leaves it behind.</summary>
    /// <param name="coordinator">The running commit coordinator naming the leader slot.</param>
    /// <param name="eligibility">The participation state of the owned group slots.</param>
    /// <returns><see langword="true" /> when some follower slot does not count in the write quorum.</returns>
    private static bool HasFollowerNotReady(ReplicaCommitCoordinator coordinator, ReplicaEligibility eligibility)
    {
        var slots = new ReplicaSlots(coordinator.LeaderReplicaIndex);
        for (var i = 0; i < eligibility.ReplicaCount; i++)
        {
            if (slots.IsFollower(i) && !eligibility.CanCountInWriteQuorum(i))
                return true;
        }

        return false;
    }

    /// <summary>Tells whether the log is ready and its copy is complete; a copy that is not complete never applies, so it never waits as pending apply.</summary>
    /// <param name="status">The durable log status.</param>
    /// <returns><see langword="true" /> when the log is ready and the copy is complete.</returns>
    private static bool IsReadyAndComplete(in FollowerLogStatus status) => status is { Readiness: FollowerLogReadiness.Ready, MemberState: GroupMemberState.Complete };

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
}
