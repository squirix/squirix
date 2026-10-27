using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Watermark rules that tie startup recovery to the restored snapshot boundary.</summary>
internal static class SnapshotBoundaryWatermarks
{
    /// <summary>Completes recovery of a populated log after its frames were walked.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="snapshotBase">The restored snapshot boundary, or zero when no snapshot is published.</param>
    /// <param name="walk">The index the walk started from and the byte offset after the last valid frame.</param>
    /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
    /// <returns>A task that completes when the log state, the applied watermark, and the retained payloads are consistent.</returns>
    internal static async Task CompleteWalkAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong snapshotBase,
        (ulong BaseIndex, long LastValidEnd) walk,
        CancellationToken cancellationToken)
    {
        owner.LogLength = walk.LastValidEnd;
        EnsureCommittedPrefixCovered(owner, snapshotBase);

        // Only a frame the walk found above its base counts as held by the log.
        await RaiseAppliedToBoundaryAsync(journal, owner, snapshotBase, owner.LastLogIndex > walk.BaseIndex ? owner.LastLogIndex : 0UL, cancellationToken)
           .ConfigureAwait(false);

        // Releases the applied entry payloads from memory, retaining their durable frame offsets.
        journal.ReleaseAppliedEntries(owner.Meta.LastAppliedIndex);
    }

    /// <summary>Resets the log state to the snapshot boundary, checks the committed prefix, and raises applied as for a log without frames.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="snapshotBase">The restored snapshot boundary, or zero when no snapshot is published.</param>
    /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
    /// <returns>A task that completes when the state is reset.</returns>
    internal static async Task ResetAndEnsureAsync(FollowerLogJournal journal, IFollowerLogContext owner, ulong snapshotBase, CancellationToken cancellationToken)
    {
        ResetLogState(owner, snapshotBase);
        EnsureCommittedPrefixCovered(owner, snapshotBase);

        // The log holds no frame, so nothing above the snapshot base is left to apply.
        await RaiseAppliedToBoundaryAsync(journal, owner, snapshotBase, 0UL, cancellationToken).ConfigureAwait(false);
    }

    private static void EnsureCommittedPrefixCovered(IFollowerLogContext owner, ulong snapshotBase)
    {
        // Indexes at or below the restored snapshot baseline stay covered by the published snapshot even when
        // the walked journal ends below them: a crash between snapshot publication and the installation log
        // rewrite leaves the previous, shorter log in place under already-advanced watermarks. The next
        // replication round rebuilds the discarded span from the leader.
        if (owner.Meta.CommitIndex <= Math.Max(owner.LastLogIndex, snapshotBase))
            return;
        owner.Readiness = FollowerLogReadiness.Failed;
        throw new InvalidDataException($"Replica group '{owner.GroupId}' commit index exceeds the durable log.");
    }

    /// <summary>Raises the applied watermark to the snapshot boundary unless the log holds a frame at or above the boundary.</summary>
    /// <remarks>
    /// The applied watermark is owned by AdvanceAppliedAsync, not by the snapshot boundary. A snapshot carries only idempotency and commit
    /// state, not the application payload, so a frame the log holds at or above the boundary keeps the watermark where it is and is applied
    /// on catch-up. Frames below the boundary are covered by the snapshot and are skipped. The boundary is compared against the highest
    /// frame found in the log file. A retried open reads the metadata from the
    /// disk again, so this step is safe to repeat.
    /// </remarks>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="snapshotBase">The restored snapshot boundary, or zero when no snapshot is published.</param>
    /// <param name="highestFrameIndex">The highest frame index found in the log file, or zero when it holds no frame.</param>
    /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
    private static async Task RaiseAppliedToBoundaryAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong snapshotBase,
        ulong highestFrameIndex,
        CancellationToken cancellationToken)
    {
        if (snapshotBase <= owner.Meta.LastAppliedIndex || snapshotBase <= highestFrameIndex)
            return;

        var candidate = owner.Meta with { LastAppliedIndex = snapshotBase };
        await FollowerLogAppend.PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
        owner.Meta = candidate;
    }

    private static void ResetLogState(IFollowerLogContext owner, ulong snapshotBase)
    {
        owner.LogLength = GroupLogSlots.HeaderSize;
        owner.LastLogIndex = snapshotBase;
    }
}
