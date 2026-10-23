using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Snapshot creation, installation, and compaction for a follower log.</summary>
internal static class FollowerLogSnapshot
{
    /// <summary>Publishes a snapshot, failing the readiness of the log when the store cannot tell whether the new snapshot became durable.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The owning log, whose readiness fails.</param>
    /// <param name="snapshot">The snapshot to publish.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the snapshot is durably published.</returns>
    /// <remarks>
    /// A snapshot too large, or a cancellation, leaves the previous snapshot in force and is rethrown as is. Any other failure may have left the new
    /// snapshot durable while the in-memory baseline trails it, so the log is never surfaced as ready afterwards.
    /// </remarks>
    internal static async Task PublishOrFailReadinessAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupSnapshot snapshot, CancellationToken cancellationToken)
    {
        try
        {
            await journal.Snapshot.PublishAsync(snapshot, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not (GroupSnapshotTooLargeException or OperationCanceledException))
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    /// <summary>Publishes a snapshot through <paramref name="index" /> and compacts the log prefix it covers.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being compacted.</param>
    /// <param name="index">The index to compact through.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The compaction outcome.</returns>
    /// <remarks>
    /// Runs under the log gate. Every refusal happens before anything durable changes and leaves readiness untouched: only a prefix
    /// that is committed and applied is compacted, and never while an entry of it still has an unresolved idempotency outcome, which
    /// the snapshot cannot carry. The index is the durable applied index; the commit index may be higher, and every frame above the
    /// index, committed or not, stays in the log as the retained tail, which must hold every index through the last one. The snapshot
    /// is published before the compaction core rewrites the log, so a crash between the two recovers from the snapshot plus the old log.
    /// </remarks>
    internal static async Task<GroupCompactionOutcome> CompactThroughAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong index,
        CancellationToken cancellationToken)
    {
        var eligible = index != 0UL && index == owner.Meta.LastAppliedIndex && index <= owner.Meta.CommitIndex && index <= owner.LastLogIndex &&
                       index >= journal.SnapshotBaseline.LastIncludedIndex;
        if (!eligible)
            return GroupCompactionOutcome.NotReady;

        if (owner.Idempotency.HasUnresolvedThrough(index))
            return GroupCompactionOutcome.UnresolvedOutcome;

        // A retained tail missing an index cannot be rewritten without leaving a gap, so it is refused before the snapshot is published.
        var tail = CollectRetainedTail(journal, index);
        if (ulong.CreateChecked(tail.Count) != owner.LastLogIndex - index)
            return GroupCompactionOutcome.NotReady;

        var snapshot = BuildSnapshot(journal, owner, index);

        try
        {
            await PublishOrFailReadinessAsync(journal, owner, snapshot, cancellationToken).ConfigureAwait(false);
        }
        catch (GroupSnapshotTooLargeException)
        {
            return GroupCompactionOutcome.SnapshotTooLarge;
        }

        owner.RestoreBaseline(new SnapshotBaseline(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm));
        var compacted = await CompactAsync(journal, owner, cancellationToken).ConfigureAwait(false);
        return compacted.Success ? GroupCompactionOutcome.Compacted : GroupCompactionOutcome.NotReady;
    }

    /// <summary>
    /// Returns the topology-mismatch refusal when the snapshot's topology fingerprint or configuration generation
    /// conflicts with the durable metadata; an empty durable fingerprint adopts the snapshot's fingerprint and never
    /// conflicts. Returns <see langword="null" /> when the snapshot is compatible.
    /// </summary>
    /// <param name="owner">They log the snapshot targets.</param>
    /// <param name="snapshot">The snapshot to validate.</param>
    /// <returns>The <see cref="FollowerLogRefusal.TopologyMismatch" /> marker, or <see langword="null" />.</returns>
    internal static string? SnapshotTopologyMismatch(IFollowerLogContext owner, in GroupSnapshot snapshot)
    {
        var memory = owner.Meta.TopologyFingerprint;
        return (!memory.IsEmpty && !memory.Span.SequenceEqual(snapshot.TopologyFingerprint.Span)) || snapshot.ConfigurationGeneration < owner.Meta.ConfigurationGeneration
            ? FollowerLogRefusal.TopologyMismatch
            : null;
    }

    internal static GroupSnapshot BuildSnapshot(FollowerLogJournal journal, IFollowerLogContext owner, ulong lastIncludedIndex)
    {
        if (!journal.EntryOffsets.TryGetValue(lastIncludedIndex, out var location))
            return BuildSnapshotFromBaseline(journal, owner, lastIncludedIndex);

        // The two adjacent ulong arguments are LastIncludedIndex and CommitIndex, in declaration order; both
        // equal the snapshot boundary for a freshly created snapshot.
        var outcomes = ExportCoveredOutcomes(owner, lastIncludedIndex, out var capturedUtc);
        return new GroupSnapshot(
            owner.GroupId,
            owner.Meta.TopologyFingerprint,
            owner.Meta.ConfigurationGeneration,
            location.Term,
            lastIncludedIndex,
            lastIncludedIndex,
            outcomes,
            capturedUtc);
    }

    private static GroupSnapshot BuildSnapshotFromBaseline(FollowerLogJournal journal, IFollowerLogContext owner, ulong lastIncludedIndex)
    {
        // The covered index may be the snapshot base itself, whose frame was already compacted away.
        if (journal.SnapshotBaseline.LastIncludedIndex == 0UL || lastIncludedIndex != journal.SnapshotBaseline.LastIncludedIndex)
            throw new InvalidOperationException($"Replica group '{owner.GroupId}' cannot snapshot from a missing index '{lastIncludedIndex}'.");

        var outcomes = ExportCoveredOutcomes(owner, lastIncludedIndex, out var capturedUtc);
        return new GroupSnapshot(
            owner.GroupId,
            owner.Meta.TopologyFingerprint,
            owner.Meta.ConfigurationGeneration,
            journal.SnapshotBaseline.LastIncludedTerm,
            lastIncludedIndex,
            lastIncludedIndex,
            outcomes,
            capturedUtc);
    }

    /// <summary>Extracts the journal indexes carried by the retained tail entries.</summary>
    /// <param name="tail">The retained tail entries.</param>
    /// <returns>The journal indexes of the retained tail.</returns>
    private static List<ulong> CollectRetainedLogIndexes(List<FollowerLogEntry> tail)
    {
        var retainedLogIndexes = new List<ulong>(tail.Count);
        for (var i = 0; i < tail.Count; i++)
            retainedLogIndexes.Add(tail[i].LogIndex);

        return retainedLogIndexes;
    }

    private static List<FollowerLogEntry> CollectRetainedTail(FollowerLogJournal journal, ulong boundaryIndex)
    {
        var tail = new List<FollowerLogEntry>();

        foreach (var pair in journal.Entries)
        {
            if (pair.Key > boundaryIndex)
                tail.Add(pair.Value);
        }

        return tail;
    }

    private static async Task<GroupCompactionResult> CompactAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
    {
        if (!journal.Snapshot.SnapshotExists)
            return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

        var snapshot = await journal.Snapshot.ReadPublishedAsync(cancellationToken).ConfigureAwait(false);
        if (snapshot == null)
            return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

        var snapshotValue = snapshot.Value;
        if (RefuseCompactionSnapshot(owner, journal, in snapshotValue) is { } refusal)
            return new GroupCompactionResult(false, null, refusal);

        var included = snapshotValue.LastIncludedIndex;
        var tail = CollectRetainedTail(journal, included);

        // Every frame above the boundary must be rewritten: a missing index would leave a gap, committed entries included, that
        // recovery refuses. Nothing durable changed yet beyond the published snapshot, which recovers on its own.
        if (ulong.CreateChecked(tail.Count) != owner.LastLogIndex - included)
            return new GroupCompactionResult(false, null, FollowerLogRefusal.NotReady);

        var retainedLogIndexes = CollectRetainedLogIndexes(tail);
        var (length, offsets) = await ReplaceRewrittenTailAsync(journal, owner, tail, cancellationToken).ConfigureAwait(false);
        ReindexTail(journal, tail, offsets);

        owner.LastLogIndex = tail.Count == 0 ? included : tail[^1].LogIndex;
        owner.LogLength = length;

        // ReindexTail already left only entries above the boundary, so the paired advance prunes nothing.
        journal.AdvanceBaseline(new SnapshotBaseline(snapshotValue.LastIncludedIndex, snapshotValue.LastIncludedTerm));

        RestoreCompactionIdempotency(owner, in snapshotValue, retainedLogIndexes);
        return new GroupCompactionResult(true, journal.Snapshot.SnapshotPath, string.Empty);
    }

    private static string? RefuseCompactionSnapshot(IFollowerLogContext owner, FollowerLogJournal journal, in GroupSnapshot snapshot)
    {
        return RefuseWatermarkMismatch(owner, snapshot.LastIncludedIndex)
            ?? RefuseIdentityMismatch(owner, in snapshot)
            ?? RefuseTermMismatch(journal, snapshot.LastIncludedIndex, in snapshot);
    }

    private static string? RefuseWatermarkMismatch(IFollowerLogContext owner, ulong included)
    {
        // The durable prefix is dropped only through a committed index the published snapshot covers; committed entries above it stay
        // in the retained tail.
        // Frames in (included, LastAppliedIndex] were released from `Entries` by PruneAppliedEntries, so the
        // retained tail cannot reproduce them. Compacting here would drop committed frames the snapshot does
        // not cover and leave a committed gap for recovery.
        return included == 0UL || included > owner.Meta.CommitIndex || included > owner.LastLogIndex || included > owner.Meta.LastAppliedIndex || included < owner.Meta.LastAppliedIndex
            ? FollowerLogRefusal.NotReady
            : null;
    }

    private static string? RefuseIdentityMismatch(IFollowerLogContext owner, in GroupSnapshot snapshot)
    {
        // Validate the published snapshot's identity and topology against the durable local state before any
        // destructive rewrite. A snapshot that diverges from the replica must not be used to truncate the durable
        // prefix; the suffix must instead survive as a replicable tail.
        return !string.Equals(snapshot.GroupId, owner.GroupId, StringComparison.Ordinal)
            ? FollowerLogRefusal.NotMember
            : SnapshotTopologyMismatch(owner, in snapshot);
    }

    private static string? RefuseTermMismatch(FollowerLogJournal journal, ulong included, in GroupSnapshot snapshot)
    {
        return journal.EntryOffsets.TryGetValue(included, out var boundary) && boundary.Term != snapshot.LastIncludedTerm
            ? FollowerLogRefusal.LogMismatch
            : null;
    }

    private static async Task<(int Length, List<long> Offsets)> ReplaceRewrittenTailAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        List<FollowerLogEntry> tail,
        CancellationToken cancellationToken)
    {
        try
        {
            return await FollowerLogDurable.ReplaceLogAsync(journal, owner, tail, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    private static void RestoreCompactionIdempotency(IFollowerLogContext owner, in GroupSnapshot snapshot, List<ulong> retainedLogIndexes)
    {
        // The discarded prefix is now owned by the snapshot, which exports only resolved outcomes; any remaining
        // record at or below the included index has lost its durable journal frame and must be released, while records
        // carried by the retained tail stay authoritative. The restore is never refused for the idempotency capacity,
        // so outcomes recorded while the snapshot was published cannot fail the log here.
        try
        {
            owner.Idempotency.RestoreFromSnapshot(snapshot.CommittedOutcomes, snapshot.CapturedUtc, retainedLogIndexes);
        }
        catch
        {
            // A failed idempotency restore leaves the in-memory map holding discarded-prefix records whose
            // journal frames no longer exist; mark the log failed so it is never surfaced as Ready.
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    private static List<GroupIdempotencyRecord> ExportCoveredOutcomes(IFollowerLogContext owner, ulong lastIncludedIndex, out DateTime capturedUtc)
    {
        var outcomes = new List<GroupIdempotencyRecord>();
        foreach (var record in owner.Idempotency.ExportResolved(out capturedUtc))
        {
            if (record.LogIndex <= lastIncludedIndex)
                outcomes.Add(record);
        }

        return outcomes;
    }

    private static void ReindexTail(FollowerLogJournal journal, List<FollowerLogEntry> tail, List<long> offsets)
    {
        journal.ClearEntries();
        for (var i = 0; i < tail.Count; i++)
            journal.AddEntry(tail[i], offsets[i], tail[i].Term);
    }
}
