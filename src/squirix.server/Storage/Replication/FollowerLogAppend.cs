using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Append-protocol operations for a follower log.</summary>
internal static class FollowerLogAppend
{
    internal static async Task<FollowerLogAppliedResult> AdvanceAppliedMonotonicAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong appliedIndex,
        CancellationToken cancellationToken)
    {
        // Applied index moves only monotonically.
        if (appliedIndex <= owner.Meta.LastAppliedIndex)
            return new FollowerLogAppliedResult(true, string.Empty, owner.Meta.LastAppliedIndex);

        // Never applied beyond the committed index.
        if (appliedIndex > owner.Meta.CommitIndex)
            return new FollowerLogAppliedResult(false, FollowerLogRefusal.NotReady, owner.Meta.LastAppliedIndex);

        // The watermark is persisted before the payloads are released; on a crash between the two, restart
        // reloads the frames, but the durable watermark still suppresses re-application of the applied prefix.
        var candidate = owner.Meta with { LastAppliedIndex = appliedIndex };
        await PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
        owner.Meta = candidate;
        journal.ReleaseAppliedEntries(owner.Meta.LastAppliedIndex);
        return new FollowerLogAppliedResult(true, string.Empty, appliedIndex);
    }

    internal static async Task<FollowerLogCommitResult> AdvanceCommitMonotonicAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong commitIndex,
        CancellationToken cancellationToken)
    {
        // Commit index moves only monotonically.
        if (commitIndex <= owner.Meta.CommitIndex && journal.Metadata.DurableCommitIndex >= owner.Meta.CommitIndex)
            return new FollowerLogCommitResult(true, string.Empty, owner.Meta.CommitIndex);

        // Never beyond the locally durable last index.
        if (commitIndex > owner.LastLogIndex)
            return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, owner.Meta.CommitIndex);

        var candidate = owner.Meta with { CommitIndex = Math.Max(commitIndex, owner.Meta.CommitIndex) };
        await PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
        owner.Meta = candidate;
        owner.Faults.OnCommitAdvanced();
        return new FollowerLogCommitResult(true, string.Empty, candidate.CommitIndex);
    }

    internal static async Task<FollowerLogAppendResult?> AdvanceTermIfHigherAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        FollowerLogAppendRequest request,
        CancellationToken cancellationToken)
    {
        // Higher term is persisted durably before any further response; the old leader stops being authoritative.
        var term = owner.Meta.CurrentTerm;
        if (request.CurrentTerm <= term)
            return request.CurrentTerm < term ? new FollowerLogAppendResult(false, FollowerLogRefusal.StaleTerm, term, owner.LastLogIndex) : null;

        var candidate = owner.Meta with { CurrentTerm = request.CurrentTerm, VotedFor = string.Empty };
        await PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
        owner.Meta = candidate;
        return null;
    }

    internal static async Task<FollowerLogAppendResult> AppendVerifiedBatchAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        FollowerLogAppendRequest request,
        bool backpressure,
        CancellationToken cancellationToken,
        Action? onScheduled = null)
    {
        // Validate the whole batch for contiguity and conflicts before writing anything.
        var entries = request.Entries;
        var lastVerifiedIndex = entries.Length == 0 ? request.PrevLogIndex : entries.Span[entries.Length - 1].LogIndex;
        if (entries.Length == 0)
            return CompleteAppend(owner, request.LeaderCommitIndex, lastVerifiedIndex);

        (FollowerLogAppendResult? Error, List<FollowerLogEntry>? ToAppend, ulong? TruncateAt) prepared;
        try
        {
            prepared = await PrepareAppendBatchAsync(journal, owner, request, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            return new FollowerLogAppendResult(false, FollowerLogRefusal.ChecksumMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);
        }
        catch (IOException)
        {
            // The frame may still be intact. Leave the WAL unchanged and report the durable term so the request can be retried.
            return new FollowerLogAppendResult(false, FollowerLogRefusal.NotReady, owner.Meta.CurrentTerm, owner.LastLogIndex);
        }

        var (error, toAppend, truncateAtIndex) = prepared;
        if (error != null)
            return error.Value;

        if (backpressure && toAppend != null && ShouldDeferAppend(journal, owner, request.LeaderCommitIndex, toAppend, truncateAtIndex))
            return new FollowerLogAppendResult(false, FollowerLogRefusal.ApplyBacklog, owner.Meta.CurrentTerm, owner.LastLogIndex);

        if (truncateAtIndex != null)
        {
            try
            {
                _ = await FollowerLogDurable.TruncateFromAsync(journal, owner, truncateAtIndex.Value, cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException ex)
            {
                // The truncate was refused or cut by the shutdown, and the frames of the entries are scheduled only after it succeeds.
                throw new ReplicaShutdownRefusedException(nameof(FollowerLog), ex);
            }
        }

        if (toAppend != null)
            await FollowerLogDurable.AppendFramesDurableAsync(journal, owner, toAppend, cancellationToken, onScheduled).ConfigureAwait(false);

        // The frames are durable and the announced commit index only moves in memory, so nothing after them can fail.
        return CompleteAppend(owner, request.LeaderCommitIndex, lastVerifiedIndex);
    }

    /// <summary>Gets the highest commit index a copy knows: the durable one, or the one the leader announced and the copy verified when it is higher.</summary>
    /// <param name="owner">The owning follower log.</param>
    /// <returns>The commit index below which the log is never truncated, replaced, or moved back.</returns>
    /// <remarks>Callers hold the log gate.</remarks>
    internal static ulong EffectiveCommitIndex(IFollowerLogState owner) => Math.Max(owner.Meta.CommitIndex, owner.CommitHint);

    internal static bool IsLogUpToDate(ulong candidateLastTerm, ulong candidateLastIndex, ulong localLastTerm, ulong localLastIndex) =>
        candidateLastTerm != localLastTerm ? candidateLastTerm > localLastTerm : candidateLastIndex >= localLastIndex;

    internal static async Task PersistMetaOrFailReadinessAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        GroupLogMetadata candidate,
        CancellationToken cancellationToken)
    {
        try
        {
            await AwaitPendingMirrorAsync(journal, cancellationToken).ConfigureAwait(false);
            await FollowerLogDurable.PersistMetaAsync(journal, owner, in candidate, false, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (FailsReadiness(ex))
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    /// <summary>
    /// Persists a vote grant, a term step, or a member state change into both metadata slots, failing readiness like
    /// <see cref="PersistMetaOrFailReadinessAsync" />.
    /// </summary>
    /// <param name="journal">The storage surface.</param>
    /// <param name="owner">The owning follower log.</param>
    /// <param name="candidate">The metadata to store in both slots.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once both copies are durable.</returns>
    internal static async Task PersistMirroredMetaOrFailReadinessAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        GroupLogMetadata candidate,
        CancellationToken cancellationToken)
    {
        try
        {
            await AwaitPendingMirrorAsync(journal, cancellationToken).ConfigureAwait(false);
            await FollowerLogDurable.PersistMetaAsync(journal, owner, in candidate, true, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (FailsReadiness(ex))
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    /// <summary>
    /// Persists a vote grant or a term step into both metadata slots, answering after the first copy is durable: the mirror copy is written
    /// right after by the same work item and is left pending on the owner. Fails readiness like <see cref="PersistMetaOrFailReadinessAsync" />.
    /// </summary>
    /// <param name="journal">The storage surface.</param>
    /// <param name="owner">The owning follower log.</param>
    /// <param name="candidate">The metadata to store in both slots.</param>
    /// <param name="cancellationToken">Cancellation token; it bounds the wait for an earlier mirror copy and the scheduling, not the write.</param>
    /// <returns>A task that completes once the first copy is durable; the mirror copy may still be pending.</returns>
    /// <remarks>
    /// A failed first copy writes no mirror copy and is reported here. A failed mirror copy is reported by the next metadata publish, or by the
    /// next gate entry that finds it finished; the first copy stays the durable record.
    /// </remarks>
    internal static async Task PersistDeferredMirrorOrFailAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        GroupLogMetadata candidate,
        CancellationToken cancellationToken)
    {
        try
        {
            await AwaitPendingMirrorAsync(journal, cancellationToken).ConfigureAwait(false);
            var (firstCopy, mirror) = FollowerLogDurable.PersistMetaDeferredMirror(journal, owner, in candidate, cancellationToken);

            // The owner retains the mirror for readiness and shutdown reporting, but a timed-out shutdown faults its ack
            // after abandoning the drain. Keep observation from scheduling onward, including a failed first copy.
            _ = mirror.ContinueWith(
                static finished => finished.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            await firstCopy.ConfigureAwait(false);

            journal.Metadata.PendingMirror = mirror;
        }
        catch (Exception ex) when (FailsReadiness(ex))
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    /// <summary>Tells whether a failed metadata publish fails readiness.</summary>
    /// <param name="exception">The failure.</param>
    /// <returns><see langword="false" /> for cancellation and a dispose, which say nothing about the storage: cancellation lets the caller retry, and a write faulted by a dispose may still land and is reconciled by restart recovery.</returns>
    internal static bool FailsReadiness(Exception exception) => exception is not (OperationCanceledException or ObjectDisposedException);

    internal static FollowerLogAppendResult? VerifyPreviousLogConsistency(FollowerLogJournal journal, IFollowerLogContext owner, in FollowerLogAppendRequest request)
    {
        // Previous-log consistency; the term at an applied index was released from memory, so the check
        // covers only the retained region above the applied watermark. The term of an applied entry is read
        // back from the retained frame metadata; a leader claiming a conflicting term there violates the
        // Leader Completeness property.
        if (request.PrevLogIndex > owner.LastLogIndex)
            return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);

        if (request.PrevLogIndex <= 0)
            return null;

        if (request.PrevLogIndex <= owner.Meta.LastAppliedIndex)
        {
            var term = TermAtApplied(journal, request.PrevLogIndex);

            // A term of zero at an applied index means the frame was compacted away and is not the snapshot
            // baseline: the prefix cannot be verified from memory. Terms start at 1, so a zero term is always
            // unverifiable and must be refused rather than accepted as consistent.
            var logMatches = term != 0UL && term == request.PrevLogTerm;
            var unverifiable = term == 0UL;
            return (logMatches, unverifiable) switch
            {
                (true, _) => null,
                (false, true) => new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex),
                (false, false) => FailReadiness(owner),
            };
        }

        // A term conflict at or below the committed index violates the Leader Completeness property; fail readiness.
        var termMatches = journal.TryGetEntryOffset(request.PrevLogIndex, out var previous) && previous.Term == request.PrevLogTerm;
        var belowCommitted = request.PrevLogIndex <= EffectiveCommitIndex(owner);
        return (termMatches, belowCommitted) switch
        {
            (true, _) => null,
            (false, true) => FailReadiness(owner),
            (false, false) => new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex),
        };

        static ulong TermAtApplied(FollowerLogJournal journal, ulong logIndex)
        {
            // The snapshot base's frame was compacted away, but its term is retained for consistency checks.
            var baseline = journal.SnapshotBaseline;
            var baselineTerm = baseline.LastIncludedIndex > 0UL && logIndex == baseline.LastIncludedIndex ? baseline.LastIncludedTerm : 0UL;
            return journal.EntryOffsets.TryGetValue(logIndex, out var location) ? location.Term : baselineTerm;
        }
    }

    /// <summary>Waits for the mirror copy of an earlier deferred publish, so the metadata store is never written concurrently.</summary>
    /// <param name="journal">The storage surface.</param>
    /// <param name="cancellationToken">Cancellation token that bounds the wait.</param>
    /// <returns>A task that completes once no mirror copy is pending.</returns>
    /// <exception cref="OperationCanceledException">The token was canceled before the mirror copy finished; the mirror copy stays pending.</exception>
    /// <remarks>The failure of the mirror copy is thrown once, to the first publish that waits for it, and that publish does not run.</remarks>
    [SuppressMessage("Usage", "VSTHRD003", Justification = "The pending mirror is the ack of a work item this log scheduled; the work item or the shutdown drain completes it.")]
    private static async Task AwaitPendingMirrorAsync(FollowerLogJournal journal, CancellationToken cancellationToken)
    {
        if (journal.Metadata.PendingMirror is not { } pending)
            return;

        if (!pending.IsCompleted)
            await pending.WaitAsync(cancellationToken).ConfigureAwait(false);

        journal.Metadata.PendingMirror = null;
        await pending.ConfigureAwait(false);
    }

    /// <summary>Answers an append whose frames are durable, and takes the commit index the leader announced into the in-memory hint.</summary>
    /// <param name="owner">The owning follower log.</param>
    /// <param name="leaderCommitIndex">The commit index of the leader.</param>
    /// <param name="lastVerifiedIndex">The last index this append verified against the leader log.</param>
    /// <returns>The accepted append.</returns>
    /// <remarks>
    /// As in Raft, the commit index of a follower is volatile: it is the minimum of the leader commit index and the last index the leader verified, and a
    /// restart learns it again from the next append. It is made durable by the apply loop before it applies, so the append answers after one flush, of the frames.
    /// </remarks>
    private static FollowerLogAppendResult CompleteAppend(IFollowerLogContext owner, ulong leaderCommitIndex, ulong lastVerifiedIndex)
    {
        AdvanceCommitHint(owner, leaderCommitIndex, lastVerifiedIndex);
        return new FollowerLogAppendResult(true, string.Empty, owner.Meta.CurrentTerm, owner.LastLogIndex);
    }

    private static void AdvanceCommitHint(IFollowerLogContext owner, ulong leaderCommitIndex, ulong lastVerifiedIndex)
    {
        var target = Math.Min(leaderCommitIndex, lastVerifiedIndex);
        if (target > EffectiveCommitIndex(owner))
            owner.CommitHint = target;
    }

    private static bool ShouldDeferAppend(
        FollowerLogJournal journal, IFollowerLogContext owner, ulong leaderCommitIndex, List<FollowerLogEntry> entries, ulong? truncateAt)
    {
        if ((entries.Count == 1 && entries[0].Payload.Length > journal.MaxPayloadBytes) || CanRetainBatch(journal, entries, truncateAt))
            return false;

        // The refused suffix is not verified. Its matching prefix can still carry a commit that lets apply release payloads.
        var verifiedThrough = entries[0].LogIndex - 1UL;
        var commitIndex = Math.Max(EffectiveCommitIndex(owner), Math.Min(leaderCommitIndex, verifiedThrough));
        foreach (var pair in journal.Entries)
        {
            if (pair.Key > commitIndex)
                break;

            if (pair.Key > owner.Meta.LastAppliedIndex && pair.Value.Payload.Length != 0)
            {
                AdvanceCommitHint(owner, leaderCommitIndex, verifiedThrough);
                journal.FlushThrough = Math.Max(journal.FlushThrough, commitIndex);
                return true;
            }
        }

        // Waiting cannot free bytes held by the uncommitted tail. Write the frames so a
        // leader can commit that tail; any payload that does not fit stays on disk.
        return false;
    }

    private static bool CanRetainBatch(FollowerLogJournal journal, List<FollowerLogEntry> entries, ulong? truncateAt)
    {
        long addedBytes = 0;
        foreach (var entry in CollectionsMarshal.AsSpan(entries))
            addedBytes += entry.Payload.Length;

        var available = journal.MaxPayloadBytes - journal.PayloadBytes;
        if (addedBytes > available && truncateAt is { } firstRemoved)
        {
            foreach (var pair in journal.Entries)
            {
                if (pair.Key >= firstRemoved)
                    available += pair.Value.Payload.Length;
            }
        }

        return addedBytes <= available;
    }

    private static FollowerLogAppendResult FailReadiness(IFollowerLogContext owner)
    {
        owner.Readiness = FollowerLogReadiness.Failed;
        return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);
    }

    private static async ValueTask<bool> IsSatisfiedByLocalStateAsync(
        FollowerLogJournal journal, IFollowerLogContext owner, FollowerLogEntry candidate, CancellationToken cancellationToken)
    {
        // The term of an applied entry was released with its payload, so it is read back from the retained
        // frame metadata. Compaction removes the covered prefix from EntryOffsets, so a frame that is absent
        // there is correctly not acknowledged; a retained frame is verifiable by term alone because Leader
        // Completeness forbids a conflicting term at an applied index.
        if (journal.TryGetEntry(candidate.LogIndex, out var existing))
        {
            return existing.Term == candidate.Term &&
                   (candidate.LogIndex <= owner.Meta.LastAppliedIndex || existing.PayloadSpan.SequenceEqual(candidate.PayloadSpan));
        }

        if (!journal.TryGetEntryOffset(candidate.LogIndex, out var location) || location.Term != candidate.Term)
            return false;
        if (candidate.LogIndex <= owner.Meta.LastAppliedIndex)
            return true;

        var frames = await FollowerLogRead.ReadPayloadsAsync(journal, owner, candidate.LogIndex, 1, long.MaxValue, cancellationToken).ConfigureAwait(false);
        return frames[0].Term == candidate.Term && frames[0].PayloadSpan.SequenceEqual(candidate.PayloadSpan);
    }

    private static async ValueTask<(FollowerLogAppendResult? Error, List<FollowerLogEntry>? ToAppend, ulong? TruncateAt)> PrepareAppendBatchAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        FollowerLogAppendRequest request,
        CancellationToken cancellationToken)
    {
        List<FollowerLogEntry>? toAppend = null;
        ulong? truncateAtIndex = null;
        var nextExpected = request.PrevLogIndex + 1;
        var entries = request.Entries;

        for (var i = 0; i < entries.Length; i++)
        {
            // A leader never sends an entry of a term above its own: such a batch is malformed, and taking it would leave this
            // copy with a last log term above its current term.
            var entry = entries.Span[i];
            if (entry.LogIndex != nextExpected || entry.Term > request.CurrentTerm)
                return (new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex), null, null);

            nextExpected++;

            // Once the divergent tail is being rewritten, every subsequent entry must be re-appended durably.
            if (truncateAtIndex != null)
            {
                toAppend!.Add(entry);
                continue;
            }

            // Entries already satisfied by local state need no durable writing: duplicates already present with
            // identical content, and applied entries whose payloads were released after application (Leader
            // Completeness guarantees a current-term leader cannot create a conflict at an applied index).
            if (entry.LogIndex <= owner.LastLogIndex && await IsSatisfiedByLocalStateAsync(journal, owner, entry, cancellationToken).ConfigureAwait(false))
                continue;

            if (entry.LogIndex <= EffectiveCommitIndex(owner))
                return (FailReadiness(owner), null, null);

            if (entry.LogIndex <= owner.LastLogIndex)
                truncateAtIndex = entry.LogIndex;

            toAppend ??= [];
            toAppend.Add(entry);
        }

        return (null, toAppend, truncateAtIndex);
    }
}
