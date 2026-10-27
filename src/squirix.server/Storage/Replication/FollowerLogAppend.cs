using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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
        if (commitIndex <= owner.Meta.CommitIndex)
            return new FollowerLogCommitResult(true, string.Empty, owner.Meta.CommitIndex);

        // Never beyond the locally durable last index.
        if (commitIndex > owner.LastLogIndex)
            return new FollowerLogCommitResult(false, FollowerLogRefusal.NotReady, owner.Meta.CommitIndex);

        var candidate = owner.Meta with { CommitIndex = commitIndex };
        await PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
        owner.Meta = candidate;
        owner.Faults.OnCommitAdvanced();
        return new FollowerLogCommitResult(true, string.Empty, commitIndex);
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
        CancellationToken cancellationToken)
    {
        // Validate the whole batch for contiguity and conflicts before writing anything.
        var entries = request.Entries;
        var lastVerifiedIndex = entries.Length == 0 ? request.PrevLogIndex : entries.Span[entries.Length - 1].LogIndex;
        if (entries.Length == 0)
            return await CompleteAppendAsync(journal, owner, request.LeaderCommitIndex, lastVerifiedIndex, cancellationToken).ConfigureAwait(false);

        var error = PrepareAppendBatch(journal, owner, in request, out var toAppend, out var truncateAtIndex);
        if (error != null)
            return error.Value;

        // Materialize payload ownership for the appending set synchronously, before the first awaits: once
        // the durable writing is scheduled, it must never reference the caller's buffer, and a cancellation
        // during truncation aborts the batch without writing any frame.
        var ownedToAppend = toAppend is { Count: > 0 } ? MaterializeOwnedEntries(toAppend) : null;

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

        if (ownedToAppend != null)
            await FollowerLogDurable.AppendFramesDurableAsync(journal, owner, ownedToAppend, cancellationToken).ConfigureAwait(false);

        // Durable frames are part of the log whatever happens next: a restart recovers them. Cancelling the metadata write behind
        // them would report an append that already happened as refused, so it runs to completion.
        if (ownedToAppend == null)
            return await CompleteAppendAsync(journal, owner, request.LeaderCommitIndex, lastVerifiedIndex, cancellationToken).ConfigureAwait(false);

        try
        {
            return await CompleteAppendAsync(journal, owner, request.LeaderCommitIndex, lastVerifiedIndex, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The frames are in the log file, so the failure of what follows them cannot be reported as a refusal. That includes a
            // shutdown that faulted the metadata write: its flush may still land through the leaked handle.
            owner.LatchAppendUnconfirmed(ex);
            throw new GroupLogAppendUnconfirmedException(ex);
        }
    }

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
            try
            {
                await firstCopy.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // The mirror ack of a failed first copy is faulted with the same failure and has no other reader.
                _ = mirror.ContinueWith(
                    static finished => finished.Exception,
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
                throw;
            }

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
        var termMatches = TermAt(journal, request.PrevLogIndex) == request.PrevLogTerm;
        var belowCommitted = request.PrevLogIndex <= owner.Meta.CommitIndex;
        return (termMatches, belowCommitted) switch
        {
            (true, _) => null,
            (false, true) => FailReadiness(owner),
            (false, false) => new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex),
        };

        static ulong TermAt(FollowerLogJournal journal, ulong logIndex)
        {
            return journal.Entries.TryGetValue(logIndex, out var entry) ? entry.Term : 0UL;
        }

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

    private static async Task<FollowerLogAppendResult> CompleteAppendAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ulong leaderCommitIndex,
        ulong lastVerifiedIndex,
        CancellationToken cancellationToken)
    {
        var commitAdvanced = false;
        GroupLogMetadata? commitLogical = null;
        if (leaderCommitIndex > owner.Meta.CommitIndex)
        {
            var target = Math.Min(leaderCommitIndex, lastVerifiedIndex);
            if (target > owner.Meta.CommitIndex)
            {
                commitLogical = owner.Meta with { CommitIndex = target };
                commitAdvanced = true;
            }
        }

        // The last log index is not stored: recovery derives it from the log file.
        if (commitLogical is { } candidate)
        {
            await PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
            owner.Meta = candidate;
        }

        if (commitAdvanced)
            owner.Faults.OnCommitAdvanced();

        return new FollowerLogAppendResult(true, string.Empty, owner.Meta.CurrentTerm, owner.LastLogIndex);
    }

    private static FollowerLogAppendResult FailReadiness(IFollowerLogContext owner)
    {
        owner.Readiness = FollowerLogReadiness.Failed;
        return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);
    }

    private static bool IsSatisfiedByLocalState(FollowerLogJournal journal, IFollowerLogContext owner, in FollowerLogEntry candidate)
    {
        // The term of an applied entry was released with its payload, so it is read back from the retained
        // frame metadata. Compaction removes the covered prefix from EntryOffsets, so a frame that is absent
        // there is correctly not acknowledged; a retained frame is verifiable by term alone because Leader
        // Completeness forbids a conflicting term at an applied index.
        var localSatisfied = candidate.LogIndex <= owner.LastLogIndex && journal.TryGetEntry(candidate.LogIndex, out var existing) && existing.Term == candidate.Term &&
                             existing.PayloadSpan.SequenceEqual(candidate.PayloadSpan);
        var appliedSatisfied = candidate.LogIndex <= owner.Meta.LastAppliedIndex && journal.TryGetEntryOffset(candidate.LogIndex, out var loc) && loc.Term == candidate.Term;
        return localSatisfied || appliedSatisfied;
    }

    private static List<FollowerLogEntry> MaterializeOwnedEntries(List<FollowerLogEntry> source)
    {
        // Ownership is needed only for what will actually hit disk or be retained above the applied
        // watermark; duplicates satisfied by local state and refused batches are never copied.
        var owned = new List<FollowerLogEntry>(source.Count);
        for (var i = 0; i < source.Count; i++)
            owned.Add(new FollowerLogEntry(source[i].LogIndex, source[i].Term, source[i].Payload.ToArray()));

        return owned;
    }

    private static FollowerLogAppendResult? PrepareAppendBatch(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        in FollowerLogAppendRequest request,
        out List<FollowerLogEntry>? toAppend,
        out ulong? truncateAtIndex)
    {
        toAppend = null;
        truncateAtIndex = null;
        var nextExpected = request.PrevLogIndex + 1;
        var entries = request.Entries.Span;

        for (var i = 0; i < entries.Length; i++)
        {
            // A leader never sends an entry of a term above its own: such a batch is malformed, and taking it would leave this
            // copy with a last log term above its current term.
            var entry = entries[i];
            if (entry.LogIndex != nextExpected || entry.Term > request.CurrentTerm)
                return new FollowerLogAppendResult(false, FollowerLogRefusal.LogMismatch, owner.Meta.CurrentTerm, owner.LastLogIndex);

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
            if (IsSatisfiedByLocalState(journal, owner, in entry))
                continue;

            if (entry.LogIndex <= owner.Meta.CommitIndex)
                return FailReadiness(owner);

            if (entry.LogIndex <= owner.LastLogIndex)
                truncateAtIndex = entry.LogIndex;

            toAppend ??= [];
            toAppend.Add(entry);
        }

        return null;
    }
}
