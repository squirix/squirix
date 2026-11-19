using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Uncommitted leader log tail recovered at coordinator start, with the current-term commit rule that guards it.</summary>
/// <remarks>
/// Every entry was locally appended by an earlier process whose commit reported an unknown outcome or none at all, so a
/// majority may or may not hold it. The coordinator pins their identities and re-reads a large tail from the WAL once a recorded
/// majority covers it. An index is committable by counting replicas only once a current-term entry reaches it
/// (<see cref="CommitBoundary" />).
/// </remarks>
[Immutable]
internal sealed class ReplicaRecoveredTail
{
    private readonly ulong? _firstCurrentTermIndex;

    /// <summary>Initializes a new instance of the <see cref="ReplicaRecoveredTail" /> class.</summary>
    /// <param name="entries">The uncommitted leader log entries, contiguous and in index order.</param>
    /// <param name="currentTerm">The leader's current term.</param>
    /// <param name="rebuilder">Rebuilds the prepared form, outcome included, of each entry.</param>
    /// <param name="idempotency">Installs pins during a gated WAL visit, when supplied.</param>
    /// <exception cref="ArgumentException"><paramref name="entries" /> is empty or not contiguous.</exception>
    /// <exception cref="System.IO.InvalidDataException">An entry payload is not a canonical replica log record, or the record is inconsistent.</exception>
    internal ReplicaRecoveredTail(IReadOnlyList<FollowerLogEntry> entries, ulong currentTerm, IReplicaTailRebuilder rebuilder, GroupIdempotencyState? idempotency = null)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(rebuilder);
        if (entries.Count == 0)
            throw new ArgumentException("A recovered tail holds at least one entry.", nameof(entries));

        var mutations = new PreparedReplicaMutation[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            if (i > 0 && entries[i].LogIndex != entries[i - 1].LogIndex + 1)
                throw new ArgumentException("Recovered tail entries must be contiguous.", nameof(entries));

            var entry = entries[i];
            if (entry.Term == currentTerm && (_firstCurrentTermIndex is not { } first || entry.LogIndex < first))
                _firstCurrentTermIndex = entry.LogIndex;

            mutations[i] = rebuilder.Rebuild(entry);
        }

        FirstIndex = entries[0].LogIndex;
        LastIndex = entries[^1].LogIndex;
        Mutations = mutations;
        if (idempotency != null)
        {
            foreach (var mutation in mutations)
                Pin(idempotency, mutation);

            PinsInstalled = true;
        }
    }

    private ReplicaRecoveredTail(ulong first, ulong last, ulong? firstCurrentTerm, WalPages pages)
    {
        FirstIndex = first;
        LastIndex = last;
        _firstCurrentTermIndex = firstCurrentTerm;
        Pages = pages;
        Mutations = [];
        PinsInstalled = true;
    }

    internal WalPages? Pages { get; }

    internal bool PinsInstalled { get; }

    /// <summary>Gets the first recovered log index.</summary>
    internal ulong FirstIndex { get; }

    /// <summary>Gets the last recovered log index.</summary>
    internal ulong LastIndex { get; }

    /// <summary>Gets the prepared entries for a resident page. A WAL-backed tail retains only its range and reader.</summary>
    internal IReadOnlyList<PreparedReplicaMutation> Mutations { get; }

    /// <summary>Gets the commit rule without retaining recovered entries or their payloads.</summary>
    internal ReplicaRecoveredCommitBoundary CommitBoundary => new(LastIndex, _firstCurrentTermIndex);

    /// <summary>Validates and pins each page before the coordinator is published. Only a tail that fits one page stays resident.</summary>
    /// <param name="log">The leader log that owns the captured suffix.</param>
    /// <param name="tail">The captured range and suffix revision.</param>
    /// <param name="currentTerm">The term whose entries can advance the commit boundary.</param>
    /// <param name="rebuilder">Validates records and recovers their prepared outcomes.</param>
    /// <param name="cancellationToken">Cancels scanning between pages.</param>
    /// <returns>The pending tail or null when the log is fully committed.</returns>
    /// <exception cref="InvalidDataException">The WAL no longer contains a contiguous captured range.</exception>
    internal static async Task<ReplicaRecoveredTail?> ReadAsync(
        IFollowerLog log, FollowerLogTail tail, ulong currentTerm, IReplicaTailRebuilder rebuilder, CancellationToken cancellationToken)
    {
        var after = tail.Status.CommitIndex;
        var last = tail.Status.LastLogIndex;
        if (after == last)
            return null;

        var idempotency = log.Idempotency;
        ulong? firstCurrentTerm = null;
        ReplicaRecoveredTail? singlePage = null;
        while (after < last)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await log.VisitLeaderTailBatchAsync(tail, after, Visit, cancellationToken).ConfigureAwait(false);
        }

        return singlePage ?? new ReplicaRecoveredTail(tail.Status.CommitIndex + 1UL, last, firstCurrentTerm, new WalPages(log, rebuilder, tail.Revision));

        void Visit(IReadOnlyList<FollowerLogEntry> entries)
        {
            if (entries.Count == 0 || entries[0].LogIndex != after + 1UL || entries[^1].LogIndex > last)
                throw new InvalidDataException("A recovered WAL page does not continue the captured leader tail.");

            var page = new ReplicaRecoveredTail(entries, currentTerm, rebuilder, idempotency);
            firstCurrentTerm ??= page._firstCurrentTermIndex;
            if (after == tail.Status.CommitIndex && page.LastIndex == last && entries[0].Payload.Length <= FollowerLogTail.BatchBytes)
                singlePage = page;

            after = page.LastIndex;
        }
    }

    internal static void Pin(GroupIdempotencyState idempotency, PreparedReplicaMutation entry)
    {
        var reserved = idempotency.Reserve(
            entry.OperationScope,
            entry.OperationId,
            entry.OperationFingerprint.Span,
            GroupRecordKinds.FromScope(entry.OperationScope),
            entry.LogIndex,
            entry.Term,
            new GroupReserveOptions(true, entry.OutcomePayload.Length));
        if (reserved != GroupIdempotencyReserveResult.Success)
            throw new InvalidOperationException($"Recovered log entry {entry.LogIndex} cannot be pinned for idempotent retries: {reserved}.");
    }

    /// <summary>Re-reads only the now-committed part of a large recovered tail.</summary>
    /// <param name="Log">The WAL containing the recovered range.</param>
    /// <param name="Rebuilder">Validates each record before applying it.</param>
    /// <param name="Revision">Revision of the validated suffix.</param>
    [Immutable]
    internal sealed record WalPages(IFollowerLog Log, IReplicaTailRebuilder Rebuilder, long Revision)
    {
        internal async Task<IReadOnlyList<FollowerLogEntry>> ReadCommittedAsync(ulong after, ulong through)
        {
            var read = await Log.ReadEntriesAsync(after + 1UL, FollowerLogTail.CountThrough(after, through), CancellationToken.None, FollowerLogTail.BatchBytes).ConfigureAwait(false);
            return read.Retained && read.TailRevision == Revision && read.Entries.Count > 0 && read.CommitIndex >= read.Entries[^1].LogIndex
                ? read.Entries
                : throw new InvalidDataException("The committed recovered WAL range is no longer available.");
        }
    }
}
