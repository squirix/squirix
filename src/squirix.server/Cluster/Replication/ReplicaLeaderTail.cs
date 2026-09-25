using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The leader's durable entries above its commit index, with the position that precedes them.</summary>
/// <param name="CommitIndex">Durable commit index of the leader log.</param>
/// <param name="CommitTerm">Term of the entry at <paramref name="CommitIndex" />; zero at the log origin.</param>
/// <param name="Entries">Uncommitted entries in index order; empty when the log tail is fully committed.</param>
[Immutable]
internal sealed record ReplicaLeaderTail(ulong CommitIndex, ulong CommitTerm, IReadOnlyList<FollowerLogEntry> Entries)
{
    /// <summary>Gets a value indicating whether the log tail is fully committed.</summary>
    internal bool IsEmpty => Entries.Count == 0;

    /// <summary>Gets the last uncommitted index, or the commit index when the tail is empty.</summary>
    internal ulong LastIndex => IsEmpty ? CommitIndex : Entries[^1].LogIndex;

    /// <summary>Determines whether counting replicas may commit the whole tail under the current-term rule.</summary>
    /// <param name="currentTerm">The leader's current term.</param>
    /// <returns><see langword="true" /> when the tail is empty or a current-term entry reaches its last index.</returns>
    internal bool IsCommittableIn(ulong currentTerm) => IsEmpty || ElectionCommitRule.HasCurrentTermEntryThrough(Entries, currentTerm, LastIndex);

    /// <summary>Reads the uncommitted tail a log status reports.</summary>
    /// <param name="log">The leader's own group log.</param>
    /// <param name="status">A status of that log; the tail is read only when it reports entries above the commit index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tail; callers holding no commit gate re-check the status before acting on it.</returns>
    internal static async Task<ReplicaLeaderTail> ReadAsync(IFollowerLog log, FollowerLogStatus status, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        if (status.LastLogIndex == status.CommitIndex)
            return new ReplicaLeaderTail(status.CommitIndex, 0, []);

        var entries = await log.GetUncommittedTailAsync(cancellationToken).ConfigureAwait(false);
        var commitTerm = await log.GetTermAtAsync(status.CommitIndex, cancellationToken).ConfigureAwait(false);
        return new ReplicaLeaderTail(status.CommitIndex, commitTerm, entries);
    }
}
