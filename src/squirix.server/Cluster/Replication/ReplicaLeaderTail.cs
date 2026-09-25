using System;
using System.Collections.Generic;
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

    /// <summary>Creates the tail from a leader log read that paired its status with its uncommitted entries.</summary>
    /// <param name="read">The leader's own group log tail, read in one step.</param>
    /// <returns>The tail at the commit index of <paramref name="read" />; callers holding no commit gate re-check the status before acting on it.</returns>
    internal static ReplicaLeaderTail From(FollowerLogTail read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return new ReplicaLeaderTail(read.Status.CommitIndex, read.CommitTerm, read.Entries);
    }
}
