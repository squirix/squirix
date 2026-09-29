using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Thresholds past which the replica group log this node owns is compacted.</summary>
/// <param name="MaxLogBytes">The group log file size in bytes that triggers a compaction.</param>
/// <param name="MaxLogEntries">The number of entry frames in the group log file that triggers a compaction.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaLogCompactionPolicy(long MaxLogBytes, int MaxLogEntries)
{
    private const long BytesPerMegabyte = 1024L * 1024L;

    /// <summary>Builds the policy from the persistence options of the node.</summary>
    /// <param name="persistence">The resolved persistence options.</param>
    /// <returns>The compaction policy.</returns>
    internal static ReplicaLogCompactionPolicy From(PersistenceOptions persistence)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        return new ReplicaLogCompactionPolicy(persistence.ReplicaLogCompactionMb * BytesPerMegabyte, persistence.ReplicaLogCompactionEntries);
    }

    /// <summary>Determines whether the retained log reaches either threshold.</summary>
    /// <param name="retention">The retained log.</param>
    /// <returns><see langword="true" /> when the log file reaches the byte or the entry threshold.</returns>
    internal bool IsReachedBy(in FollowerLogRetention retention) => retention.LogBytes >= MaxLogBytes || retention.RetainedEntries >= MaxLogEntries;
}
