using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Backs up follower progress after mismatches.</summary>
internal static class ReplicaRepairPlanner
{
    /// <summary>Backs up the next index after a previous-entry mismatch without crossing the log origin.</summary>
    /// <param name="currentNextIndex">Current optimistic next index.</param>
    /// <param name="followerLastIndex">Last index reported by the follower.</param>
    /// <returns>The next earlier probe index.</returns>
    internal static ulong BackUpNextIndex(ulong currentNextIndex, ulong followerLastIndex)
    {
        if (currentNextIndex <= 1UL)
            return 1UL;

        var precedingProbe = currentNextIndex - 1UL;
        var followerProbe = followerLastIndex == ulong.MaxValue ? ulong.MaxValue : followerLastIndex + 1UL;
        return Math.Max(1UL, Math.Min(precedingProbe, followerProbe));
    }
}
