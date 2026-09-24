namespace Squirix.Server.Cluster.Replication;

/// <summary>Verdict of a leader-side Log Matching probe.</summary>
internal enum ReplicaProbeKind
{
    /// <summary>The follower could not be reached in time; nothing is known about it.</summary>
    Unreachable = 0,

    /// <summary>The follower refused for a reason that says nothing about its log (stale term, topology, not ready).</summary>
    Refused = 1,

    /// <summary>The follower holds the named entry with the same term.</summary>
    Accepted = 2,

    /// <summary>The follower lacks the named entry or holds a different term at it.</summary>
    LogMismatch = 3,
}
