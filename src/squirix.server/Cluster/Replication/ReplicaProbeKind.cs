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

    /// <summary>The follower answered that its copy is incomplete: it is reachable and needs a repair, and it takes no entries until then.</summary>
    Incomplete = 4,

    /// <summary>
    /// The follower answered that its copy lost its disk: it is reachable, takes no entries and votes for nobody until a leader that checked the
    /// term ceiling repairs it by a state transfer (#1126).
    /// </summary>
    Unseeded = 5,
}
