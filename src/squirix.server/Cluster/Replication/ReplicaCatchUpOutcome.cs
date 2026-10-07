namespace Squirix.Server.Cluster.Replication;

/// <summary>How an entry catch-up session ended.</summary>
internal enum ReplicaCatchUpOutcome
{
    /// <summary>The follower holds the leader log exactly through the index the session targeted.</summary>
    CaughtUp = 0,

    /// <summary>The follower needs entries below the leader's retained range; nothing was sent for the missing prefix.</summary>
    Compacted = 1,

    /// <summary>The follower's answers did not let the session converge: it did not back up, backed up too often, or reported an index the leader cannot verify.</summary>
    Diverged = 2,

    /// <summary>The follower holds a higher term than the leader's.</summary>
    StaleTerm = 3,

    /// <summary>The follower refused a request for a reason other than a log mismatch or a stale term.</summary>
    Refused = 4,

    /// <summary>A request failed in transport or timed out.</summary>
    Unreachable = 5,

    /// <summary>The leader log could not read a retained entry back intact.</summary>
    Corrupt = 6,

    /// <summary>The sender behind the lease closed or started draining, so the session stopped.</summary>
    Aborted = 7,
}
