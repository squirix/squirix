namespace Squirix.Server.Cluster.Replication;

/// <summary>Observable durable boundaries in the leader commit pipeline.</summary>
internal enum ReplicaCommitStage
{
    /// <summary>The immutable mutation bytes and exact outcome are ready.</summary>
    Prepared = 0,

    /// <summary>The leader copy is durably appended.</summary>
    LocalAppendDurable = 1,

    /// <summary>Every configured follower call has been started.</summary>
    FollowerFanOutStarted = 2,

    /// <summary>A verified durable majority covers the entry.</summary>
    MajorityReached = 3,

    /// <summary>The local commit index is durably advanced.</summary>
    CommitIndexDurable = 4,

    /// <summary>The committed mutation is applied to memory.</summary>
    MemoryApplied = 5,

    /// <summary>The exact successful outcome is ready for the caller.</summary>
    ResponseReady = 6,
}
