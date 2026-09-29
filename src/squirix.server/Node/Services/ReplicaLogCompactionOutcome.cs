namespace Squirix.Server.Node.Services;

/// <summary>Outcome of one compaction step on the replica group log this node owns.</summary>
internal enum ReplicaLogCompactionOutcome
{
    /// <summary>The log holds fewer bytes and entries than the compaction thresholds; nothing was checked further.</summary>
    BelowThreshold = 0,

    /// <summary>A snapshot now covers the whole committed log, and the log holds its header alone.</summary>
    Compacted = 1,

    /// <summary>A follower slot is not verified ready, so the leader cannot tell which entries it still needs.</summary>
    FollowerNotReady = 2,

    /// <summary>A ready follower has not durably acknowledged every committed entry, so the log keeps the entries it may still need.</summary>
    FollowerBehind = 3,

    /// <summary>A committed entry is not applied to memory yet.</summary>
    PendingApply = 4,

    /// <summary>The log holds entries above its commit index.</summary>
    UncommittedTail = 5,

    /// <summary>A committed entry still has an unresolved idempotency outcome, which the snapshot cannot carry.</summary>
    UnresolvedOutcome = 6,

    /// <summary>The log or the committer is not ready: the log is not open and ready, or no commit coordinator runs yet.</summary>
    NotReady = 7,

    /// <summary>The snapshot would exceed the maximum snapshot size; compaction stalls until idempotency outcomes age out.</summary>
    SnapshotTooLarge = 8,
}
