namespace Squirix.Server.Node.Services;

/// <summary>Outcome of one compaction step on a replica group log this node serves.</summary>
internal enum ReplicaLogCompactionOutcome
{
    /// <summary>The log holds fewer bytes and entries than the compaction thresholds; nothing was checked further.</summary>
    BelowThreshold = 0,

    /// <summary>A snapshot now covers the log through its applied index, and the log holds only the entries above it.</summary>
    Compacted = 1,

    /// <summary>A follower slot is not verified ready, so the leader cannot tell which entries it still needs.</summary>
    FollowerNotReady = 2,

    /// <summary>
    /// A ready follower has not durably acknowledged every committed entry, so the log keeps the entries it may still need. Past the
    /// retention limit this means that leaving it behind would cost the write majority, or that it was behind only under the commit gate.
    /// </summary>
    FollowerBehind = 3,

    /// <summary>A committed entry is not applied to memory yet, or a follower group has not rebuilt the outcomes of its applied entries.</summary>
    PendingApply = 4,

    /// <summary>The log holds entries above its commit index.</summary>
    UncommittedTail = 5,

    /// <summary>A committed entry still has an unresolved idempotency outcome, which the snapshot cannot carry.</summary>
    UnresolvedOutcome = 6,

    /// <summary>The log or the committer is not ready: the log is not open and ready, or no commit coordinator runs yet.</summary>
    NotReady = 7,

    /// <summary>The snapshot would exceed the maximum snapshot size; compaction stalls until idempotency outcomes age out.</summary>
    SnapshotTooLarge = 8,

    /// <summary>The followers or the commit gate of a led group stayed busy past the wait budget of the step; it is retried on the next pass.</summary>
    Busy = 9,

    /// <summary>
    /// The log reached its retention limit and was compacted although a follower slot is not ready or stayed behind the commit index; the
    /// step took such a follower out of the write quorum, and it is repaired by catch-up or a state transfer.
    /// </summary>
    CompactedPastFollower = 10,
}
