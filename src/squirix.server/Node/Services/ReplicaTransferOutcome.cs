namespace Squirix.Server.Node.Services;

/// <summary>How a state transfer of the owner of a replica group to one of its followers ended.</summary>
internal enum ReplicaTransferOutcome
{
    /// <summary>The follower holds the state of the group at the captured index; the entry catch-up continues from the next one.</summary>
    Transferred = 0,

    /// <summary>The capture was refused: this committer does not lead the term of the follower's pipeline, or its pipeline was replaced.</summary>
    NotLeading = 1,

    /// <summary>The capture was refused: a committed entry is still to be applied, or the applied index is not yet durable.</summary>
    PendingApply = 2,

    /// <summary>The capture was refused: an entry at or below the applied index has no known idempotency outcome yet.</summary>
    UnresolvedOutcome = 3,

    /// <summary>The capture was refused: nothing is applied yet, so there is no state to transfer.</summary>
    NothingApplied = 4,

    /// <summary>The capture was refused: the group log is not ready or its copy is not complete.</summary>
    NotReady = 5,

    /// <summary>The capture was refused: the snapshot exceeds the configured size.</summary>
    SnapshotTooLarge = 6,

    /// <summary>The follower refused a message; the refusal code names why.</summary>
    FollowerRefused = 7,

    /// <summary>A message failed in transport or timed out, and an abort was sent best effort.</summary>
    Unreachable = 8,

    /// <summary>The follower answered with a higher term than the leader's.</summary>
    Superseded = 9,

    /// <summary>The sender behind the follower started draining, so the transfer stopped.</summary>
    Aborted = 10,

    /// <summary>
    /// The follower lost its disk, and the term ceiling could not be read: fewer members than a majority without the follower answered with their
    /// current term (#1126). Nothing was sent.
    /// </summary>
    CeilingUnavailable = 11,
}
