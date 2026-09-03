namespace Squirix.Server.Storage.Replication;

/// <summary>Mutable state shared by the durable follower log components.</summary>
internal interface IFollowerLogState
{
    /// <summary>Gets the replica group identifier.</summary>
    string GroupId { get; }

    /// <summary>Gets the follower log readiness.</summary>
    FollowerLogReadiness Readiness { get; }

    /// <summary>Gets the retained idempotency state.</summary>
    GroupIdempotencyState Idempotency { get; }

    /// <summary>Gets the durable group log metadata.</summary>
    GroupLogMetadata Meta { get; }

    /// <summary>Gets the index of the last log entry.</summary>
    ulong LastLogIndex { get; }

    /// <summary>Updates the durable group log metadata.</summary>
    /// <param name="meta">The metadata to store.</param>
    void SetMeta(GroupLogMetadata meta);

    /// <summary>Updates the follower log readiness.</summary>
    /// <param name="readiness">The readiness to store.</param>
    void SetReadiness(FollowerLogReadiness readiness);

    /// <summary>Updates the index of the last log entry.</summary>
    /// <param name="logIndex">The last log entry index.</param>
    void SetLastLogIndex(ulong logIndex);

    /// <summary>Updates the durable log length.</summary>
    /// <param name="logLength">The log length in bytes.</param>
    void SetLogLength(long logLength);
}
