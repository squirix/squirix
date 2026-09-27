namespace Squirix.Server.Storage.Replication;

/// <summary>Mutable state shared by the durable follower log components.</summary>
internal interface IFollowerLogState
{
    /// <summary>Gets the replica group identifier.</summary>
    string GroupId { get; }

    /// <summary>Gets or sets the follower log readiness.</summary>
    FollowerLogReadiness Readiness { get; set; }

    /// <summary>Gets the retained idempotency state.</summary>
    GroupIdempotencyState Idempotency { get; }

    /// <summary>Gets or sets the durable group log metadata.</summary>
    GroupLogMetadata Meta { get; set; }

    /// <summary>Gets or sets the index of the last log entry.</summary>
    ulong LastLogIndex { get; set; }
}
