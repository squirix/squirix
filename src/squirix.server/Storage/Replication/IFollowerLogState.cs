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

    /// <summary>Gets or sets the commit index the leader announced and this copy verified but has not made durable yet.</summary>
    /// <remarks>
    /// Kept in memory under the log gate. An entry at or below it is committed, so the log never truncates or replaces it, whatever the durable commit says;
    /// it never exceeds the last log index. The apply loop of the group makes it durable with one metadata write per pass, before it applies. Use
    /// <see cref="FollowerLogAppend.EffectiveCommitIndex" /> for every check that must not go below a committed entry.
    /// </remarks>
    ulong CommitHint { get; set; }

    /// <summary>Gets or sets the durable group log metadata.</summary>
    GroupLogMetadata Meta { get; set; }

    /// <summary>Gets or sets the index of the last log entry.</summary>
    ulong LastLogIndex { get; set; }
}
