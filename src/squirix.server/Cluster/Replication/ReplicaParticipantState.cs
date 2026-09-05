namespace Squirix.Server.Cluster.Replication;

/// <summary>Participation state of one member of a fixed replica group.</summary>
internal enum ReplicaParticipantState
{
    /// <summary>The participant is validating local durable state after startup.</summary>
    Recovering = 0,

    /// <summary>The participant is reconciling its durable state with the current leader.</summary>
    CatchingUp = 1,

    /// <summary>The participant has passed identity and index verification and may participate.</summary>
    Ready = 2,

    /// <summary>The participant failed a non-recoverable identity or durability check.</summary>
    Quarantined = 3,
}
