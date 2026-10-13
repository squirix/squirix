namespace Squirix.Server.Cluster.Replication;

/// <summary>Denial reason reported when automatic failover election is not eligible.</summary>
internal enum FailoverDenial
{
    /// <summary>The node is eligible to start an election.</summary>
    None = 0,

    /// <summary>Automatic failover is not enabled: the opt-in switch is off.</summary>
    Disabled = 1,

    /// <summary>Single-node groups never elect.</summary>
    SingleNode = 2,

    /// <summary>Replica factor two never promotes after peer loss.</summary>
    ReplicaFactorTooLow = 3,

    /// <summary>The node lost majority contact and fails closed.</summary>
    NoMajority = 4,

    /// <summary>The rejoining node has not caught up and is not eligible yet.</summary>
    LogNotCaughtUp = 5,

    /// <summary>A higher term was observed; the old leader steps down.</summary>
    StaleTerm = 6,

    /// <summary>The copy of the group on this node is not complete yet: it holds no data of the group and may not lead or campaign.</summary>
    Unseeded = 7,

    /// <summary>The copy of the group on this node is marked incomplete and awaits a state transfer: it may not lead or campaign, but keeps its term, vote and log.</summary>
    Incomplete = 8,
}
