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

    /// <summary>The local log of the group is not ready (still catching up, unknown or failed), so the node is not eligible yet.</summary>
    LogNotCaughtUp = 5,

    /// <summary>A higher term was observed; the old leader steps down.</summary>
    StaleTerm = 6,

    /// <summary>The copy of the group on this node is not complete yet: it holds no data of the group and may not lead or campaign.</summary>
    Unseeded = 7,

    /// <summary>The copy of the group on this node is marked incomplete and awaits a state transfer: it may not lead or campaign, but keeps its term, vote and log.</summary>
    Incomplete = 8,

    /// <summary>The node cannot apply the committed entries of the group it holds, so a win would fail its start: it does not campaign.</summary>
    ApplyBlocked = 9,

    /// <summary>The apply loop of the group has not looked at it yet, so the node cannot know whether a win would fail its start: it waits and asks again.</summary>
    ApplyUnassessed = 10,
}
