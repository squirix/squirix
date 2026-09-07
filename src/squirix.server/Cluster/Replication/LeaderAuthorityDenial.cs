namespace Squirix.Server.Cluster.Replication;

/// <summary>Denial reason reported when the leader authority gate refuses a read or write.</summary>
internal enum LeaderAuthorityDenial
{
    /// <summary>The request is allowed.</summary>
    None = 0,

    /// <summary>This node is not the leader.</summary>
    NotLeader = 1,

    /// <summary>The leader lost majority contact and fails closed.</summary>
    MinorityFenced = 2,

    /// <summary>A higher term was observed; the old leader steps down.</summary>
    StaleTerm = 3,

    /// <summary>Quorum confirmation failed; the current read is rejected.</summary>
    QuorumNotConfirmed = 4,

    /// <summary>The read index is not applied yet; the read waits.</summary>
    ReadIndexNotApplied = 5,
}
