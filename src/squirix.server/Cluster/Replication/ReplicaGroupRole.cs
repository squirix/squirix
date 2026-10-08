namespace Squirix.Server.Cluster.Replication;

/// <summary>The election role of this node in one replica group.</summary>
internal enum ReplicaGroupRole
{
    /// <summary>The node follows a leader, or waits for one; every group starts here.</summary>
    Follower = 0,

    /// <summary>The node probes the voters, without changing any term, whether an election for the next term could win.</summary>
    PreCandidate = 1,

    /// <summary>The node persisted the next term and its own vote and asks the voters for theirs.</summary>
    Candidate = 2,

    /// <summary>The node won its term; it serves writes only once its leader-term entry is committed.</summary>
    Leader = 3,
}
