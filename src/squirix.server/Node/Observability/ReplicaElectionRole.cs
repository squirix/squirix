namespace Squirix.Server.Node.Observability;

/// <summary>The election role of the observing node in a replica group, as the role gauge reports it.</summary>
internal enum ReplicaElectionRole
{
    /// <summary>The node follows a leader or waits for one.</summary>
    Follower = 0,

    /// <summary>The node probes, without changing any term, whether it could win an election.</summary>
    PreCandidate = 1,

    /// <summary>The node campaigns in a new term.</summary>
    Candidate = 2,

    /// <summary>The node won its term, and its leader-term entry is not committed yet: it serves no write.</summary>
    Leader = 3,

    /// <summary>The node leads with authority: elected with its leader-term entry committed, or the owner leading its group statically.</summary>
    AuthorizedLeader = 4,
}
