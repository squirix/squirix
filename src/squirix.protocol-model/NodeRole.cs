namespace Squirix.ProtocolModel;

/// <summary>Raft role of a modeled replica.</summary>
internal enum NodeRole
{
    /// <summary>Replica follows a leader (or is idle before election).</summary>
    Follower = 0,

    /// <summary>Replica is soliciting votes for a new term.</summary>
    Candidate = 1,

    /// <summary>Replica owns the term and accepts client writes/reads.</summary>
    Leader = 2,
}
