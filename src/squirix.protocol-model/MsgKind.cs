namespace Squirix.ProtocolModel;

/// <summary>In-flight message kinds in the Raft-equivalent model.</summary>
internal enum MsgKind
{
    /// <summary>Candidate requests a vote from a peer.</summary>
    RequestVote = 0,

    /// <summary>Peer answers a vote request.</summary>
    VoteResponse = 1,

    /// <summary>Leader replicates a log entry (or heartbeat payload).</summary>
    AppendEntries = 2,

    /// <summary>Follower answers an AppendEntries RPC.</summary>
    AppendResponse = 3,

    /// <summary>Leader asks peers to confirm a read index.</summary>
    ReadIndexRequest = 4,

    /// <summary>Peer answers a read-index confirm request.</summary>
    ReadIndexResponse = 5,
}
