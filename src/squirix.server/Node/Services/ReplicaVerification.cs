namespace Squirix.Server.Node.Services;

/// <summary>State of the leader-side verification of the owned replica group slots.</summary>
internal enum ReplicaVerification
{
    /// <summary>Every slot, including the leader's own, is verified and counts toward the write quorum.</summary>
    AllReady = 0,

    /// <summary>Some follower is not yet verified; verification should be retried.</summary>
    Pending = 1,

    /// <summary>The leader tail is not fully committed or its log is not ready; slots cannot be verified now.</summary>
    Blocked = 2,
}
